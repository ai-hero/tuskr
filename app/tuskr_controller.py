import logging
import time

import kopf
import kubernetes

import falcon
import json
from wsgiref.simple_server import make_server

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Kopf Startup: Load Kubernetes config
# ------------------------------------------------------------------
@kopf.on.startup()
def startup_fn(logger, **kwargs):
    logger.info("Tuskr controller is starting up.")
    # If running in the cluster:
    kubernetes.config.load_incluster_config()
    # If testing locally:
    # kubernetes.config.load_kube_config()


# ------------------------------------------------------------------
# Handlers for JobTemplate CRD
# ------------------------------------------------------------------
@kopf.on.create("tuskr.io", "v1alpha1", "jobtemplates")
def create_jobtemplate(body, spec, **kwargs):
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was created with spec: {spec}")
    return {"message": f"Created JobTemplate {name}"}


@kopf.on.update("tuskr.io", "v1alpha1", "jobtemplates")
def update_jobtemplate(body, spec, **kwargs):
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was updated with spec: {spec}")
    return {"message": f"Updated JobTemplate {name}"}


@kopf.on.delete("tuskr.io", "v1alpha1", "jobtemplates")
def delete_jobtemplate(body, spec, **kwargs):
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was deleted.")
    return {"message": f"Deleted JobTemplate {name}"}


# ------------------------------------------------------------------
# Falcon HTTP Server for launching Jobs from JobTemplates
# ------------------------------------------------------------------

# Simple in-memory rate limiting example:
MIN_LAUNCH_INTERVAL_SEC = 30  # e.g. 30 seconds
last_launch_time = 0.0  # Track last job launch


class LaunchResource:
    def on_post(self, req, resp):
        global last_launch_time

        # Check rate limit
        now = time.time()
        if (now - last_launch_time) < MIN_LAUNCH_INTERVAL_SEC:
            logger.warning("Launch request denied due to rate limit.")
            resp.status = falcon.HTTP_429  # Too Many Requests
            resp.body = json.dumps({"error": "Rate limit exceeded. Please wait."})
            return

        # Parse incoming JSON
        raw_body = req.stream.read(req.content_length or 0)
        if not raw_body:
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"error": "No JSON body provided."})
            return

        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError:
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"error": "Invalid JSON."})
            return

        jobtemplate_name = data.get("jobTemplateName")
        jobtemplate_namespace = data.get("jobTemplateNamespace")
        command_override = data.get("command")
        args_override = data.get("args")

        if not jobtemplate_name or not jobtemplate_namespace:
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"error": "Must provide 'jobTemplateName' and 'jobTemplateNamespace'."})
            return

        # Retrieve the JobTemplate from the specified namespace
        crd_api = kubernetes.client.CustomObjectsApi()
        try:
            jobtemplate = crd_api.get_namespaced_custom_object(
                group="tuskr.io",
                version="v1alpha1",
                namespace=jobtemplate_namespace,
                plural="jobtemplates",
                name=jobtemplate_name,
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                msg = f"JobTemplate {jobtemplate_name} not found in namespace {jobtemplate_namespace}."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.body = json.dumps({"error": msg})
                return
            else:
                logger.exception("Unexpected error fetching JobTemplate.")
                resp.status = falcon.HTTP_500
                resp.body = json.dumps({"error": str(e)})
                return

        # Extract the Kubernetes Job spec from the JobTemplate.
        job_spec_from_template = jobtemplate.get("spec", {}).get("template")
        if not job_spec_from_template:
            msg = f"No 'spec.template' found in JobTemplate {jobtemplate_name}"
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.body = json.dumps({"error": msg})
            return

        # Overwrite command/args if provided
        pod_spec = job_spec_from_template.get("template", {}).get("spec", {})
        containers = pod_spec.get("containers", [])
        if containers:
            if command_override:
                containers[0]["command"] = command_override
                logger.info(f"Overriding command with: {command_override}")
            if args_override:
                containers[0]["args"] = args_override
                logger.info(f"Overriding args with: {args_override}")
        else:
            logger.warning("JobTemplate has no containers to override.")

        # Construct the actual Job manifest
        # Decide which namespace to create the Job in.
        #  - If you always want to create in the same namespace as the JobTemplate:
        target_namespace = jobtemplate_namespace
        #  - Or if you want a custom field for the target namespace:
        # target_namespace = data.get("targetNamespace", jobtemplate_namespace)

        job_name = f"{jobtemplate_name}-launch-{int(time.time())}"
        job_body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {"jobtemplate": jobtemplate_name},
            },
            "spec": job_spec_from_template,
        }

        # Create the Job
        batch_api = kubernetes.client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=target_namespace, body=job_body)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create Job.")
            resp.status = falcon.HTTP_500
            resp.body = json.dumps({"error": str(e)})
            return

        # Update last_launch_time for rate limiting
        last_launch_time = time.time()

        msg = (
            f"Created Job '{job_name}' in namespace '{target_namespace}' "
            f"from template '{jobtemplate_name}' in namespace '{jobtemplate_namespace}'."
        )
        logger.info(msg)
        resp.status = falcon.HTTP_201
        resp.body = json.dumps({"message": msg})


# ------------------------------------------------------------------
# Start Falcon server in Kopf
# ------------------------------------------------------------------
# You could also run this as a separate WSGI process,
# but here's an example of running it inside Kopf's main loop.


@kopf.on.startup()
def start_http_server(**kwargs):
    """
    Spin up a simple Falcon HTTP server on a chosen port.
    This will run in a separate thread alongside Kopf's event loop.
    """
    app = falcon.App()
    launch_resource = LaunchResource()
    app.add_route("/launch", launch_resource)

    def _run_server():
        with make_server("", 8000, app) as httpd:
            logger.info("Falcon HTTP server running on port 8000...")
            httpd.serve_forever()

    import threading

    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()
