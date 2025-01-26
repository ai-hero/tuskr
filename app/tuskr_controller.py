import random
import string
import logging
import json
import threading
import os

import kopf
import kubernetes

import falcon
from wsgiref.simple_server import make_server
from helpers.encoder import CustomJsonEncoder


# Redis client
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
# Redis setup
# Adjust these if you have a different Redis connection
# ------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


# ------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------
def generate_random_suffix(length=5):
    """Generate a short random string of letters/digits."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choices(chars, k=length))


def job_redis_key(namespace, job_name):
    """Generate a Redis key name for storing logs of a given Job."""
    return f"job_logs::{namespace}::{job_name}"


# Create a custom Falcon JSON handler
class CustomJSONHandler:
    def serialize(self, media, content_type):
        return json.dumps(media, cls=CustomJsonEncoder)

    def deserialize(self, stream, content_type, content_length):
        return json.loads(stream.read().decode("utf-8"))


# ------------------------------------------------------------------
# Falcon Resource: Launch a Job from a JobTemplate
# ------------------------------------------------------------------
class LaunchResource:
    def on_post(self, req, resp):
        # Parse incoming JSON
        raw_body = req.stream.read(req.content_length or 0)
        if not raw_body:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "No JSON body provided."}
            return

        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Invalid JSON."}
            return

        jobtemplate_info = data.get("jobTemplate", {})
        jobtemplate_name = jobtemplate_info.get("name")
        jobtemplate_namespace = jobtemplate_info.get("namespace")
        command_override = data.get("command")
        args_override = data.get("args")

        if not jobtemplate_name or not jobtemplate_namespace:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Must provide 'jobTemplate.name' and 'jobTemplate.namespace'."}
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
                resp.media = {"error": msg}
                return
            else:
                logger.exception("Unexpected error fetching JobTemplate.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

        # Extract the Kubernetes Job spec from the JobTemplate.
        job_spec_from_template = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template", {})
        if not job_spec_from_template:
            msg = f"No 'spec.jobSpec.template' found in JobTemplate {jobtemplate_name}"
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.media = {"error": msg}
            return

        # Override containers if command/args provided
        pod_spec = job_spec_from_template.get("spec", {})
        containers = pod_spec.get("containers", [])
        if containers:
            if command_override:
                containers[0]["command"] = command_override
            if args_override:
                containers[0]["args"] = args_override

        # Construct the actual Job manifest
        random_suffix = generate_random_suffix()
        job_name = f"{jobtemplate_name}-{random_suffix}"
        target_namespace = jobtemplate_namespace
        job_body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {"jobtemplate": jobtemplate_name},
                # Add TTL setting to automatically delete job after completion
                "annotations": {
                    "tuskr.io/ttl-seconds-after-finished": "900"  # 15 minutes = 900 seconds
                },
            },
            "spec": {
                "template": job_spec_from_template,
                # Add TTL controller setting
                "ttlSecondsAfterFinished": 900,  # 15 minutes
            },
        }

        # Create the Job
        batch_api = kubernetes.client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=target_namespace, body=job_body)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create Job.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        msg = f"Created Job '{job_name}' in namespace '{target_namespace}' from template '{jobtemplate_name}'."
        logger.info(msg)
        resp.status = falcon.HTTP_201
        resp.media = {"message": msg}


# ------------------------------------------------------------------
# Falcon Resource: Basic GET/DELETE on existing Jobs
# ------------------------------------------------------------------
class JobResource:
    def on_get(self, req, resp, namespace, job_name):
        """
        Retrieve details of a specific Kubernetes Job (raw JSON).
        """
        batch_api = kubernetes.client.BatchV1Api()
        try:
            job = batch_api.read_namespaced_job(name=job_name, namespace=namespace)
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                msg = f"Job {job_name} not found in namespace {namespace}."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            else:
                logger.exception("Unexpected error reading Job.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

        # Convert the response to dict
        job_dict = job.to_dict()
        resp.status = falcon.HTTP_200
        resp.media = job_dict

    def on_delete(self, req, resp, namespace, job_name):
        """
        Delete a specific Kubernetes Job (foreground propagation).
        """
        batch_api = kubernetes.client.BatchV1Api()
        try:
            delete_resp = batch_api.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                body=kubernetes.client.V1DeleteOptions(propagation_policy="Foreground"),
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                msg = f"Job {job_name} not found in namespace {namespace}."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            else:
                logger.exception("Unexpected error deleting Job.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

        resp.status = falcon.HTTP_200
        resp.media = {"message": f"Job {job_name} deleted.", "status": delete_resp.to_dict()}


# ------------------------------------------------------------------
# Falcon Resource: "describe"-like endpoint for a Job
# ------------------------------------------------------------------
class JobDescribeResource:
    """
    Returns a "describe" style JSON that includes:
      - The Job object
      - The Pod(s) belonging to that Job
      - The Events related to that Job
    """

    def on_get(self, req, resp, namespace, job_name):
        batch_api = kubernetes.client.BatchV1Api()
        core_api = kubernetes.client.CoreV1Api()

        # 1) Read the Job object
        try:
            job_obj = batch_api.read_namespaced_job(name=job_name, namespace=namespace)
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                msg = f"Job {job_name} not found in namespace {namespace}."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            else:
                logger.exception("Unexpected error reading Job.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

        # 2) Find Pods that belong to this Job
        # Typically by matching the job name label or the controller-uid in ownerReferences
        label_selector = f"job-name={job_name}"
        pods_list = core_api.list_namespaced_pod(namespace, label_selector=label_selector)

        # 3) Retrieve events for the Job (and possibly for the pods)
        events_api = kubernetes.client.CoreV1Api()
        events_for_job = events_api.list_namespaced_event(
            namespace=namespace, field_selector=f"involvedObject.kind=Job,involvedObject.name={job_name}"
        )

        # We can also gather Pod events if desired
        pod_events = []
        for pod in pods_list.items:
            pod_name = pod.metadata.name
            ev = events_api.list_namespaced_event(
                namespace=namespace, field_selector=f"involvedObject.kind=Pod,involvedObject.name={pod_name}"
            )
            pod_events.append({pod_name: [e.to_dict() for e in ev.items]})

        # Build a "describe"-like output in JSON
        describe_output = {
            "job": job_obj.to_dict(),
            "pods": [p.to_dict() for p in pods_list.items],
            "events_for_job": [e.to_dict() for e in events_for_job.items],
            "events_for_pods": pod_events,
        }

        resp.status = falcon.HTTP_200
        resp.media = describe_output


# ------------------------------------------------------------------
# Falcon Resource: Logs endpoint for a Job (store/append in Redis)
# ------------------------------------------------------------------
class JobLogsResource:
    """
    Returns the aggregated logs for all Pods of a Job, while storing them in Redis.
    On each request:
      1) Retrieves the logs from each Pod container.
      2) Appends them into Redis under a known key.
      3) Returns the logs from Redis as a single string (or as a dict).
    """

    def on_get(self, req, resp, namespace, job_name):
        core_api = kubernetes.client.CoreV1Api()
        batch_api = kubernetes.client.BatchV1Api()

        # Check if Job exists first
        try:
            batch_api.read_namespaced_job(name=job_name, namespace=namespace)
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                msg = f"Job {job_name} not found in namespace {namespace}."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            else:
                logger.exception("Unexpected error reading Job.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

        # Find all Pods that belong to this job
        label_selector = f"job-name={job_name}"
        pods_list = core_api.list_namespaced_pod(namespace, label_selector=label_selector)

        # Retrieve the logs for each Pod & container, then store/append in Redis
        aggregated_logs = ""
        for pod in pods_list.items:
            pod_name = pod.metadata.name
            # A Pod can have multiple containers; fetch logs for each
            for container in pod.spec.containers:
                container_name = container.name
                try:
                    pod_logs = core_api.read_namespaced_pod_log(
                        name=pod_name, namespace=namespace, container=container_name
                    )
                    # Append to aggregator
                    aggregated_logs += f"--- Logs for Pod: {pod_name}, Container: {container_name} ---\n{pod_logs}\n\n"
                except kubernetes.client.exceptions.ApiException as log_e:
                    aggregated_logs += (
                        f"--- Failed to get logs for Pod: {pod_name}, Container: {container_name}: {str(log_e)} ---\n\n"
                    )

        # Store logs in Redis (append to existing logs)
        redis_key = job_redis_key(namespace, job_name)
        if aggregated_logs:
            # The Redis 'append' command can only work with raw bytes or strings,
            # so we do .encode() if needed
            redis_client.append(redis_key, aggregated_logs)

        # Return the *entire* logs from Redis (including newly appended portion)
        full_logs = redis_client.get(redis_key)
        if full_logs is None:
            full_logs_str = ""
        else:
            full_logs_str = full_logs.decode("utf-8", errors="replace")

        resp.status = falcon.HTTP_200
        resp.media = {"job": job_name, "namespace": namespace, "logs": full_logs_str}


# ------------------------------------------------------------------
# Start Falcon server in Kopf
# ------------------------------------------------------------------
@kopf.on.startup()
def start_http_server(**kwargs):
    """
    Spin up a simple Falcon HTTP server on a chosen port.
    This will run in a separate thread alongside Kopf's event loop.
    """
    app = falcon.App(
        media_type=falcon.MEDIA_JSON,
        json_handler=CustomJSONHandler(),
    )

    # Route for launching Jobs from JobTemplates
    launch_resource = LaunchResource()
    app.add_route("/launch", launch_resource)

    # Routes for direct GET/DELETE on existing Jobs
    job_resource = JobResource()
    app.add_route("/jobs/{namespace}/{job_name}", job_resource)

    # Route for "describe"-like output
    describe_resource = JobDescribeResource()
    app.add_route("/jobs/{namespace}/{job_name}/describe", describe_resource)

    # Route for aggregated logs (with Redis)
    logs_resource = JobLogsResource()
    app.add_route("/jobs/{namespace}/{job_name}/logs", logs_resource)

    def _run_server():
        with make_server("", 8080, app) as httpd:
            logger.info("Falcon HTTP server running on port 8080...")
            httpd.serve_forever()

    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()
