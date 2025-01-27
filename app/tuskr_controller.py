# MIT License
#
# Copyright (c) 2025 A.I. Hero, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""The controller."""

import json
import logging
import os
import random
import string
import threading
import traceback
from functools import partial
from typing import Any, Dict, List
from wsgiref.simple_server import make_server

import falcon
import kopf
import kubernetes
import redis  # type: ignore
from falcon import Request, Response, media
from pydantic import BaseModel, ValidationError

from helpers.encoder import CustomJsonDecoder, CustomJsonEncoder

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ------------------------------------------------------------------
# Kopf Startup: Load Kubernetes config
# ------------------------------------------------------------------
@kopf.on.startup()  # type: ignore
def startup_fn(logger: logging.Logger, **kwargs: Any) -> None:
    """Load the Kubernetes configuration on startup."""
    logger.info("Tuskr controller is starting up.")
    # If running in the cluster:
    kubernetes.config.load_incluster_config()
    # If testing locally:
    # kubernetes.config.load_kube_config()


# ------------------------------------------------------------------
# Handlers for JobTemplate CRD
# ------------------------------------------------------------------
@kopf.on.create("tuskr.io", "v1alpha1", "jobtemplates")  # type: ignore
def create_jobtemplate(body: Dict[str, Any], spec: Dict[str, Any], **kwargs: Any) -> Dict[str, str]:
    """Handle creation of a JobTemplate."""
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was created with spec: {spec}")
    return {"message": f"Created JobTemplate {name}"}


@kopf.on.update("tuskr.io", "v1alpha1", "jobtemplates")  # type: ignore
def update_jobtemplate(body: Dict[str, Any], spec: Dict[str, Any], **kwargs: Any) -> Dict[str, str]:
    """Handle update of a JobTemplate."""
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was updated with spec: {spec}")
    return {"message": f"Updated JobTemplate {name}"}


@kopf.on.delete("tuskr.io", "v1alpha1", "jobtemplates")  # type: ignore
def delete_jobtemplate(body: Dict[str, Any], spec: Dict[str, Any], **kwargs: Any) -> Dict[str, str]:
    """Handle deletion of a JobTemplate."""
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
def generate_random_suffix(length: int = 5) -> str:
    """Generate a short random string of letters/digits."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choices(chars, k=length))


def job_redis_key(namespace: str, job_name: str) -> str:
    """Generate a Redis key name for storing logs of a given Job."""
    return f"job_logs::{namespace}::{job_name}"


# ------------------------------------------------------------------
# Falcon Resource: Launch a Job from a JobTemplate
# ------------------------------------------------------------------
class LaunchResource:
    """Launch a Job from a JobTemplate, with optional command/args overrides."""

    def store_input_files(self, job_name: str, input_files: Dict[str, str]) -> None:
        """Store input files in Redis with job name prefix.

        Args:
        ----
            job_name: Name of the job
            input_files: Dictionary mapping filenames to their content

        """
        for filename, content in input_files.items():
            key = f"{job_name}/{filename}"
            redis_client.set(key, content)
            # Store file list for this job for 1 hour
            redis_client.sadd(f"{job_name}/files", filename)
            redis_client.expire(f"{job_name}/files", 3600)

    def on_post(self, req: Request, resp: Response) -> None:
        """Create a Job from a JobTemplate, with optional command/args overrides."""
        try:
            data = req.media
        except Exception as e:
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"Invalid request format: {str(e)}"}
            return

        jobtemplate_info = data.get("jobTemplate", {})
        jobtemplate_name = jobtemplate_info.get("name")
        jobtemplate_namespace = jobtemplate_info.get("namespace")
        command_override = data.get("command")
        args_override = data.get("args")
        input_files = data.get("inputs", {})

        if not jobtemplate_name or not jobtemplate_namespace:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Must provide 'jobTemplate.name' and 'jobTemplate.namespace'."}
            return

        # Create volume configuration for inputs
        volumes = [{"name": "inputs-volume", "emptyDir": {}}]
        volume_mounts = [{"name": "inputs-volume", "mountPath": "/mnt/data/inputs"}]

        # Retrieve the JobTemplate and modify its spec
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

        # Extract and modify the job spec
        job_spec_from_template = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template", {})
        if not job_spec_from_template:
            msg = f"No 'spec.jobSpec.template' found in JobTemplate {jobtemplate_name}"
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.media = {"error": msg}
            return

        # Generate job name early as we need it for file storage
        random_suffix = generate_random_suffix()
        job_name = f"{jobtemplate_name}-{random_suffix}"

        # Store input files in Redis if any
        if input_files:
            try:
                self.store_input_files(job_name, input_files)
            except Exception as e:
                logger.exception("Failed to store input files in Redis.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": f"Failed to store input files: {str(e)}"}
                return

        # Modify pod spec to include volumes and sidecars
        pod_spec = job_spec_from_template.get("spec", {})
        containers = pod_spec.get("containers", [])

        if containers:
            # Add volume mounts to the first container
            existing_mounts = containers[0].get("volumeMounts", [])
            containers[0]["volumeMounts"] = existing_mounts + volume_mounts

            if command_override:
                containers[0]["command"] = command_override
            if args_override:
                containers[0]["args"] = args_override

        # Add volumes to pod spec
        existing_volumes = pod_spec.get("volumes", [])
        pod_spec["volumes"] = existing_volumes + volumes

        # Create init container to handle input files
        if input_files:
            init_containers = [
                {
                    "name": "input-setup",
                    "image": "alpine",  # Using Alpine as base
                    "command": ["sh", "-c"],
                    "args": [
                        "apk add --no-cache redis && "  # Install redis-cli
                        "set -ex && "  # Exit on error, print commands
                        "mkdir -p /mnt/data/inputs && "
                        "chmod 777 /mnt/data/inputs && "
                        'files=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT SMEMBERS "${JOB_NAME}/files") && '
                        'if [ -z "$files" ]; then '
                        "  echo 'No files found in Redis' && exit 1; "
                        "fi && "
                        "for filename in $files; do "
                        '  echo "Processing file: $filename" && '
                        '  redis-cli -h $REDIS_HOST -p $REDIS_PORT GET "${JOB_NAME}/${filename}" > \
"/mnt/data/inputs/${filename}" && '
                        '  if [ ! -s "/mnt/data/inputs/${filename}" ]; then '
                        '    echo "Failed to retrieve file: $filename" && exit 1; '
                        "  fi && "
                        '  redis-cli -h $REDIS_HOST -p $REDIS_PORT DEL "${JOB_NAME}/${filename}"; '
                        "done"
                    ],
                    "env": [
                        {
                            "name": "REDIS_HOST",
                            "value": REDIS_HOST,
                        },
                        {
                            "name": "REDIS_PORT",
                            "value": str(REDIS_PORT),
                        },
                        {
                            "name": "JOB_NAME",
                            "value": job_name,
                        },
                    ],
                    "volumeMounts": volume_mounts,
                }
            ]
            pod_spec["initContainers"] = init_containers

        # Construct the Job manifest
        target_namespace = jobtemplate_namespace

        job_body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {"jobtemplate": jobtemplate_name},
                "annotations": {"tuskr.io/ttl-seconds-after-finished": "900"},
            },
            "spec": {
                "template": job_spec_from_template,
                "ttlSecondsAfterFinished": 900,
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
        resp.media = {"message": msg, "job_name": job_name, "namespace": target_namespace}


# ------------------------------------------------------------------
# Falcon Resource: Basic GET/DELETE on existing Jobs
# ------------------------------------------------------------------
class JobResource:
    """Retrieve details of a specific Kubernetes Job (raw JSON)."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Retrieve details of a specific Kubernetes Job (raw JSON)."""
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

    def on_delete(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Delete a specific Kubernetes Job (foreground propagation)."""
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
        resp.media = {
            "message": f"Job {job_name} deleted.",
            "status": delete_resp.to_dict(),
        }


# ------------------------------------------------------------------
# Falcon Resource: "describe"-like endpoint for a Job
# ------------------------------------------------------------------
class JobDescribeResource:
    """Returns a detailed "describe"-like output for a Job, including Pods and Events."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Return a detailed "describe"-like output for a Job, including Pods and Events."""
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
            namespace=namespace,
            field_selector=f"involvedObject.kind=Job,involvedObject.name={job_name}",
        )

        # We can also gather Pod events if desired
        pod_events = []
        for pod in pods_list.items:
            pod_name = pod.metadata.name
            ev = events_api.list_namespaced_event(
                namespace=namespace,
                field_selector=f"involvedObject.kind=Pod,involvedObject.name={pod_name}",
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
    """Returns the aggregated logs for all Pods of a Job, while storing them in Redis."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Return the aggregated logs for all Pods of a Job, while storing them in Redis."""
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

        # Retrieve the logs for each Pod & container
        pod_logs_list = []
        for pod in pods_list.items:
            pod_name = pod.metadata.name
            # A Pod can have multiple containers; fetch logs for each
            for container in pod.spec.containers:
                container_name = container.name
                try:
                    pod_logs = core_api.read_namespaced_pod_log(
                        name=pod_name, namespace=namespace, container=container_name
                    )
                    pod_logs_list.append({"pod_name": pod_name, "container_name": container_name, "logs": pod_logs})
                except kubernetes.client.exceptions.ApiException as log_e:
                    pod_logs_list.append({"pod_name": pod_name, "container_name": container_name, "error": str(log_e)})

        # Store logs in Redis (as JSON string to preserve structure)
        redis_key = job_redis_key(namespace, job_name)
        if pod_logs_list:
            # Convert the list to JSON string before storing
            import json

            logs_json = json.dumps(pod_logs_list)
            redis_client.set(redis_key, logs_json)

        # Retrieve logs from Redis
        stored_logs = redis_client.get(redis_key)
        if stored_logs is None:
            logs_data = []
        else:
            try:
                logs_data = json.loads(stored_logs.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logs_data = []
                logger.error(f"Failed to decode logs from Redis for key {redis_key}")

        resp.status = falcon.HTTP_200
        resp.media = {"job": job_name, "namespace": namespace, "logs": logs_data}


def handle_validation_error(req: Request, resp: Response, exception: ValidationError, params: Any) -> None:
    """Handle Pydantic ValidationError exceptions."""
    # Optionally log the exception details
    logger.error(f"Validation error: {exception}")

    # Set the HTTP status code to 422 Unprocessable Entity
    resp.status = falcon.HTTP_422

    # Prepare a detailed error response
    resp.media = {
        "title": "Unprocessable Entity",
        "description": "The request contains invalid data.",
        "errors": exception.errors(),
    }


def custom_handle_uncaught_exception(req: Request, resp: Response, exception: Exception, params: Any) -> None:
    """Handle uncaught exceptions."""
    traceback.print_exc()
    resp.status = falcon.HTTP_500
    resp.media = f"{exception}"


class LaunchJobModel(BaseModel):
    """Pydantic model for the LaunchResource POST request."""

    jobTemplate: Dict[str, Any]
    command: List[str] = []
    args: List[str] = []
    inputs: Dict[str, str] = {}


# ------------------------------------------------------------------
# Start Falcon server in Kopf
# ------------------------------------------------------------------
@kopf.on.startup()  # type: ignore
def start_http_server(**kwargs: Any) -> None:
    """Start the Falcon HTTP server."""
    app = falcon.App()

    app.add_error_handler(ValidationError, handle_validation_error)
    app.add_error_handler(Exception, custom_handle_uncaught_exception)

    # JSON Handler for the config
    json_handler = media.JSONHandler(
        dumps=partial(json.dumps, cls=CustomJsonEncoder, sort_keys=True),
        loads=partial(json.loads, cls=CustomJsonDecoder),
    )
    extra_handlers = {
        "application/json": json_handler,
    }
    app.req_options.media_handlers.update(extra_handlers)
    app.resp_options.media_handlers.update(extra_handlers)

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

    def _run_server() -> None:
        with make_server("", 8080, app) as httpd:
            logger.info("Falcon HTTP server running on port 8080...")
            httpd.serve_forever()

    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()
