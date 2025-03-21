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

"""Resources for the Falcon API, including Job, Launch, and Payload endpoints."""

import json
import secrets

import falcon
import kubernetes
from falcon import Request, Response
from pydantic import ValidationError

from helpers.redis import (
    REDIS_TTL,
    event_redis_key,
    job_access_token_key,
    job_inputs_key,
    job_logs_key,
    job_pod_set_key,
    job_redis_key,
    pod_redis_key,
    redis_client,
)
from helpers.schema import LaunchJobModel
from helpers.utils import generate_random_suffix, logger

# ------------------------------------------------------------------------------
# Falcon Resources
# ------------------------------------------------------------------------------


class JobResource:
    """Retrieve details of a specific Job (from Redis) or delete it from K8s."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Retrieve the Job object from Redis."""
        key = job_redis_key(namespace, job_name)
        data = redis_client.get(key)
        if not data:
            msg = f"No data found in Redis for Job {job_name} in namespace {namespace}."
            logger.warning(msg)
            resp.status = falcon.HTTP_404
            resp.media = {"error": msg}
            return

        try:
            job_dict = json.loads(data)
        except json.JSONDecodeError:
            msg = f"Stored job data for {job_name} is invalid JSON."
            logger.error(msg)
            resp.status = falcon.HTTP_500
            resp.media = {"error": msg}
            return

        resp.status = falcon.HTTP_200
        resp.media = job_dict

    def on_delete(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Delete the Job from Kubernetes and Redis."""
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

        # Also clean up Redis (optionally remove pods/events).
        redis_client.delete(job_redis_key(namespace, job_name))

        resp.status = falcon.HTTP_200
        resp.media = {
            "message": f"Job {job_name} deleted.",
            "status": delete_resp.to_dict(),
        }


class JobDescribeResource:
    """Returns a detailed 'describe'-like output for a Job from Redis, including pods and events."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Retrieve the Job object, associated Pods, and events from Redis."""
        # 1) Get the Job object
        job_bytes = redis_client.get(job_redis_key(namespace, job_name))
        if not job_bytes:
            msg = f"No job data found for {job_name} in Redis."
            logger.warning(msg)
            resp.status = falcon.HTTP_404
            resp.media = {"error": msg}
            return
        try:
            job_obj = json.loads(job_bytes)
        except json.JSONDecodeError:
            msg = f"Job data for {job_name} is invalid JSON."
            logger.error(msg)
            resp.status = falcon.HTTP_500
            resp.media = {"error": msg}
            return

        # 2) Get the list of pods for that job
        pods_set_key = job_pod_set_key(namespace, job_name)
        pod_names = redis_client.smembers(pods_set_key)  # bytes
        pods = []
        for pn in pod_names:
            pn_str = pn.decode("utf-8")
            pod_data = redis_client.get(pod_redis_key(namespace, pn_str))
            if not pod_data:
                continue
            try:
                pods.append(json.loads(pod_data))
            except json.JSONDecodeError:
                logger.error(f"Pod data for {pn_str} is invalid JSON.")

        # 3) Retrieve events for the Job
        job_event_key = event_redis_key(namespace, "Job", job_name)
        job_events_bytes = redis_client.get(job_event_key)
        if job_events_bytes:
            try:
                job_events = json.loads(job_events_bytes)
            except json.JSONDecodeError:
                job_events = []
        else:
            job_events = []

        # 4) Retrieve events for each Pod
        pods_events = {}
        for pod in pods:
            pod_name = pod["metadata"]["name"]
            pod_event_key = event_redis_key(namespace, "Pod", pod_name)
            pod_events_bytes = redis_client.get(pod_event_key)
            if pod_events_bytes:
                try:
                    pods_events[pod_name] = json.loads(pod_events_bytes)
                except json.JSONDecodeError:
                    pods_events[pod_name] = []
            else:
                pods_events[pod_name] = []

        describe_output = {
            "job": job_obj,
            "pods": pods,
            "events_for_job": job_events,
            "events_for_pods": pods_events,
        }

        resp.status = falcon.HTTP_200
        resp.media = describe_output


class JobLogsResource:
    """Return aggregated logs from Redis (if you have stored them)."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Retrieve the logs for a Job from Redis."""
        logs_key = job_logs_key(namespace, job_name)
        stored_logs = redis_client.get(logs_key)
        if not stored_logs:
            resp.status = falcon.HTTP_200
            resp.media = []
            return

        try:
            logs_data = json.loads(stored_logs.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logs_data = []
            logger.error(f"Failed to decode logs from Redis for key {logs_key}")

        resp.status = falcon.HTTP_200
        resp.media = logs_data


class LaunchResource:
    """Launch a Job from a JobTemplate, with optional command/args and file inputs."""

    def on_post(self, req: Request, resp: Response) -> None:
        """Launch a Job in Kubernetes."""
        # Parse request body
        body = req.media
        try:
            payload = LaunchJobModel(**body)
        except ValidationError as e:
            raise falcon.HTTPUnprocessableEntity(description=str(e))

        job_template = payload.jobTemplate

        # 1) Figure out namespace and name
        namespace = job_template["metadata"].get("namespace", "default")
        base_name = job_template["metadata"]["name"]
        random_suffix = generate_random_suffix()
        job_name = f"{base_name}-{random_suffix}"

        # 2) Generate an access token for the Job
        job_access_token = secrets.token_urlsafe(32)

        # 3) Store inputs/payload in Redis with a TTL
        if payload.inputs:
            redis_client.setex(
                job_inputs_key(namespace, job_name),
                REDIS_TTL,
                json.dumps(payload.inputs),
            )

        # Also store the token in Redis
        redis_client.setex(
            job_access_token_key(namespace, job_name),
            REDIS_TTL,
            job_access_token,
        )

        # 4) Inject only OPERATOR_ENDPOINT + the token (and job identity) into the Pod's env
        containers = job_template["spec"]["template"]["spec"]["containers"]
        for container in containers:
            env = container.setdefault("env", [])
            env.append({"name": "OPERATOR_ENDPOINT", "value": "http://my-operator-service:8080"})
            env.append({"name": "JOB_ACCESS_TOKEN", "value": job_access_token})
            env.append({"name": "JOB_NAMESPACE", "value": namespace})
            env.append({"name": "JOB_NAME", "value": job_name})

            # Optionally override command/args
            if payload.command:
                container["command"] = payload.command
            if payload.args:
                container["args"] = payload.args

        # 5) Create the Job in Kubernetes
        batch_api = kubernetes.client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=namespace, body=job_template)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create the job via K8s API.")
            raise falcon.HTTPInternalServerError(description=str(e))

        resp.status = falcon.HTTP_201
        resp.media = {"job_name": job_name, "namespace": namespace}


class JobPayloadResource:
    """The job pod can fetch its inputs by presenting its token to this endpoint."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Retrieve the payload for a Job (if the token is valid)."""
        # 1) Check for the token in the Authorization header: "Bearer <token>"
        auth_header = req.headers.get("AUTHORIZATION", "")
        if not auth_header.startswith("Bearer "):
            raise falcon.HTTPUnauthorized(description="Missing or invalid Authorization header.")
        provided_token = auth_header.split(" ", 1)[1]

        # 2) Look up the expected token in Redis
        token_key = job_access_token_key(namespace, job_name)
        stored_token = redis_client.get(token_key)
        if not stored_token:
            raise falcon.HTTPNotFound(description="No such job or token has expired.")

        if provided_token != stored_token.decode("utf-8"):
            raise falcon.HTTPForbidden(description="Invalid token.")

        # 3) If tokens match, retrieve the stored payload
        inputs_key = job_inputs_key(namespace, job_name)
        inputs_bytes = redis_client.get(inputs_key)
        if not inputs_bytes:
            resp.status = falcon.HTTP_200
            resp.media = {}
            return

        try:
            inputs_dict = json.loads(inputs_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise falcon.HTTPInternalServerError(description="Corrupted payload in Redis.")

        # 4) Return them
        resp.status = falcon.HTTP_200
        resp.media = inputs_dict
