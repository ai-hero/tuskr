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

"""Helper module for launching jobs."""

import json
import logging

import falcon
import kubernetes
from falcon import Request, Response

from helpers.constants import JOB_CONTEXT_PREFIX, TOKEN_PREFIX
from helpers.redis_client import redis_client
from helpers.utils import generate_random_suffix

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LaunchResource:
    """Falcon resource that handles POST /launch to create a Job from a JobTemplate."""

    def on_post(self, req: Request, resp: Response) -> None:
        """Create a Job from a JobTemplate, storing inputs/env-vars in Redis.

        and passing only a temporary token to the job container.
        """
        try:
            data = req.media
        except Exception as e:
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"Invalid request format: {str(e)}"}
            return

        jobtemplate_info = data.get("jobTemplate", {})
        jobtemplate_name = jobtemplate_info.get("name")
        jobtemplate_namespace = jobtemplate_info.get("namespace")
        command_override = data.get("command", [])
        args_override = data.get("args", [])
        env_vars = data.get("env_vars", {})
        input_files = data.get("inputs", {})

        if not jobtemplate_name or not jobtemplate_namespace:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Must provide 'jobTemplate.name' and 'jobTemplate.namespace'."}
            return

        # Retrieve the JobTemplate (CRD) from cluster
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

        # Generate the final job name
        random_suffix = generate_random_suffix()
        job_name = f"{jobtemplate_name}-{random_suffix}"
        target_namespace = jobtemplate_namespace

        # Obtain the base job spec from the CRD
        job_spec_from_template = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template", {})
        if not job_spec_from_template:
            msg = f"No 'spec.jobSpec.template' found in JobTemplate {jobtemplate_name}"
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.media = {"error": msg}
            return

        # Save inputs/env-vars into Redis under a unique key
        # We'll also generate a random token. The job can use this token to fetch from /context.
        token = generate_random_suffix(length=20)  # longer token
        context_key = f"{JOB_CONTEXT_PREFIX}:{jobtemplate_namespace}:{job_name}"
        context_data = {
            "env_vars": env_vars,
            "inputs": input_files,
        }
        # Store in Redis with TTL = 60 minutes
        redis_client.setex(context_key, 3600, json.dumps(context_data))

        # Map the token -> that specific context key
        token_key = f"{TOKEN_PREFIX}:{token}"
        token_data = {"namespace": jobtemplate_namespace, "job_name": job_name}
        redis_client.setex(token_key, 3600, json.dumps(token_data))

        # Merge command/args into container spec
        pod_spec = job_spec_from_template.get("spec", {})
        containers = pod_spec.get("containers", [])
        if containers:
            # If user provided a custom command/args, override
            if command_override:
                containers[0]["command"] = command_override
            if args_override:
                containers[0]["args"] = args_override

            # Inject the job token as an environment variable
            existing_env = containers[0].get("env", [])
            existing_env.append({"name": "TUSKR_JOB_TOKEN", "value": token})
            containers[0]["env"] = existing_env

        # Construct the final Job manifest
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
                "ttlSecondsAfterFinished": 60 * 60 * 3,  # 3-hour cleanup
                "backoffLimit": 0,
            },
        }

        # Create the Job in the cluster
        batch_api = kubernetes.client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=target_namespace, body=job_body)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create Job.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        # Optional callback logic if you have a callback param
        callback_url = req.get_param("callback")
        if callback_url:
            # Store callback info in Redis for job watchers
            callback_key = f"job_callbacks::{target_namespace}::{job_name}"
            callback_info = {"url": callback_url}
            redis_client.setex(callback_key, 3600, json.dumps(callback_info))

        msg = f"Created Job '{job_name}' in '{target_namespace}' from template '{jobtemplate_name}'."
        logger.info(msg)

        resp.status = falcon.HTTP_201
        resp.media = {
            "message": msg,
            "job_name": job_name,
            "namespace": target_namespace,
            "token": token,  # Return token if you wish the client to know
        }
