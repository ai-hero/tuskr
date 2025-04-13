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

"""LaunchResource module. Handles POST /launch to create a Kubernetes Job from a JobTemplate using context data."""

import json
import logging
import os

import falcon
import kubernetes
from falcon import Request, Response

from helpers.constants import JOB_CONTEXT_PREFIX, TOKEN_PREFIX
from helpers.redis_client import redis_client
from helpers.utils import generate_random_suffix

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Paths to local Python scripts that will be embedded into the containers:
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INIT_FETCH_SCRIPT_PATH = os.path.join(BASE_DIR, "playout_init.py")
SIDECAR_SCRIPT_PATH = os.path.join(BASE_DIR, "playout_sidecar.py")


def load_local_script(script_path: str) -> str:
    """Read a local script and replace 'EOF' with 'E0F' to avoid heredoc collision."""
    with open(script_path, encoding="utf-8") as f:
        content = f.read()
    return content.replace("EOF", "E0F")


class LaunchResource:
    """Falcon resource handling POST /launch to create a Job from a JobTemplate."""

    def on_post(self, req: Request, resp: Response) -> None:
        """Handle POST requests to create a Job from a JobTemplate."""
        # --------------------------------------
        # 1) Parse incoming request data
        # --------------------------------------
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

        # --------------------------------------
        # 2) Retrieve the JobTemplate CRD
        # --------------------------------------
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
                msg = f"JobTemplate '{jobtemplate_name}' not found " f"in namespace '{jobtemplate_namespace}'."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            else:
                logger.exception("Unexpected error fetching JobTemplate.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

        # --------------------------------------
        # 3) Generate Job name & get base spec
        # --------------------------------------
        random_suffix = generate_random_suffix()
        job_name = f"{jobtemplate_name}-{random_suffix}"
        target_namespace = jobtemplate_namespace

        job_spec_from_template = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template", {})
        if not job_spec_from_template:
            msg = f"No 'spec.jobSpec.template' found in JobTemplate '{jobtemplate_name}'."
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.media = {"error": msg}
            return

        # --------------------------------------
        # 4) Save context data to Redis
        # --------------------------------------
        token = generate_random_suffix(length=20)
        context_key = f"{JOB_CONTEXT_PREFIX}:{jobtemplate_namespace}:{job_name}"
        context_data = {
            "env_vars": env_vars,
            "inputs": input_files,
        }
        # TTL = 1 hour
        redis_client.setex(context_key, 3600, json.dumps(context_data))

        token_key = f"{TOKEN_PREFIX}:{token}"
        token_data = {"namespace": jobtemplate_namespace, "job_name": job_name}
        redis_client.setex(token_key, 3600, json.dumps(token_data))

        # --------------------------------------
        # 5) Modify the Pod spec
        # --------------------------------------
        pod_spec = job_spec_from_template.get("spec", {})
        pod_spec["shareProcessNamespace"] = True
        containers = pod_spec.get("containers", [])

        # 5a) Add volumes for inputs/outputs
        volumes = [
            {"name": "inputs-volume", "emptyDir": {}},
            {"name": "outputs-volume", "emptyDir": {}},
        ]
        existing_volumes = pod_spec.get("volumes", [])
        pod_spec["volumes"] = existing_volumes + volumes

        # 5b) Main container overrides + environment
        if containers:
            # Override command/args if provided
            if command_override:
                containers[0]["command"] = command_override
            if args_override:
                containers[0]["args"] = args_override

            # Inject TUSKR_JOB_TOKEN into main container
            existing_env = containers[0].get("env", [])
            existing_env.append({"name": "TUSKR_JOB_TOKEN", "value": token})
            containers[0]["env"] = existing_env

            # Mount volumes in main container
            existing_mounts = containers[0].get("volumeMounts", [])
            existing_mounts += [
                {"name": "inputs-volume", "mountPath": "/mnt/data/inputs"},
                {"name": "outputs-volume", "mountPath": "/mnt/data/outputs"},
            ]
            containers[0]["volumeMounts"] = existing_mounts

        # --------------------------------------
        # 6) Create init container (Python-based)
        # --------------------------------------
        init_fetch_script_content = load_local_script(INIT_FETCH_SCRIPT_PATH)

        init_containers = [
            {
                "name": "playout-init",
                "image": "python:3.9-slim",
                "command": ["sh", "-c"],
                "args": [
                    f"""set -ex
                    # Write the Python script into a file
                    cat <<'__PY__' > /init_fetch.py
{init_fetch_script_content}
__PY__

                    chmod +x /init_fetch.py

                    # Install dependencies needed by init_fetch.py
                    pip install --no-cache-dir httpx

                    chmod -R 755 /mnt/data/inputs
                    chmod -R 755 /mnt/data/outputs

                    # Run the script
                    python /init_fetch.py
                    """
                ],
                "env": [
                    {"name": "NAMESPACE", "value": jobtemplate_namespace},
                    {"name": "JOB_NAME", "value": job_name},
                    {"name": "TUSKR_JOB_TOKEN", "value": token},
                ],
                "volumeMounts": [
                    {"name": "inputs-volume", "mountPath": "/mnt/data/inputs"},
                ],
            }
        ]
        pod_spec["initContainers"] = init_containers

        # --------------------------------------
        # 7) Create sidecar container (Python-based)
        # --------------------------------------
        sidecar_script_content = load_local_script(SIDECAR_SCRIPT_PATH)

        sidecar_container = {
            "name": "playout-sidecar",
            "image": "python:3.9-slim",
            "command": ["sh", "-c"],
            "args": [
                f"""set -ex
                # Write the Python script into a file
                cat <<'__SIDE__' > /sidecar.py
{sidecar_script_content}
__SIDE__

                chmod +x /sidecar.py

                # Install dependencies
                pip install --no-cache-dir httpx kubernetes

                # Run the script
                python /sidecar.py
                """
            ],
            "env": [
                {"name": "NAMESPACE", "value": jobtemplate_namespace},
                {"name": "JOB_NAME", "value": job_name},
                {"name": "TUSKR_JOB_TOKEN", "value": token},
                {
                    "name": "POD_NAME",
                    "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
                },
            ],
            "volumeMounts": [
                {"name": "outputs-volume", "mountPath": "/mnt/data/outputs"},
            ],
        }
        containers.append(sidecar_container)
        pod_spec["containers"] = containers

        # --------------------------------------
        # 8) Construct and create the Job
        # --------------------------------------
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
                "ttlSecondsAfterFinished": 3 * 60 * 60,  # 3-hour cleanup
                "backoffLimit": 0,  # No retries
            },
        }

        batch_api = kubernetes.client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=target_namespace, body=job_body)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create Job.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        # (Optional) If there's a callback param, store it in Redis
        callback_url = req.get_param("callback")
        if callback_url:
            callback_key = f"job_callbacks::{target_namespace}::{job_name}"
            callback_info = {"url": callback_url}
            # If there's an AUTHORIZATION env var, store it
            if env_vars.get("AUTHORIZATION"):
                callback_info["authorization"] = env_vars["AUTHORIZATION"]
            redis_client.setex(callback_key, 3600, json.dumps(callback_info))

        msg = f"Created Job '{job_name}' in '{target_namespace}' from template '{jobtemplate_name}'."
        logger.info(msg)

        # Return the token if needed
        resp.status = falcon.HTTP_201
        resp.media = {
            "message": msg,
            "job_name": job_name,
            "namespace": target_namespace,
            "token": token,
        }
