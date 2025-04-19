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

"""LaunchResource module ‑ updated with a thin wrapper that sources .env for every runtime container.

Key changes
-----------
* The init‑container now writes **/mnt/data/run_with_env.sh** into the shared *emptyDir* volume. The
  script:
    1. `set -a` to export every variable it sources.
    2. sources `/mnt/data/inputs/.env` if it exists (plain `KEY=VALUE` lines).
    3. `exec "$@"` ‑‑ handing control to the real entrypoint / command while inheriting the env‑vars.
* For each runtime container we prepend that wrapper to its existing `command:` list. If the
  container did **not** specify a custom `command` in the JobTemplate we leave it untouched (Kubernetes
  will then run the image’s default ENTRYPOINT, which we can’t know ahead of time). In that case the
  application itself should read the env file (e.g. with **python‑dotenv**) if it needs the values.
* The side‑car container is also wrapped so it inherits the same environment.
"""

import json
import logging
import os
from typing import List

import falcon
import kubernetes
from falcon import Request, Response

from helpers.constants import JOB_CONTEXT_PREFIX, TOKEN_PREFIX
from helpers.redis_client import redis_client
from helpers.utils import generate_random_suffix

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Paths to local Python scripts that will be embedded into the containers
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INIT_FETCH_SCRIPT_PATH = os.path.join(BASE_DIR, "playout_init.py")
SIDECAR_SCRIPT_PATH = os.path.join(BASE_DIR, "playout_sidecar.py")

# Location of the runtime wrapper written by the init‑container
WRAPPER_PATH = "/mnt/data/run_with_env.sh"


def load_local_script(script_path: str) -> str:
    """Read a local script and replace 'EOF' with 'E0F' to avoid heredoc collision."""
    with open(script_path, encoding="utf-8") as f:
        content = f.read()
    return content.replace("EOF", "E0F")


class LaunchResource:
    """Falcon resource handling POST /launch to create a Job from a JobTemplate."""

    def on_post(self, req: Request, resp: Response) -> None:  # noqa: C901 – method is long but clear
        """Handle POST request to create a Job from a JobTemplate."""
        # --------------------------------------
        # 1) Parse incoming request data
        # --------------------------------------
        try:
            data = req.media
        except Exception as e:  # pragma: no cover – malformed JSON
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"Invalid request format: {str(e)}"}
            return

        # Required fields from the request
        jobtemplate_info = data.get("jobTemplate", {})
        jobtemplate_name = jobtemplate_info.get("name")
        jobtemplate_namespace = jobtemplate_info.get("namespace")
        command_override: List[str] = data.get("command", [])
        args_override: List[str] = data.get("args", [])
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
        # 4) Save context data to Redis (TTL 1 h)
        # --------------------------------------
        token = generate_random_suffix(length=20)
        context_key = f"{JOB_CONTEXT_PREFIX}:{jobtemplate_namespace}:{job_name}"
        redis_client.setex(context_key, 3600, json.dumps({"env_vars": env_vars, "inputs": input_files}))

        token_key = f"{TOKEN_PREFIX}:{token}"
        redis_client.setex(token_key, 3600, json.dumps({"namespace": jobtemplate_namespace, "job_name": job_name}))

        # --------------------------------------
        # 5) Mutate the Pod spec from the template
        # --------------------------------------
        pod_spec = job_spec_from_template.get("spec", {})
        pod_spec["shareProcessNamespace"] = True
        containers = pod_spec.get("containers", [])

        # 5a) Volumes for inputs and outputs
        pod_spec["volumes"] = pod_spec.get("volumes", []) + [
            {"name": "inputs-volume", "emptyDir": {}},
            {"name": "outputs-volume", "emptyDir": {}},
        ]

        # 5b) Main container overrides & env injection
        if containers:
            main = containers[0]

            # Apply command/args overrides from the request (optional)
            if command_override:
                main["command"] = command_override
            if args_override:
                main["args"] = args_override

            # Add the job token so the side‑car has context
            main.setdefault("env", []).append({"name": "TUSKR_JOB_TOKEN", "value": token})

            # Mount shared volumes
            main.setdefault("volumeMounts", []).extend(
                [
                    {"name": "inputs-volume", "mountPath": "/mnt/data"},
                    {"name": "outputs-volume", "mountPath": "/mnt/data/outputs"},
                ]
            )

        # --------------------------------------
        # 6) Init‑container: fetch inputs **and** drop the wrapper script
        # --------------------------------------
        init_fetch_script = load_local_script(INIT_FETCH_SCRIPT_PATH)

        init_containers = [
            {
                "name": "playout-init",
                "image": "python:3.9-slim",
                "command": ["sh", "-c"],
                "args": [
                    f"""set -ex
                    # --- python helper that pulls env / inputs from redis ---
                    cat <<'__PY__' > /playout_init.py
{init_fetch_script}
__PY__
                    chmod +x /playout_init.py
                    pip install --no-cache-dir httpx
                    python /playout_init.py

                    # --- write the thin wrapper so runtime containers inherit the env ---
                    cat <<'__WRAP__' > {WRAPPER_PATH}
#!/bin/sh
set -a                          # export every key=value we source
[ -f /mnt/data/inputs/.env ] && . /mnt/data/inputs/.env
set +a
exec "$@"                     # jump to the real entrypoint/command
__WRAP__
                    chmod +x {WRAPPER_PATH}

                    chmod -R 777 /mnt/data /mnt/data/inputs /mnt/data/outputs
                    """
                ],
                "env": [
                    {"name": "NAMESPACE", "value": jobtemplate_namespace},
                    {"name": "JOB_NAME", "value": job_name},
                    {"name": "TUSKR_JOB_TOKEN", "value": token},
                ],
                "volumeMounts": [
                    {"name": "inputs-volume", "mountPath": "/mnt/data"},
                    {"name": "outputs-volume", "mountPath": "/mnt/data/outputs"},
                ],
            }
        ]
        pod_spec["initContainers"] = init_containers

        # --------------------------------------
        # 7) Side‑car container (also wrapped)
        # --------------------------------------
        sidecar_py = load_local_script(SIDECAR_SCRIPT_PATH)

        sidecar_container = {
            "name": "playout-sidecar",
            "image": "python:3.9-slim",
            # We "wrap" its original entrypoint (python /playout_sidecar.py) with the env loader
            "command": [WRAPPER_PATH, "python", "/playout_sidecar.py"],
            "args": [],
            "env": [
                {"name": "NAMESPACE", "value": jobtemplate_namespace},
                {"name": "JOB_NAME", "value": job_name},
                {"name": "TUSKR_JOB_TOKEN", "value": token},
                {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            ],
            "volumeMounts": [
                {"name": "outputs-volume", "mountPath": "/mnt/data/outputs"},
            ],
        }

        # Side‑car needs its python script – we embed via an ENTRYPOINT shell on first run.
        # The wrapper will execute "python /playout_sidecar.py" but the file doesn’t exist yet.
        # Therefore we preload it via an *init* style command at container start (here using args):
        sidecar_container["args"] = [
            "sh",
            "-c",
            f"""set -ex
            cat <<'__SIDE__' > /playout_sidecar.py
{sidecar_py}
__SIDE__
            chmod +x /playout_sidecar.py
            pip install --no-cache-dir httpx psutil
            exec \"$@\"   # jump back to wrapper's python call
            """,
        ]

        containers.append(sidecar_container)
        pod_spec["containers"] = containers

        # --------------------------------------
        # 8) Prepend the wrapper to any container that *already* defines a command
        # --------------------------------------
        for c in containers:
            if c.get("name") == "playout-sidecar":
                continue  # already wrapped above
            original_cmd = c.get("command", [])
            if original_cmd:  # Only safe if we know what to exec
                c["command"] = [WRAPPER_PATH] + original_cmd

        # --------------------------------------
        # 9) Create the Job object
        # --------------------------------------
        job_body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {"jobtemplate": jobtemplate_name},
                "annotations": {"tuskr.io/launched-by": "tuskr"},
            },
            "spec": {
                "template": job_spec_from_template,
                "ttlSecondsAfterFinished": 3 * 60 * 60,  # 3 h cleanup
                "backoffLimit": 0,
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

        # --------------------------------------
        # 10) Optional: store callback URL if provided
        # --------------------------------------
        callback_url = req.get_param("callback")
        if callback_url:
            callback_key = f"job_callbacks::{target_namespace}::{job_name}"
            callback_info = {"url": callback_url}
            if env_vars.get("AUTHORIZATION"):
                callback_info["authorization"] = env_vars["AUTHORIZATION"]
            redis_client.setex(callback_key, 3600, json.dumps(callback_info))

        logger.info("Created Job '%s' in '%s' from template '%s'.", job_name, target_namespace, jobtemplate_name)

        resp.status = falcon.HTTP_201
        resp.media = {
            "message": f"Created Job '{job_name}' in '{target_namespace}' from template '{jobtemplate_name}'.",
            "job_name": job_name,
            "namespace": target_namespace,
            "token": token,
        }
