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
from typing import Any

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
        """Handle POST /launch and create a Job from a JobTemplate."""
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
        jobtemplate_ns = jobtemplate_info.get("namespace")
        command_override = data.get("command", [])
        args_override = data.get("args", [])
        env_vars = data.get("env_vars", {})  # <‑‑ now injected directly
        input_files = data.get("inputs", {})

        if not jobtemplate_name or not jobtemplate_ns:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Must provide 'jobTemplate.name' and 'jobTemplate.namespace'."}
            return

        # --------------------------------------
        # 2) Fetch the JobTemplate CR
        # --------------------------------------
        crd_api = kubernetes.client.CustomObjectsApi()
        try:
            jobtemplate = crd_api.get_namespaced_custom_object(
                group="tuskr.io",
                version="v1alpha1",
                namespace=jobtemplate_ns,
                plural="jobtemplates",
                name=jobtemplate_name,
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                msg = f"JobTemplate '{jobtemplate_name}' not found in namespace '{jobtemplate_ns}'."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            logger.exception("Unexpected error fetching JobTemplate.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        # --------------------------------------
        # 3) Build Job name & pull base Pod spec
        # --------------------------------------
        random_suffix = generate_random_suffix()
        job_name = f"{jobtemplate_name}-{random_suffix}"

        pod_tmpl = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template", {})
        if not pod_tmpl:
            msg = f"No 'spec.jobSpec.template' found in JobTemplate '{jobtemplate_name}'."
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.media = {"error": msg}
            return

        # --------------------------------------
        # 4) Persist context & token to Redis
        # --------------------------------------
        token = generate_random_suffix(length=20)
        context_key = f"{JOB_CONTEXT_PREFIX}:{jobtemplate_ns}:{job_name}"
        token_key = f"{TOKEN_PREFIX}:{token}"

        context_data = {
            "inputs": input_files,
        }
        redis_client.setex(context_key, 3600, json.dumps(context_data))
        redis_client.setex(token_key, 3600, json.dumps({"namespace": jobtemplate_ns, "job_name": job_name}))

        # --------------------------------------
        # 5) Prepare Pod spec – shared helper
        # --------------------------------------
        pod_spec = pod_tmpl.get("spec", {})
        pod_spec["shareProcessNamespace"] = True
        containers = pod_spec.get("containers", [])

        def inject_env(existing: list[dict[str, Any]], extra: dict[str, str]) -> list[dict[str, Any]]:
            """Merge/override environment variables."""
            merged = {e["name"]: e for e in existing}
            for k, v in extra.items():
                merged[k] = {"name": k, "value": str(v)}
            return list(merged.values())

        # 5a) Volumes for IO – preserve existing volumes and add missing ones
        volumes = pod_spec.get("volumes", [])

        def add_unique_volume(volumes: list[dict[str, Any]], new_vol: dict[str, Any]) -> None:
            if not any(v.get("name") == new_vol["name"] for v in volumes):
                volumes.append(new_vol)

        add_unique_volume(volumes, {"name": "playout-inputs", "emptyDir": {}})
        add_unique_volume(volumes, {"name": "playout-outputs", "emptyDir": {}})
        pod_spec["volumes"] = volumes

        # 5b) MAIN container tweaks – keep existing env, envFrom and volumeMounts; add missing mounts
        if containers:
            if command_override:
                containers[0]["command"] = command_override
            if args_override:
                containers[0]["args"] = args_override
            containers[0]["env"] = inject_env(containers[0].get("env", []), {"TUSKR_JOB_TOKEN": token, **env_vars})
            existing_env_from = containers[0].get("envFrom", [])
            if existing_env_from:
                containers[0]["envFrom"] = existing_env_from
            # Note: any pre-existing 'envFrom' is not modified
            volume_mounts = containers[0].get("volumeMounts", [])

            def add_unique_mount(mounts: list[dict[str, Any]], new_mount: dict[str, Any]) -> None:
                if not any(m.get("name") == new_mount["name"] for m in mounts):
                    mounts.append(new_mount)

            add_unique_mount(volume_mounts, {"name": "playout-inputs", "mountPath": "/mnt/data/inputs"})
            add_unique_mount(volume_mounts, {"name": "playout-outputs", "mountPath": "/mnt/data/outputs"})
            containers[0]["volumeMounts"] = volume_mounts

        # --------------------------------------
        # 6) Init‑container: playout_init
        # --------------------------------------
        init_containers = [
            {
                "name": "playout-init",
                "image": "python:3.9-slim",
                "command": ["sh", "-c"],
                "args": [
                    f"""set -ex
cat <<'__PY__' >/playout_init.py
{load_local_script(INIT_FETCH_SCRIPT_PATH)}
__PY__
chmod +x /playout_init.py
pip install --no-cache-dir httpx
python /playout_init.py
chmod -R 777 /mnt/data/inputs /mnt/data/outputs
"""
                ],
                "env": inject_env(
                    [
                        {"name": "NAMESPACE", "value": jobtemplate_ns},
                        {"name": "JOB_NAME", "value": job_name},
                        {"name": "TUSKR_JOB_TOKEN", "value": token},
                    ],
                    env_vars,  # give it the same extras in case it needs them
                ),
                "volumeMounts": [
                    {"name": "playout-inputs", "mountPath": "/mnt/data/inputs"},
                    {"name": "playout-outputs", "mountPath": "/mnt/data/outputs"},
                ],
            }
        ]
        pod_spec["initContainers"] = init_containers

        # --------------------------------------
        # 7) Sidecar container: playout_sidecar
        # --------------------------------------
        sidecar_container = {
            "name": "playout-sidecar",
            "image": "python:3.9-slim",
            "command": ["sh", "-c"],
            "args": [
                f"""set -ex
cat <<'__PY__' >/playout_sidecar.py
{load_local_script(SIDECAR_SCRIPT_PATH)}
__PY__
chmod +x /playout_sidecar.py
pip install --no-cache-dir httpx psutil
python /playout_sidecar.py
"""
            ],
            "env": inject_env(
                [
                    {"name": "NAMESPACE", "value": jobtemplate_ns},
                    {"name": "JOB_NAME", "value": job_name},
                    {"name": "TUSKR_JOB_TOKEN", "value": token},
                    {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                ],
                env_vars,
            ),
            "volumeMounts": [
                {"name": "playout-outputs", "mountPath": "/mnt/data/outputs"},
            ],
        }
        containers.append(sidecar_container)
        pod_spec["containers"] = containers

        # --------------------------------------
        # 8) Create the Job
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
                "template": pod_tmpl,
                "ttlSecondsAfterFinished": 600,
                "backoffLimit": 0,
            },
        }

        print(f"Job body: {json.dumps(job_body, indent=2)}")

        batch_api = kubernetes.client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=jobtemplate_ns, body=job_body)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create Job.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        # Optional callback
        callback_url = req.get_param("callback")
        if callback_url:
            callback_key = f"job_callbacks::{jobtemplate_ns}::{job_name}"
            cb_info = {"url": callback_url}
            if env_vars.get("AUTHORIZATION"):
                cb_info["authorization"] = env_vars["AUTHORIZATION"]
            redis_client.setex(callback_key, 3600, json.dumps(cb_info))

        msg = f"Created Job '{job_name}' in '{jobtemplate_ns}' from template '{jobtemplate_name}'."
        logger.info(msg)

        resp.status = falcon.HTTP_201
        resp.media = {
            "message": msg,
            "job_name": job_name,
            "namespace": jobtemplate_ns,
            "token": token,
        }
