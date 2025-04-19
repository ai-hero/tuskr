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
from typing import Any, List

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
ENTRYPOINT_SCRIPT_PATH = os.path.join(BASE_DIR, "playout_entrypoint.sh")
ENTRYPOINT_PATH = "/mnt/data/playout_entrypoint.sh"


def load_local_script(script_path: str) -> str:
    """Read a local script and replace 'EOF' with 'E0F' to avoid heredoc collision."""
    with open(script_path, encoding="utf-8") as f:
        content = f.read()
    return content.replace("EOF", "E0F")


class LaunchResource:
    """Falcon resource handling POST /launch to create a Job from a JobTemplate."""

    def on_post(self, req: Request, resp: Response) -> None:  # noqa: C901 – long but explicit
        """Handle POST /launch ‑– create a batch/v1 Job from a JobTemplate.

        Handle POST /launch ‑– create a batch/v1 Job from a JobTemplate, inject a
        thin wrapper that sources /mnt/data/inputs/.env for every runtime
        container, and start background polling threads.

        The tricky bit: the wrapper script is written by the *init‑container*
        into an emptyDir volume; therefore every *runtime* container must invoke
        it **from** a binary that already lives inside the image (e.g. /bin/sh),
        otherwise runc will try to exec it before the volume is mounted and the
        container will fail with ENOENT.
        """
        import shlex  # local import to avoid polluting the module namespace

        # ------------------------------------------------------------------
        # 1) Parse and validate the incoming request
        # ------------------------------------------------------------------
        try:
            data = req.media
        except Exception as e:  # pragma: no cover – malformed JSON
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"Invalid request format: {str(e)}"}
            return

        jt_info = data.get("jobTemplate", {})
        jt_name = jt_info.get("name")
        jt_ns = jt_info.get("namespace")
        command_override: List[str] = data.get("command", [])
        args_override: List[str] = data.get("args", [])
        env_vars = data.get("env_vars", {})
        input_files = data.get("inputs", {})

        if not jt_name or not jt_ns:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Must provide 'jobTemplate.name' and 'jobTemplate.namespace'."}
            return

        # ------------------------------------------------------------------
        # 2) Fetch the JobTemplate CRD
        # ------------------------------------------------------------------
        crd_api = kubernetes.client.CustomObjectsApi()
        try:
            jobtemplate = crd_api.get_namespaced_custom_object(
                group="tuskr.io",
                version="v1alpha1",
                namespace=jt_ns,
                plural="jobtemplates",
                name=jt_name,
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                resp.status = falcon.HTTP_404
                resp.media = {"error": f"JobTemplate '{jt_name}' not found in '{jt_ns}'."}
                return
            logger.exception("Unexpected error fetching JobTemplate.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        # ------------------------------------------------------------------
        # 3) Basic book‑keeping (Job name, token, Redis context, …)
        # ------------------------------------------------------------------
        random_suffix = generate_random_suffix()
        job_name = f"{jt_name}-{random_suffix}"

        jt_pod_template = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template")
        if not jt_pod_template:
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"No 'spec.jobSpec.template' in JobTemplate '{jt_name}'."}
            return

        # Redis context (TTL 1 h)
        token = generate_random_suffix(length=20)
        ctx_key = f"{JOB_CONTEXT_PREFIX}:{jt_ns}:{job_name}"
        redis_client.setex(ctx_key, 3600, json.dumps({"env_vars": env_vars, "inputs": input_files}))
        token_key = f"{TOKEN_PREFIX}:{token}"
        redis_client.setex(token_key, 3600, json.dumps({"namespace": jt_ns, "job_name": job_name}))

        # ------------------------------------------------------------------
        # 4) Mutate the Pod spec
        # ------------------------------------------------------------------
        pod_spec = jt_pod_template.setdefault("spec", {})
        pod_spec["shareProcessNamespace"] = True
        containers: list[dict[str, Any]] = pod_spec.get("containers", [])

        # 4a) Shared emptyDir
        pod_spec["volumes"] = pod_spec.get("volumes", []) + [{"name": "data-volume", "emptyDir": {}}]

        # 4b) Mutate the *main* container (first in list) if present
        if containers:
            main = containers[0]

            if command_override:
                main["command"] = command_override
            if args_override:
                main["args"] = args_override

            main.setdefault("env", []).append({"name": "TUSKR_JOB_TOKEN", "value": token})
            main.setdefault("volumeMounts", []).append({"name": "data-volume", "mountPath": "/mnt/data"})

        # ------------------------------------------------------------------
        # 5) Init‑container: fetch inputs + emit the wrapper script
        # ------------------------------------------------------------------
        init_fetch_script = load_local_script(INIT_FETCH_SCRIPT_PATH)
        entrypoint_script = load_local_script(ENTRYPOINT_SCRIPT_PATH)

        init_container = {
            "name": "playout-init",
            "image": "python:3.9-slim",
            "command": ["sh", "-c"],
            "args": [
                f"""
        set -ex

        # 1) Python helper that fetches inputs/outputs
        cat <<'__PY__' > /playout_init.py
{init_fetch_script}
__PY__

        # 2) The unified wrapper / entry‑point
        cat <<'__ENT__' > {ENTRYPOINT_PATH}
{entrypoint_script}
__ENT__
        chmod +x {ENTRYPOINT_PATH}

        # 3) Run the Python init code
        pip install --no-cache-dir httpx
        python /playout_init.py

        # 4) Fix permissions for downstream containers
        chmod -R 777 /mnt/data /mnt/data/inputs /mnt/data/outputs
        """
            ],
            "env": [
                {"name": "NAMESPACE", "value": jt_ns},
                {"name": "JOB_NAME", "value": job_name},
                {"name": "TUSKR_JOB_TOKEN", "value": token},
            ],
            "volumeMounts": [{"name": "data-volume", "mountPath": "/mnt/data"}],
        }
        pod_spec["initContainers"] = [init_container]

        # ------------------------------------------------------------------
        # 6) Side‑car container (invokes the wrapper *via* /bin/sh)
        # ------------------------------------------------------------------
        sidecar_py = load_local_script(SIDECAR_SCRIPT_PATH)
        sidecar_container = {
            "name": "playout-sidecar",
            "image": "python:3.9-slim",
            "command": [
                "/bin/sh",
                "-c",
                f"""set -ex
cat <<'__SIDE__' > /playout_sidecar.py
{sidecar_py}
__SIDE__
chmod +x /playout_sidecar.py
pip install --no-cache-dir httpx psutil
exec {ENTRYPOINT_PATH} python /playout_sidecar.py
""",
            ],
            "env": [
                {"name": "NAMESPACE", "value": jt_ns},
                {"name": "JOB_NAME", "value": job_name},
                {"name": "TUSKR_JOB_TOKEN", "value": token},
                {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            ],
            "volumeMounts": [{"name": "data-volume", "mountPath": "/mnt/data"}],
        }
        containers.append(sidecar_container)

        # ------------------------------------------------------------------
        # 7) Wrap every *other* container that already declares a command
        # ------------------------------------------------------------------
        for c in containers:
            if c["name"] == "playout-sidecar":
                continue
            original_cmd = c.get("command") or []
            if not original_cmd:  # image ENTRYPOINT – leave untouched
                continue
            joined = " ".join(shlex.quote(tok) for tok in original_cmd)
            c["command"] = ["/bin/sh", "-c", f"exec {ENTRYPOINT_PATH} {joined}"]

        pod_spec["containers"] = containers

        # ------------------------------------------------------------------
        # 8) Create and submit the Job
        # ------------------------------------------------------------------
        job_body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {"jobtemplate": jt_name},
                "annotations": {"tuskr.io/launched-by": "tuskr"},
            },
            "spec": {
                "template": jt_pod_template,
                "ttlSecondsAfterFinished": 3 * 60 * 60,
                "backoffLimit": 0,
            },
        }

        try:
            kubernetes.client.BatchV1Api().create_namespaced_job(namespace=jt_ns, body=job_body)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create Job.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        # Optional callback registration
        if cb := req.get_param("callback"):
            cb_key = f"job_callbacks::{jt_ns}::{job_name}"
            cb_info = {"url": cb}
            if env_vars.get("AUTHORIZATION"):
                cb_info["authorization"] = env_vars["AUTHORIZATION"]
            redis_client.setex(cb_key, 3600, json.dumps(cb_info))

        logger.info("Created Job '%s' in '%s' from template '%s'.", job_name, jt_ns, jt_name)
        resp.status = falcon.HTTP_201
        resp.media = {
            "message": f"Created Job '{job_name}' in '{jt_ns}' from template '{jt_name}'.",
            "job_name": job_name,
            "namespace": jt_ns,
            "token": token,
        }
