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

# MIT License
#
# Copyright (c) 2025 A.I. Hero, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of the
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
# OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""LaunchResource module.

This Falcon resource handles POST /launch to create a Kubernetes Job from a JobTemplate
while storing and retrieving context data (env-vars, inputs, outputs) via the
JobContextResource.
"""

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
    """Falcon resource that handles POST /launch to create a Job from a JobTemplate.

    1) Retrieves the JobTemplate via CustomObjectsApi.
    2) Generates a unique job_name using the template name and a random suffix.
    3) Stores env vars and inputs in Redis under a "context" key.
    4) Injects an init container to fetch context (env_vars/inputs).
    5) Adds a sidecar container that waits for non-"playout-" containers to finish,
       then gathers and posts outputs back to the context endpoint.
    6) Finally, creates the Job in the specified namespace.
    """

    def on_post(self, req: Request, resp: Response) -> None:
        """Handle POST requests to create a Job from a JobTemplate."""
        try:
            data = req.media
        except Exception as e:
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"Invalid request format: {str(e)}"}
            return

        # --- Extract request data ---
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

        # --- Retrieve the JobTemplate (CRD) from the cluster ---
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
                msg = f"JobTemplate '{jobtemplate_name}' not found in namespace " f"'{jobtemplate_namespace}'."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            else:
                logger.exception("Unexpected error fetching JobTemplate.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

        # --- Generate the final job name ---
        random_suffix = generate_random_suffix()
        job_name = f"{jobtemplate_name}-{random_suffix}"
        target_namespace = jobtemplate_namespace

        # --- Obtain the base Job spec from the JobTemplate CRD ---
        job_spec_from_template = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template", {})
        if not job_spec_from_template:
            msg = f"No 'spec.jobSpec.template' found in JobTemplate " f"'{jobtemplate_name}'."
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.media = {"error": msg}
            return

        # --- Create a token + store context (env_vars, inputs) in Redis ---
        token = generate_random_suffix(length=20)  # a longer random token
        context_key = f"{JOB_CONTEXT_PREFIX}:{jobtemplate_namespace}:{job_name}"
        context_data = {"env_vars": env_vars, "inputs": input_files}

        # Store context data in Redis with TTL = 60 minutes
        redis_client.setex(context_key, 3600, json.dumps(context_data))

        # Map the token to (namespace, job_name) for validation
        token_key = f"{TOKEN_PREFIX}:{token}"
        token_data = {"namespace": jobtemplate_namespace, "job_name": job_name}
        redis_client.setex(token_key, 3600, json.dumps(token_data))

        # --- Modify the Pod spec to add volumes, initContainer, sidecar, etc. ---
        pod_spec = job_spec_from_template.get("spec", {})
        containers = pod_spec.get("containers", [])

        # 1) Create volumes for inputs and outputs
        volumes = [
            {"name": "inputs-volume", "emptyDir": {}},
            {"name": "outputs-volume", "emptyDir": {}},
        ]
        pod_spec["volumes"] = pod_spec.get("volumes", []) + volumes

        # 2) For the first container, override command/args if provided, inject token,
        #    and mount volumes.
        if containers:
            if command_override:
                containers[0]["command"] = command_override
            if args_override:
                containers[0]["args"] = args_override

            # Inject TUSKR_JOB_TOKEN as an env var
            existing_env = containers[0].get("env", [])
            existing_env.append({"name": "TUSKR_JOB_TOKEN", "value": token})
            containers[0]["env"] = existing_env

            # Mount volumes into the main container
            existing_mounts = containers[0].get("volumeMounts", [])
            existing_mounts += [
                {"name": "inputs-volume", "mountPath": "/mnt/data/inputs"},
                {"name": "outputs-volume", "mountPath": "/mnt/data/outputs"},
            ]
            containers[0]["volumeMounts"] = existing_mounts

        # 3) Create an init container to fetch the context (env_vars + inputs) from the API
        init_script = (
            "set -e\n"
            "chown -R 1000:1000 /mnt/data\n"
            "apk add --no-cache curl jq\n\n"
            'echo "Fetching context..."\n'
            "HTTP_CODE=$(curl -s -o /tmp/context.json -w '%{http_code}' \\\n"
            '  "http://tuskr-controller.tuskr.svc.cluster.local:8080/jobs/$NAMESPACE/" \\\n'
            '  "$JOB_NAME/context?token=$TUSKR_JOB_TOKEN")\n\n'
            'if [ "$HTTP_CODE" -ne 200 ]; then\n'
            '  echo "Error: Failed to fetch context. HTTP code: $HTTP_CODE" >&2\n'
            "  exit 1\n"
            "fi\n\n"
            "mkdir -p /mnt/data/inputs\n\n"
            "for filename in $(jq -r '.inputs | keys[]' /tmp/context.json); do\n"
            '  content=$(jq -r ".inputs[\\"$filename\\"]" /tmp/context.json)\n'
            '  echo "$content" > "/mnt/data/inputs/$filename"\n'
            '  echo "Input saved: $filename"\n'
            "done\n\n"
            "touch /mnt/data/inputs/.env\n"
            "for varname in $(jq -r '.env_vars | keys[]' /tmp/context.json); do\n"
            '  value=$(jq -r ".env_vars[\\"$varname\\"]" /tmp/context.json)\n'
            '  echo "export $varname=\\"$value\\"" >> /mnt/data/inputs/.env\n'
            "done\n\n"
            "chmod -R 755 /mnt/data/inputs\n"
        )

        init_containers = [
            {
                "name": "playout-init",
                "image": "alpine:3.17",
                "command": ["sh", "-c"],
                "args": [init_script],
                "env": [
                    {"name": "NAMESPACE", "value": jobtemplate_namespace},
                    {"name": "JOB_NAME", "value": job_name},
                    {"name": "TUSKR_JOB_TOKEN", "value": token},
                ],
                "volumeMounts": [{"name": "inputs-volume", "mountPath": "/mnt/data/inputs"}],
            }
        ]
        pod_spec["initContainers"] = init_containers

        # 4) Create a sidecar container that polls for non-"playout-" containers to terminate,
        #    then gathers and posts output files back to the context endpoint.
        sidecar_script = (
            "set -e\n"
            "chown -R 1000:1000 /mnt/data\n"
            "apk add --no-cache curl jq coreutils\n\n"
            'echo "Checking container statuses..."\n'
            "while true; do\n"
            "  curl -s \\\n"
            '    -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \\\n'
            "    --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt \\\n"
            '    "https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}'
            '/api/v1/namespaces/${NAMESPACE}/" \\\n'
            '    "pods/${POD_NAME}" \\\n'
            "    > /tmp/pod.json\n\n"
            "  NOT_TERMINATED_COUNT=$(jq -r '\n"
            "    [\n"
            "      .status.containerStatuses[] |\n"
            '      select(.name | startswith("playout-") | not) |\n'
            "      .state.terminated\n"
            "    ] | map(select(. == null)) | length\n"
            "  ' /tmp/pod.json)\n\n"
            '  if [ "$NOT_TERMINATED_COUNT" -eq 0 ]; then\n'
            '    echo "All main containers terminated."\n'
            "    break\n"
            "  fi\n\n"
            "  sleep 2\n"
            "done\n\n"
            'echo "Gathering outputs..."\n'
            "if [ -d /mnt/data/outputs ] && [ \"$(ls -A /mnt/data/outputs | grep -v '^done$')\" ]; then\n"
            "  cat <<EOF > /tmp/out.json\n"
            "  {\n"
            '    "outputs": {\n'
            "EOF\n\n"
            "  first=true\n"
            "  for out_file in $(ls /mnt/data/outputs | grep -v '^done$'); do\n"
            "    encoded=$(base64 /mnt/data/outputs/$out_file | tr -d '\\n')\n"
            '    if [ "$first" = true ]; then\n'
            '      echo "    \\"$out_file\\": \\"$encoded\\"" >> /tmp/out.json\n'
            "      first=false\n"
            "    else\n"
            '      echo "    ,\\"$out_file\\": \\"$encoded\\"" >> /tmp/out.json\n'
            "    fi\n"
            "  done\n\n"
            "  cat <<EOF2 >> /tmp/out.json\n"
            "    }\n"
            "  }\n"
            "EOF2\n"
            "else\n"
            "  echo '{\"outputs\": {}}' > /tmp/out.json\n"
            "fi\n\n"
            'echo "Posting outputs..."\n'
            "HTTP_CODE_POST=$(curl -s -o /tmp/post_response.json -w '%{http_code}' \\\n"
            '  -X POST -H "Content-Type: application/json" \\\n'
            "  -d @/tmp/out.json \\\n"
            '  "http://tuskr-controller.tuskr.svc.cluster.local:8080/jobs/'
            '${NAMESPACE}/${JOB_NAME}/context?token=${TUSKR_JOB_TOKEN}")\n'
            'if [ "$HTTP_CODE_POST" -ne 200 ]; then\n'
            '  echo "Error: Failed to post outputs. HTTP code: $HTTP_CODE_POST" >&2\n'
            "  exit 1\n"
            "fi\n\n"
            'echo "Outputs successfully posted."\n'
        )

        sidecar = {
            "name": "playout-sidecar",
            "image": "alpine:3.17",
            "command": ["sh", "-c"],
            "args": [sidecar_script],
            "env": [
                {"name": "NAMESPACE", "value": jobtemplate_namespace},
                {"name": "JOB_NAME", "value": job_name},
                {"name": "TUSKR_JOB_TOKEN", "value": token},
                {
                    "name": "POD_NAME",
                    "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
                },
            ],
            "volumeMounts": [{"name": "outputs-volume", "mountPath": "/mnt/data/outputs"}],
        }
        containers.append(sidecar)
        pod_spec["containers"] = containers

        # --- Construct the final Job manifest ---
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
                "backoffLimit": 0,  # No retries
            },
        }

        # --- Create the Job in the cluster ---
        batch_api = kubernetes.client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=target_namespace, body=job_body)
        except kubernetes.client.exceptions.ApiException as e:
            logger.exception("Failed to create Job.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        # Optional: If there's a callback param, store that in Redis for watchers
        callback_url = req.get_param("callback")
        if callback_url:
            callback_key = f"job_callbacks::{target_namespace}::{job_name}"
            callback_info = {"url": callback_url}
            if env_vars.get("AUTHORIZATION"):
                callback_info["authorization"] = env_vars["AUTHORIZATION"]
            redis_client.setex(callback_key, 3600, json.dumps(callback_info))

        msg = f"Created Job '{job_name}' in '{target_namespace}' from " f"template '{jobtemplate_name}'."
        logger.info(msg)

        # Return the token for client use
        resp.status = falcon.HTTP_201
        resp.media = {
            "message": msg,
            "job_name": job_name,
            "namespace": target_namespace,
            "token": token,
        }
