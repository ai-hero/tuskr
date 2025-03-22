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

"""LaunchResource module.

This Falcon resource handles POST /launch to create a Kubernetes Job from a JobTemplate
while storing/retrieving context data (env-vars, inputs, outputs) via the JobContextResource.
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

    1) It retrieves the JobTemplate from the cluster (via CustomObjectsApi).
    2) It generates a unique job_name (templateName + random suffix).
    3) It stores environment variables and inputs in Redis under a "context" key.
    4) It injects an init container into the Job's spec to fetch context data (env_vars/inputs).
    5) It injects a sidecar container to POST output files back to the same context endpoint.
    6) Finally, it creates the Job in the specified namespace.
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

        # --- Generate the final job name ---
        random_suffix = generate_random_suffix()
        job_name = f"{jobtemplate_name}-{random_suffix}"
        target_namespace = jobtemplate_namespace

        # --- Obtain the base Job spec from the JobTemplate CRD ---
        job_spec_from_template = jobtemplate.get("spec", {}).get("jobSpec", {}).get("template", {})
        if not job_spec_from_template:
            msg = f"No 'spec.jobSpec.template' found in JobTemplate '{jobtemplate_name}'."
            logger.warning(msg)
            resp.status = falcon.HTTP_400
            resp.media = {"error": msg}
            return

        # --- Create a token + store context (env_vars, inputs) in Redis ---
        token = generate_random_suffix(length=20)  # a longer random token
        context_key = f"{JOB_CONTEXT_PREFIX}:{jobtemplate_namespace}:{job_name}"
        context_data = {
            "env_vars": env_vars,  # dictionary
            "inputs": input_files,  # dictionary (filename -> content)
        }

        # Store context data in Redis with TTL = 60 minutes
        redis_client.setex(context_key, 3600, json.dumps(context_data))

        # Also map the token -> that specific (namespace, job_name) for validation
        token_key = f"{TOKEN_PREFIX}:{token}"
        token_data = {"namespace": jobtemplate_namespace, "job_name": job_name}
        redis_client.setex(token_key, 3600, json.dumps(token_data))

        # --- Now we modify the Pod spec to add volumes, initContainer, sidecar, etc. ---
        pod_spec = job_spec_from_template.get("spec", {})
        containers = pod_spec.get("containers", [])

        # 1) Create volumes for inputs and outputs
        volumes = [
            {"name": "inputs-volume", "emptyDir": {}},
            {"name": "outputs-volume", "emptyDir": {}},
        ]
        pod_spec["volumes"] = pod_spec.get("volumes", []) + volumes

        # 2) If there's at least one container, override command/args if provided,
        #    and inject the TUSKR_JOB_TOKEN env var, plus mount volumes.
        if containers:
            if command_override:
                containers[0]["command"] = command_override
            if args_override:
                containers[0]["args"] = args_override

            # Inject TUSKR_JOB_TOKEN as env var
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

        # 3) Create an init container that fetches the context (env_vars + inputs) from the API
        init_containers = [
            {
                "name": "init-fetch-context",
                "image": "alpine:3.17",
                "command": ["sh", "-c"],
                "args": [
                    """
                    set -ex
                    chown -R 1000:1000 /mnt/data
                    apk add --no-cache curl jq

                    echo "Fetching context for job $JOB_NAME in ns $NAMESPACE"
                    HTTP_CODE=$(curl -s -o /tmp/context.json -w '%{http_code}' \
                        "http://tuskr-controller.tuskr.svc.cluster.local:8080/jobs/$NAMESPACE/$JOB_NAME/context?token=$TUSKR_JOB_TOKEN")

                    if [ "$HTTP_CODE" -ne 200 ]; then
                      echo "Failed to fetch context. HTTP code: $HTTP_CODE"
                      cat /tmp/context.json
                      exit 1
                    fi

                    mkdir -p /mnt/data/inputs

                    # Save inputs to /mnt/data/inputs
                    for filename in $(jq -r '.inputs | keys[]' /tmp/context.json); do
                      content=$(jq -r ".inputs[\\"$filename\\"]" /tmp/context.json)
                      echo "$content" > "/mnt/data/inputs/$filename"
                      echo "Wrote input file: $filename"
                    done

                    # Write out a .env file with environment variables
                    touch /mnt/data/inputs/.env
                    for varname in $(jq -r '.env_vars | keys[]' /tmp/context.json); do
                      value=$(jq -r ".env_vars[\\"$varname\\"]" /tmp/context.json)
                      echo "export $varname=\\"$value\\"" >> /mnt/data/inputs/.env
                    done

                    chmod -R 755 /mnt/data/inputs
                    """
                ],
                "env": [
                    {"name": "NAMESPACE", "value": jobtemplate_namespace},
                    {"name": "JOB_NAME", "value": job_name},
                    {"name": "TUSKR_JOB_TOKEN", "value": token},
                ],
                "volumeMounts": [{"name": "inputs-volume", "mountPath": "/mnt/data/inputs"}],
            }
        ]
        pod_spec["initContainers"] = init_containers

        # 4) Create a sidecar container that waits for main container to finish
        #    (detected by e.g. /mnt/data/outputs/done) and then POSTs them back to context.
        sidecar = {
            "name": "post-outputs",
            "image": "alpine:3.17",
            "command": ["sh", "-c"],
            "args": [
                """
                set -ex
                chown -R 1000:1000 /mnt/data
                apk add --no-cache curl jq coreutils

                echo "Sidecar waiting for the main container to produce /mnt/data/outputs/done..."
                while [ ! -f /mnt/data/outputs/done ]; do
                  sleep 0.23
                done

                echo "Main container done. Gathering output files..."

                # Build JSON with base64-encoded files
                cat <<EOF > /tmp/out.json
                {
                  "outputs": {
                EOF

                first=true
                for out_file in $(ls /mnt/data/outputs | grep -v '^done$'); do
                  encoded=$(base64 /mnt/data/outputs/$out_file | tr -d '\\n')
                  if [ "$first" = true ]; then
                    echo "    \\"$out_file\\": \\"$encoded\\"" >> /tmp/out.json
                    first=false
                  else
                    echo "    ,\\"$out_file\\": \\"$encoded\\"" >> /tmp/out.json
                  fi
                done

                cat <<EOF2 >> /tmp/out.json
                  }
                }
                EOF2

                echo "Posting outputs back to context..."
                curl -v -X POST -H "Content-Type: application/json" \
                     -d @/tmp/out.json \
                     "http://tuskr-controller.tuskr.svc.cluster.local:8080/jobs/$NAMESPACE/$JOB_NAME/context?token=$TUSKR_JOB_TOKEN"

                echo "Sidecar post complete. Exiting..."
                """
            ],
            "env": [
                {"name": "NAMESPACE", "value": jobtemplate_namespace},
                {"name": "JOB_NAME", "value": job_name},
                {"name": "TUSKR_JOB_TOKEN", "value": token},
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
                # This is the Job spec using the updated Pod template
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

        # (Optional) If there's a callback param, store that in Redis for watchers
        callback_url = req.get_param("callback")
        if callback_url:
            callback_key = f"job_callbacks::{target_namespace}::{job_name}"
            callback_info = {"url": callback_url}
            redis_client.setex(callback_key, 3600, json.dumps(callback_info))

        msg = f"Created Job '{job_name}' in '{target_namespace}' from template '{jobtemplate_name}'."
        logger.info(msg)

        # Return the token if you want the client to see it
        resp.status = falcon.HTTP_201
        resp.media = {
            "message": msg,
            "job_name": job_name,
            "namespace": target_namespace,
            "token": token,
        }
