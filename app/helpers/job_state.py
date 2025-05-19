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
"""Module for job state management."""

import json
import logging

import falcon
import kubernetes
from falcon import Request, Response

from helpers.redis_client import redis_client
from helpers.utils import redis_key_for_job_data, redis_key_for_job_state

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobResource:
    """Endpoint for /jobs/{namespace}/{job_name} to get or delete a job."""

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Return the stored state/details of the given job from Redis.

        If it doesn't exist in Redis, return 404.
        """
        state_key = redis_key_for_job_state(namespace, job_name)
        data_key = redis_key_for_job_data(namespace, job_name)

        job_state = redis_client.get(state_key)
        job_data = redis_client.get(data_key)
        if not job_state:
            msg = f"Job {job_name} not found in namespace {namespace} (no state in Redis)."
            logger.warning(msg)
            resp.status = falcon.HTTP_404
            resp.media = {"error": msg}
            return

        if not job_data:
            msg = f"Job {job_name} not found in namespace {namespace} (no data in Redis)."
            logger.warning(msg)
            resp.status = falcon.HTTP_404
            resp.media = {"error": msg}
            return

        # job_state is just a short string ("Pending", "Running", "Succeeded", etc.)
        # job_data is the JSON representation of the job object we stored
        resp.status = falcon.HTTP_200
        resp.media = json.loads(job_data.decode("utf-8"))

    def on_delete(
        self,
        req: Request,
        resp: Response,
        namespace: str,
        job_name: str,
    ) -> None:
        """Cancel (grace‑stop) a Job by default.

        If the client supplies ?force_delete=true we actually delete the Job
        (behaviour identical to the old implementation).

        Grace‑stop strategy
        -------------------
        1.  Suspend the Job (`spec.suspend = True`) so Kubernetes stops
            creating new Pods.
        2.  Find all running Pods whose label `job-name=<job_name>`.
        3.  Send each Pod a `DELETE` with a non‑zero `grace_period_seconds`
            (SIGTERM → graceful shutdown inside the container).
        4.  Return 202 Accepted so the caller knows the stop was initiated
            but may still be finishing.

        This approach lets every container’s pre‑stop hook / signal handler
        run, avoids creating “Failed” Job history objects, and leaves the Job
        object around so you can still inspect logs or retry it later.
        """
        force_delete = req.get_param_as_bool("force_delete") or False
        batch_api = kubernetes.client.BatchV1Api()
        core_api = kubernetes.client.CoreV1Api()

        if force_delete:
            # ---------- HARD DELETE ----------
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
                logger.exception("Unexpected error deleting Job.")
                resp.status = falcon.HTTP_500
                resp.media = {"error": str(e)}
                return

            resp.status = falcon.HTTP_200
            resp.media = {
                "message": f"Job {job_name} deleted (force=true).",
                "status": delete_resp.to_dict(),
            }
            return

        # ---------- GRACEFUL CANCEL ----------
        try:
            # 1) Suspend the Job so no new Pods will be created.
            batch_api.patch_namespaced_job(
                name=job_name,
                namespace=namespace,
                body={"spec": {"suspend": True}},
            )

            # 2) Find all Pods belonging to the Job.
            pods = core_api.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job_name}",
            )

            # 3) Delete each Pod with a grace period (defaults to 30 s here).
            for pod in pods.items:
                core_api.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=namespace,
                    grace_period_seconds=30,
                )

        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                msg = f"Job {job_name} not found in namespace {namespace}."
                logger.error(msg)
                resp.status = falcon.HTTP_404
                resp.media = {"error": msg}
                return
            logger.exception("Unexpected error cancelling Job.")
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            return

        resp.status = falcon.HTTP_202  # accepted, still terminating
        resp.media = {
            "message": (
                f"Job {job_name} cancellation initiated; pods are " f"terminating with SIGTERM (graceful‑stop)."
            ),
            "suspended": True,
            "pods_signalled": len(pods.items),
        }
