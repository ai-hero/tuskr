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
        resp.media = {
            "state": job_state.decode("utf-8") if job_state else "Unknown",
            "job": job_data.decode("utf-8") if job_data else "{}",
        }

    def on_delete(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Delete the Job from Kubernetes (foreground propagation). Leaves Redis data as-is."""
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
