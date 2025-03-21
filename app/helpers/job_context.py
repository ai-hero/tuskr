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

"""Job Context Resource module."""

import json
import logging

import falcon
from falcon import Request, Response

from helpers.constants import JOB_CONTEXT_PREFIX
from helpers.redis_client import redis_client
from helpers.utils import parse_token

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobContextResource:
    """Endpoint for the job container to GET/POST the environment vars, inputs, or outputs.

    Usage:
      - GET /jobs/{namespace}/{job_name}/context?token=<token> => retrieve inputs/env-vars
      - POST /jobs/{namespace}/{job_name}/context?token=<token> => store outputs (optional)
    """

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Handle GET requests to retrieve context data."""
        token = req.get_param("token")
        if not token:
            resp.status = falcon.HTTP_401
            resp.media = {"error": "Missing token"}
            return

        # Validate token -> (ns, job_name)
        valid_ns, valid_name = parse_token(token)
        if not valid_ns or not valid_name or (valid_ns != namespace or valid_name != job_name):
            resp.status = falcon.HTTP_403
            resp.media = {"error": "Invalid token/job mismatch"}
            return

        # Retrieve context
        context_key = f"{JOB_CONTEXT_PREFIX}:{namespace}:{job_name}"
        raw = redis_client.get(context_key)
        if not raw:
            resp.status = falcon.HTTP_404
            resp.media = {"error": "No context found"}
            return

        data = json.loads(raw)
        resp.status = falcon.HTTP_200
        resp.media = {"env_vars": data.get("env_vars", {}), "inputs": data.get("inputs", {})}

    def on_post(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Handle POST requests to store job outputs."""
        token = req.get_param("token")
        if not token:
            resp.status = falcon.HTTP_401
            resp.media = {"error": "Missing token"}
            return

        valid_ns, valid_name = parse_token(token)
        if not valid_ns or not valid_name or (valid_ns != namespace or valid_name != job_name):
            resp.status = falcon.HTTP_403
            resp.media = {"error": "Invalid token/job mismatch"}
            return

        try:
            body = req.media
        except Exception as e:
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"Invalid JSON: {str(e)}"}
            return

        outputs = body.get("outputs", {})

        # Here you could store the job's outputs in Redis if desired
        # e.g. context_data["outputs"] = outputs
        context_key = f"{JOB_CONTEXT_PREFIX}:{namespace}:{job_name}"
        raw = redis_client.get(context_key)
        if not raw:
            resp.status = falcon.HTTP_404
            resp.media = {"error": "Context not found"}
            return

        context_data = json.loads(raw)
        context_data["outputs"] = outputs

        # Re-store with same TTL (you might want to refresh TTL if relevant)
        # Let's just reset it to 1 hour again for demonstration
        redis_client.setex(context_key, 3600, json.dumps(context_data))

        resp.status = falcon.HTTP_200
        resp.media = {"message": "Outputs stored successfully."}
