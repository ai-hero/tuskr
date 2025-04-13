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

"""Module for job description functionality.

Provides a Falcon resource to describe job details using Redis.
"""

import json
import logging

import falcon
from falcon import Request, Response

from helpers.custom_json_decoder import CustomJsonDecoder
from helpers.redis_client import redis_client
from helpers.utils import redis_key_for_job_describe

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobDescribeResource:
    """Returns a "describe"-like output for a Job from Redis.

    (Requires your watchers to store detailed Pod/Event data there.)
    """

    def on_get(self, req: Request, resp: Response, namespace: str, job_name: str) -> None:
        """Handle GET requests for a Job description."""
        describe_key = redis_key_for_job_describe(namespace, job_name)
        raw = redis_client.get(describe_key)
        if not raw:
            msg = f"No describe data found for Job {job_name} in {namespace}."
            resp.status = falcon.HTTP_404
            resp.media = {"error": msg}
            return

        # Decode using CustomJsonDecoder
        desc_data = json.loads(raw.decode("utf-8"), cls=CustomJsonDecoder)
        resp.status = falcon.HTTP_200
        resp.media = {"describe": desc_data}
