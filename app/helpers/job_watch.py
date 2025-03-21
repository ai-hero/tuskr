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

"""Job watch helper module."""

import json
import logging
import traceback
from typing import Any, Dict

import httpx

from helpers.redis_client import redis_client
from helpers.utils import redis_key_for_job_data, redis_key_for_job_state

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def watch_jobs(event: Dict[str, Any], logger: logging.Logger, **kwargs: Any) -> None:
    """Watch for Job events.

    Store the job object and its state in Redis with a 60-min TTL.
    If there's a callback registered, POST to it.
    """
    job_obj = event.get("object")
    if not job_obj:
        return

    namespace = job_obj["metadata"]["namespace"]
    job_name = job_obj["metadata"]["name"]

    status = job_obj.get("status", {})
    conditions = status.get("conditions", [])

    current_state = "Unknown"
    failure_reason = None

    # Check typical terminal conditions first
    for condition in conditions:
        if condition.get("type") == "Complete" and condition.get("status") == "True":
            current_state = "Succeeded"
            break
        elif condition.get("type") == "Failed" and condition.get("status") == "True":
            current_state = "Failed"
            failure_reason = condition.get("message")
            break

    # If not terminal, check if active pods exist
    if current_state == "Unknown":
        if status.get("active", 0) > 0:
            current_state = "Running"
        elif not conditions and not status.get("active"):
            current_state = "Pending"

    # Store job data in Redis
    data_key = redis_key_for_job_data(namespace, job_name)
    state_key = redis_key_for_job_state(namespace, job_name)
    # The entire job object might be large. We'll store as JSON
    redis_client.setex(data_key, 3600, json.dumps(job_obj))
    redis_client.setex(state_key, 3600, current_state)

    # Check if there's a callback
    callback_key = f"job_callbacks::{namespace}::{job_name}"
    callback_info = redis_client.get(callback_key)

    if callback_info:
        callback_info = json.loads(callback_info)
        callback_url = callback_info.get("url")
        if callback_url:
            try:
                # Add state/failure reason to job_obj for convenience in callback
                job_obj["tuskr_state"] = current_state
                if failure_reason:
                    job_obj["tuskr_failure_reason"] = failure_reason

                headers = {"Content-Type": "application/json"}
                full_url = f"{callback_url.rstrip('/')}/jobs/{namespace}/{job_name}"
                with httpx.Client() as client:
                    response = client.post(full_url, json=job_obj, headers=headers)
                    response.raise_for_status()

                # If job is terminal, remove the callback registration
                if current_state in ("Succeeded", "Failed"):
                    redis_client.delete(callback_key)

                log_msg = f"Callback for {namespace}/{job_name} => state: {current_state}"
                if failure_reason:
                    log_msg += f" (reason: {failure_reason})"
                logger.info(log_msg)

            except Exception as e:
                traceback.print_exc()
                logger.error(f"Callback failed for {namespace}/{job_name}: {str(e)}")
