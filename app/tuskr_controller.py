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
"""Tuskr controller for Kubernetes."""

import json
from typing import Any, Dict

import httpx
import kopf
import kubernetes

from helpers.redis import REDIS_TTL, event_redis_key, job_pod_set_key, job_redis_key, pod_redis_key, redis_client
from helpers.server import start_http_server


# ------------------------------------------------------------------------------
# Kubernetes config on startup
# ------------------------------------------------------------------------------
@kopf.on.startup()  # type: ignore
def startup_fn(logger: Any, **kwargs: Any) -> None:
    """Load the Kubernetes configuration on startup."""
    logger.info("Tuskr controller starting up.")
    # If running inside a cluster:
    kubernetes.config.load_incluster_config()
    # If testing locally, you could use:
    # kubernetes.config.load_kube_config()


# ------------------------------------------------------------------------------
# Start the Falcon server on startup
# ------------------------------------------------------------------------------
@kopf.on.startup()  # type: ignore
def startup_http_server(**kwargs: Any) -> None:
    """Launch the Falcon server in a background thread at operator startup."""
    start_http_server()


# ------------------------------------------------------------------------------
# Watcher for Jobs
# ------------------------------------------------------------------------------
@kopf.on.event("batch", "v1", "jobs")  # type: ignore
def watch_jobs(event: Dict[str, Any], logger: Any, **kwargs: Any) -> None:
    """Watch for Job events, store the Job object in Redis with TTL=60m, and do callback logic if needed."""
    job_obj = event.get("object")
    if not job_obj:
        return

    # Basic data
    namespace = job_obj["metadata"]["namespace"]
    job_name = job_obj["metadata"]["name"]

    # 1) Store the entire Job object in Redis
    job_dict = job_obj  # Already a dict in Kopf event
    redis_client.setex(
        job_redis_key(namespace, job_name),
        REDIS_TTL,
        json.dumps(job_dict),
    )

    # 2) Determine current state
    status = job_obj.get("status", {})
    conditions = status.get("conditions", [])
    current_state = "Unknown"
    failure_reason = None

    # Identify terminal states
    for condition in conditions:
        if condition.get("type") == "Complete" and condition.get("status") == "True":
            current_state = "Succeeded"
            break
        elif condition.get("type") == "Failed" and condition.get("status") == "True":
            current_state = "Failed"
            failure_reason = condition.get("message")
            break

    # If not terminal, check active vs. pending
    if current_state == "Unknown":
        if status.get("active", 0) > 0:
            current_state = "Running"
        else:
            current_state = "Pending"

    # 3) Handle callback logic if relevant (optional)
    callback_key = f"job_callbacks::{namespace}::{job_name}"
    callback_url = redis_client.get(callback_key)
    if callback_url:
        try:
            job_dict["tuskr_state"] = current_state
            if failure_reason:
                job_dict["tuskr_failure_reason"] = failure_reason

            full_url = f"{callback_url.decode().rstrip('/')}/jobs/{namespace}/{job_name}"
            with httpx.Client() as client:
                response = client.post(full_url, json=job_dict, headers={"Content-Type": "application/json"})
                response.raise_for_status()

            # Clear callback if job is terminal
            if current_state in ("Succeeded", "Failed"):
                redis_client.delete(callback_key)

            log_message = f"Callback for job {job_name} in state {current_state}"
            if failure_reason:
                log_message += f" reason: {failure_reason}"
            logger.info(log_message)

        except Exception as e:
            logger.error(f"Failed callback for {job_name} in state {current_state}: {str(e)}")


# ------------------------------------------------------------------------------
# Watcher for Pods
# ------------------------------------------------------------------------------
@kopf.on.event("", "v1", "pods")  # type: ignore
def watch_pods(event: Dict[str, Any], logger: Any, **kwargs: Any) -> None:
    """Watch for Pod events. If the Pod is associated with a known Job (by label), store in Redis."""
    pod_obj = event.get("object")
    if not pod_obj:
        return

    metadata = pod_obj["metadata"]
    namespace = metadata["namespace"]
    pod_name = metadata["name"]
    labels = metadata.get("labels", {})

    job_name = labels.get("job-name")  # standard label for job pods
    if not job_name:
        return  # not a pod we care about

    # Store the entire Pod object in Redis
    redis_client.setex(
        pod_redis_key(namespace, pod_name),
        REDIS_TTL,
        json.dumps(pod_obj),
    )

    # Also add pod_name to a Redis set for the parent Job
    redis_client.sadd(job_pod_set_key(namespace, job_name), pod_name)
    # Refresh the set's TTL
    redis_client.expire(job_pod_set_key(namespace, job_name), REDIS_TTL)


# ------------------------------------------------------------------------------
# Watcher for Events
# ------------------------------------------------------------------------------
@kopf.on.event("", "v1", "events")  # type: ignore
def watch_events(event: Dict[str, Any], event_logger: Any, **kwargs: Any) -> None:
    """Watch all v1 Events. If the involvedObject is a Job or a Pod, store in Redis."""
    event_obj = event.get("object")
    if not event_obj:
        return

    involved = event_obj.get("involvedObject", {})
    if not involved:
        return

    kind = involved.get("kind", "")
    namespace = involved.get("namespace", "")
    name = involved.get("name", "")

    # We only care about Job or Pod events
    if kind not in ["Job", "Pod"]:
        return

    key = event_redis_key(namespace, kind, name)

    # We'll store a list of events. Let's do an append approach:
    try:
        existing_str = redis_client.get(key)
        if existing_str:
            events_list = json.loads(existing_str)
        else:
            events_list = []
        events_list.append(event_obj)
        redis_client.setex(key, REDIS_TTL, json.dumps(events_list))
    except Exception as exc:
        event_logger.error(f"Failed to store event for {kind}/{name} in Redis: {exc}")
