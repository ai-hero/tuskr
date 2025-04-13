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
import threading
import time
import traceback
from typing import Any, Dict

import httpx
import kubernetes

from helpers.encoder import CustomJsonEncoder
from helpers.redis_client import redis_client
from helpers.utils import (
    redis_key_for_job_data,
    redis_key_for_job_describe,
    redis_key_for_job_logs,
    redis_key_for_job_state,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_job_description(namespace: str, job_name: str) -> None:
    """Fetch and store job description data using the Kubernetes Batch API."""
    try:
        batch_api = kubernetes.client.BatchV1Api()
        job_description_obj = batch_api.read_namespaced_job(job_name, namespace)
        describe_key = redis_key_for_job_describe(namespace, job_name)
        # Use CustomJsonEncoder to serialize the dict
        redis_client.setex(describe_key, 3600, json.dumps(job_description_obj.to_dict(), cls=CustomJsonEncoder))
        logger.info(f"Job description stored for {namespace}/{job_name}")
    except Exception as e:
        logger.warning(f"Failed to gather describe data for {namespace}/{job_name}: {str(e)}")


def fetch_all_job_pod_logs(namespace: str, job_name: str) -> None:
    """For each pod associated with the job, stream logs live for each container in that pod.

    Then combine the logs and store them in Redis.
    """
    try:
        core_api = kubernetes.client.CoreV1Api()
        pods = core_api.list_namespaced_pod(namespace, label_selector=f"job-name={job_name}").items

        logs_key = redis_key_for_job_logs(namespace, job_name)
        state_key = redis_key_for_job_state(namespace, job_name)
        # Set initial job state to indicate logs are processing.
        redis_client.setex(state_key, 3600, "Running logs")

        # Shared dictionary & lock for progressive logs.
        logs_dict: Dict[str, Any] = {}
        logs_lock = threading.Lock()
        stop_event = threading.Event()

        def update_aggregated_logs() -> None:
            """Update the aggregated logs in Redis."""
            with logs_lock:
                aggregated = list(logs_dict.values())
            redis_client.setex(logs_key, 3600, json.dumps(aggregated, cls=CustomJsonEncoder))

        def update_description_periodically() -> None:
            """Periodically update the job description in Redis."""
            while not stop_event.is_set():
                fetch_job_description(namespace, job_name)
                time.sleep(2)

        def stream_pod_logs(pod: Any) -> None:
            """Stream logs for each container in the pod."""
            pod_name = pod.metadata.name
            for container in pod.spec.containers:
                # Skip specific containers.
                if container.name in ("playout-init", "playout-sidecar"):
                    continue
                container_key = f"{pod_name}/{container.name}"
                logs_dict[container_key] = ""
                try:
                    log_stream = core_api.read_namespaced_pod_log(
                        name=pod_name,
                        namespace=namespace,
                        container=container.name,
                        follow=True,
                        _preload_content=False,
                        tail_lines=10,
                    )
                    # New loop reading fixed-size chunks until no more data is received.
                    while True:
                        chunk = log_stream.read(1024)
                        if not chunk:
                            break
                        with logs_lock:
                            logs_dict[container_key] += chunk.decode("utf-8")
                        update_aggregated_logs()
                    logger.info(f"Live logs streamed for {pod_name}/{container.name}")
                except Exception as inner_e:
                    error_msg = str(inner_e)
                    logger.warning(f"Could not stream logs for {pod_name}/{container.name}: {error_msg}")
                    with logs_lock:
                        logs_dict[container_key] += f"\nError: {error_msg}"
                    update_aggregated_logs()

        # Start the description updater thread.
        desc_thread = threading.Thread(target=update_description_periodically)
        desc_thread.start()

        # Launch a thread for each pod to stream its logs.
        threads = []
        for pod in pods:
            t = threading.Thread(target=stream_pod_logs, args=(pod,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        # Signal the periodic updater to stop and wait.
        stop_event.set()
        desc_thread.join()

        # Final aggregated logs update.
        update_aggregated_logs()
        redis_client.setex(state_key, 3600, "Completed logs")
        logger.info(f"Combined pod logs stored for {namespace}/{job_name}")
    except Exception as e:
        logger.warning(f"Failed to gather logs for {namespace}/{job_name}: {str(e)}")


def watch_jobs(event: Dict[str, Any], logger: logging.Logger, **kwargs: Any) -> None:
    """Watch for Job events.

    Store the job object and its state in Redis with a 60-min TTL.
    If there's a callback registered, POST to it.
    For terminal jobs (Succeeded/Failed), gather the detailed job description and live pod logs concurrently
    using separate threads.
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

    # Determine job state from its conditions.
    for condition in conditions:
        if condition.get("type") == "Complete" and condition.get("status") == "True":
            current_state = "Succeeded"
            break
        elif condition.get("type") == "Failed" and condition.get("status") == "True":
            current_state = "Failed"
            failure_reason = condition.get("message")
            break

    if current_state == "Unknown":
        if status.get("active", 0) > 0:
            current_state = "Running"
        elif not conditions and not status.get("active"):
            current_state = "Pending"

    # Store job data and current state in Redis.
    data_key = redis_key_for_job_data(namespace, job_name)
    state_key = redis_key_for_job_state(namespace, job_name)
    redis_client.setex(data_key, 3600, json.dumps(job_obj, cls=CustomJsonEncoder))
    redis_client.setex(state_key, 3600, current_state)

    # For terminal jobs, gather additional details concurrently.
    if current_state in ("Succeeded", "Failed"):
        desc_thread = threading.Thread(target=fetch_job_description, args=(namespace, job_name))
        pod_logs_thread = threading.Thread(target=fetch_all_job_pod_logs, args=(namespace, job_name))
        desc_thread.start()
        pod_logs_thread.start()
        desc_thread.join()
        pod_logs_thread.join()
    else:
        logger.info(
            f"Job {namespace}/{job_name} is in state {current_state}; "
            "skipping live log streaming and detailed description."
        )

    # Check if there's a callback registered.
    callback_key = f"job_callbacks::{namespace}::{job_name}"
    callback_info = redis_client.get(callback_key)

    if callback_info:
        callback_info = json.loads(callback_info)
        callback_url = callback_info.get("url")
        if callback_url:
            try:
                # Augment the job object with state details.
                job_obj["tuskr_state"] = current_state
                if failure_reason:
                    job_obj["tuskr_failure_reason"] = failure_reason

                headers = {"Content-Type": "application/json"}
                if callback_info.get("authorization"):
                    headers["Authorization"] = callback_info["authorization"]
                full_url = f"{callback_url.rstrip('/')}/jobs/{namespace}/{job_name}"
                with httpx.Client() as client:
                    response = client.post(full_url, json=job_obj, headers=headers)
                    response.raise_for_status()

                # Remove the callback registration for terminal jobs.
                if current_state in ("Succeeded", "Failed"):
                    redis_client.delete(callback_key)

                log_msg = f"Callback for {namespace}/{job_name} => state: {current_state}"
                if failure_reason:
                    log_msg += f" (reason: {failure_reason})"
                logger.info(log_msg)

            except Exception as e:
                traceback.print_exc()
                logger.error(f"Callback failed for {namespace}/{job_name}: {str(e)}")
