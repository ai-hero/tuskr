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

"""Polling job helper module with callback support."""

import json
import logging
import threading
import time
import traceback
from typing import Dict

import httpx
import kubernetes

from helpers.encoder import CustomJsonEncoder
from helpers.redis_client import redis_client
from helpers.utils import (
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
        redis_client.setex(describe_key, 3600, json.dumps(job_description_obj.to_dict(), cls=CustomJsonEncoder))
        logger.info(f"Job description stored for {namespace}/{job_name}")
    except Exception as e:
        logger.warning(f"Failed to gather describe data for {namespace}/{job_name}: {str(e)}")


def poll_job_state(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Poll the job state and update the respective Redis key.

    This thread continuously polls the job via the Kubernetes Batch API. When it detects that the
    job has reached a terminal state ("Succeeded" or "Failed"), it updates the state key and then exits.
    """
    state_key = redis_key_for_job_state(namespace, job_name)
    batch_api = kubernetes.client.BatchV1Api()
    while not stop_event.is_set():
        try:
            job_obj = batch_api.read_namespaced_job(job_name, namespace)
            status = job_obj.status
            conditions = status.conditions if status and status.conditions else []
            current_state = "Unknown"
            # Determine state and capture failure reason if job failed.
            for condition in conditions:
                if condition.type == "Complete" and condition.status == "True":
                    current_state = "Succeeded"
                    break
                elif condition.type == "Failed" and condition.status == "True":
                    current_state = "Failed"
                    break
            if current_state == "Unknown":
                if status.active and status.active > 0:
                    current_state = "Running"
                elif not conditions and not status.active:
                    current_state = "Pending"
            redis_client.setex(state_key, 3600, current_state)
            logger.info(f"Updated job state for {namespace}/{job_name}: {current_state}")
            # Exit if in terminal state.
            if current_state in ("Succeeded", "Failed"):
                return
        except Exception as e:
            logger.warning(f"Error polling state for {namespace}/{job_name}: {str(e)}")
        time.sleep(poll_interval)


def poll_job_description(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Periodically poll and update the job description."""
    while not stop_event.is_set():
        try:
            fetch_job_description(namespace, job_name)
        except Exception as e:
            logger.warning(f"Error polling description for {namespace}/{job_name}: {str(e)}")
        time.sleep(poll_interval)


def poll_job_logs(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Periodically poll and update the job logs.

    For each pod associated with the job, fetch the latest logs (using tailing)
    and update the aggregated logs in Redis.
    """
    core_api = kubernetes.client.CoreV1Api()
    logs_key = redis_key_for_job_logs(namespace, job_name)
    aggregated_logs: Dict[str, str] = {}

    while not stop_event.is_set():
        try:
            pods = core_api.list_namespaced_pod(namespace, label_selector=f"job-name={job_name}").items
            for pod in pods:
                pod_name = pod.metadata.name
                # Process each container (skip specific ones if needed).
                for container in pod.spec.containers:
                    if container.name in ("playout-init", "playout-sidecar"):
                        continue
                    container_key = f"{pod_name}/{container.name}"
                    try:
                        logs = core_api.read_namespaced_pod_log(
                            name=pod_name,
                            namespace=namespace,
                            container=container.name,
                            tail_lines=10,
                            _preload_content=True,
                        )
                        aggregated_logs[container_key] = logs
                    except Exception as inner_e:
                        current = aggregated_logs.get(container_key, "")
                        aggregated_logs[container_key] = current + f"\nError: {str(inner_e)}"
            redis_client.setex(logs_key, 3600, json.dumps(list(aggregated_logs.values()), cls=CustomJsonEncoder))
            logger.info(f"Updated logs for {namespace}/{job_name}")
        except Exception as e:
            logger.warning(f"Error polling logs for {namespace}/{job_name}: {str(e)}")
        time.sleep(poll_interval)
    # Optionally, mark that log polling is complete.
    state_key = redis_key_for_job_state(namespace, job_name)
    redis_client.setex(state_key, 3600, "Completed logs")


def watch_job(namespace: str, job_name: str, poll_interval: int = 2) -> None:
    """Poll job description, state, and logs concurrently until a terminal state is reached.

    This function starts three threads that update the Redis keys for description, state, and logs.
    Once the job reaches a terminal state ("Succeeded" or "Failed"), it signals the other threads to stop,
    performs a final update, and sends a callback if one is registered.
    """
    stop_event = threading.Event()

    state_thread = threading.Thread(target=poll_job_state, args=(namespace, job_name, poll_interval, stop_event))
    description_thread = threading.Thread(
        target=poll_job_description, args=(namespace, job_name, poll_interval, stop_event)
    )
    logs_thread = threading.Thread(target=poll_job_logs, args=(namespace, job_name, poll_interval, stop_event))

    # Start all polling threads.
    state_thread.start()
    description_thread.start()
    logs_thread.start()

    # Wait for the state polling thread to detect a terminal state.
    state_thread.join()

    # Signal to the other threads that they should exit.
    stop_event.set()
    description_thread.join()
    logs_thread.join()

    logger.info(f"Polling complete for {namespace}/{job_name}")

    # Retrieve final job state and details.
    batch_api = kubernetes.client.BatchV1Api()
    try:
        job_obj = batch_api.read_namespaced_job(job_name, namespace)
    except Exception as e:
        logger.error(f"Failed to retrieve final job data for {namespace}/{job_name}: {str(e)}")
        return

    state_key = redis_key_for_job_state(namespace, job_name)
    final_state = redis_client.get(state_key)
    if isinstance(final_state, bytes):
        final_state = final_state.decode("utf-8")
    job_dict = job_obj.to_dict()
    job_dict["tuskr_state"] = final_state

    failure_reason = None
    if final_state == "Failed":
        conditions = job_dict.get("status", {}).get("conditions", [])
        for condition in conditions:
            if condition.get("type") == "Failed" and condition.get("status") == "True":
                failure_reason = condition.get("message")
                break
        if failure_reason:
            job_dict["tuskr_failure_reason"] = failure_reason

    # Check if a callback is registered.
    callback_key = f"job_callbacks::{namespace}::{job_name}"
    callback_info = redis_client.get(callback_key)
    if callback_info:
        callback_info = json.loads(callback_info)
        callback_url = callback_info.get("url")
        if callback_url:
            try:
                headers = {"Content-Type": "application/json"}
                if callback_info.get("authorization"):
                    headers["Authorization"] = callback_info["authorization"]
                full_url = f"{callback_url.rstrip('/')}/jobs/{namespace}/{job_name}"
                with httpx.Client() as client:
                    response = client.post(full_url, json=job_dict, headers=headers)
                    response.raise_for_status()
                # Remove the callback registration for terminal jobs.
                if final_state in ("Succeeded", "Failed"):
                    redis_client.delete(callback_key)
                log_msg = f"Callback for {namespace}/{job_name} sent; state: {final_state}"
                if failure_reason:
                    log_msg += f" (reason: {failure_reason})"
                logger.info(log_msg)
            except Exception as e:
                traceback.print_exc()
                logger.error(f"Callback failed for {namespace}/{job_name}: {str(e)}")
