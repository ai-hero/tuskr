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
        redis_client.setex(describe_key, 3600, json.dumps(job_description_obj.to_dict(), cls=CustomJsonEncoder))
        logger.info(f"Job description stored for {namespace}/{job_name}")
    except Exception as e:
        if "404" in str(e) or "NotFound" in str(e):
            logger.info(f"Job description not found for {namespace}/{job_name}, exiting fetch_job_description")
            raise
        logger.warning(f"Failed to gather description data for {namespace}/{job_name}: {str(e)}")


def poll_job_state(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Poll the job state and update the respective Redis key.

    This thread continuously polls the job via the Kubernetes Batch API. When it detects that the
    job has reached a terminal state ("Succeeded" or "Failed"), it updates the state key and then exits.
    """
    logger.info(f"Started poll_job_state for {namespace}/{job_name}")  # Added log at thread start
    state_key = redis_key_for_job_state(namespace, job_name)
    data_key = redis_key_for_job_data(namespace, job_name)
    batch_api = kubernetes.client.BatchV1Api()
    while not stop_event.is_set():
        logger.info(f"Polling job state iteration for {namespace}/{job_name}")  # Added debug log
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
            redis_client.setex(data_key, 3600, json.dumps(job_obj.to_dict(), cls=CustomJsonEncoder))
            logger.info(f"Updated job state for {namespace}/{job_name}: {current_state}")
            # Exit if in terminal state.
            if current_state in ("Succeeded", "Failed"):
                logger.info(
                    f"Terminal state reached, exiting poll_job_state for {namespace}/{job_name}"
                )  # Added log at exit
                return
        except Exception as e:
            if "404" in str(e) or "NotFound" in str(e):
                logger.info(f"Job {namespace}/{job_name} not found. Exiting poll_job_state")
                redis_client.setex(state_key, 3600, "NotFound")
                return
            logger.warning(f"Error polling state for {namespace}/{job_name}: {str(e)}")
        time.sleep(poll_interval)


def poll_job_description(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Periodically poll and update the job description."""
    logger.info(f"Started poll_job_description for {namespace}/{job_name}")  # Added log at thread start
    while not stop_event.is_set():
        logger.info(f"Polling job description for {namespace}/{job_name}")  # Added debug log
        try:
            fetch_job_description(namespace, job_name)
        except Exception as e:
            if "404" in str(e) or "NotFound" in str(e):
                logger.info(
                    f"Job {namespace}/{job_name} not found during description polling. Exiting poll_job_description"
                )
                return
            logger.warning(f"Error polling description for {namespace}/{job_name}: {str(e)}")
        time.sleep(poll_interval)
    logger.info(f"Exiting poll_job_description for {namespace}/{job_name}")  # Added log at exit


def poll_job_logs(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Periodically poll and update the job logs.

    This function waits until the job is in a running (or terminal) state before fetching logs.
    """
    logger.info(f"Started poll_job_logs for {namespace}/{job_name}")  # Added log at thread start
    core_api = kubernetes.client.CoreV1Api()
    logs_key = redis_key_for_job_logs(namespace, job_name)
    aggregated_logs: Dict[str, str] = {}
    state_key = redis_key_for_job_state(namespace, job_name)

    while not stop_event.is_set():
        logger.info(f"Polling job logs iteration for {namespace}/{job_name}")  # Added debug log
        try:
            # Check the job state; proceed only if Running or later.
            job_state_bytes = redis_client.get(state_key)
            current_state = job_state_bytes.decode("utf-8") if job_state_bytes else "Pending"
            if current_state not in ("Running", "Succeeded", "Failed"):
                time.sleep(poll_interval)
                continue

            pods = core_api.list_namespaced_pod(namespace, label_selector=f"job-name={job_name}").items
            for pod in pods:
                pod_name = pod.metadata.name
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
    logger.info(f"Exiting poll_job_logs for {namespace}/{job_name}")  # Added log at exit
    # Optionally, mark that log polling is complete.
    redis_client.setex(logs_key, 3600, json.dumps(list(aggregated_logs.values()), cls=CustomJsonEncoder))


def poll_job(namespace: str, job_name: str, poll_interval: int = 2) -> None:
    """Poll job description, state, and logs concurrently until a terminal state is reached.

    As soon as polling starts the job state, data, and description are updated.
    Once the state becomes "Running", logs polling begins.
    """
    logger.info(f"Started poll_job for {namespace}/{job_name}")  # Added log at start
    stop_event = threading.Event()

    # Start state and description polling immediately.
    state_thread = threading.Thread(target=poll_job_state, args=(namespace, job_name, poll_interval, stop_event))
    description_thread = threading.Thread(
        target=poll_job_description, args=(namespace, job_name, poll_interval, stop_event)
    )
    state_thread.start()
    description_thread.start()

    # Perform an initial update of job data.
    batch_api = kubernetes.client.BatchV1Api()
    try:
        job_obj = batch_api.read_namespaced_job(job_name, namespace)
        redis_client.setex(
            redis_key_for_job_data(namespace, job_name),
            3600,
            json.dumps(job_obj.to_dict(), cls=CustomJsonEncoder),
        )
    except Exception as e:
        logger.error(f"Failed initial update of job data for {namespace}/{job_name}: {str(e)}")

    logs_thread = None
    # Wait until the job state indicates it is running (or terminal) before starting log polling.
    while True:
        logger.info(f"Waiting for job state update for {namespace}/{job_name}")  # Added debug log in loop
        try:
            job_state_bytes = redis_client.get(redis_key_for_job_state(namespace, job_name))
            if job_state_bytes:
                current_state = job_state_bytes.decode("utf-8")
                if current_state == "Running":
                    # Start logs polling as soon as the job is running.
                    logs_thread = threading.Thread(
                        target=poll_job_logs, args=(namespace, job_name, poll_interval, stop_event)
                    )
                    logs_thread.start()
                    break
                elif current_state in ("Succeeded", "Failed"):
                    break
            time.sleep(poll_interval)
        except Exception as e:
            logger.warning(f"Error while waiting for job state to update: {str(e)}")
            time.sleep(poll_interval)

    # Wait for the state polling thread to detect a terminal state.
    state_thread.join()

    # Signal all polling threads to stop.
    stop_event.set()
    description_thread.join()
    if logs_thread:
        logs_thread.join()

    logger.info(f"Completed polling threads for {namespace}/{job_name}")  # Added log after joining threads

    # Retrieve final job state and details.
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
                    job_json = json.loads(json.dumps(job_dict, cls=CustomJsonEncoder))
                    response = client.post(full_url, json=job_json, headers=headers)
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


def watch_jobs(event: Dict[str, Any], logger: logging.Logger, **kwargs: Any) -> None:
    """Start a new thread to poll job state, description, and logs."""
    job_obj = event.get("object")
    if not job_obj:
        return

    namespace = job_obj["metadata"]["namespace"]
    job_name = job_obj["metadata"]["name"]

    if not namespace or not job_name:
        logger.error("Namespace and job name are required.")
        return

    # Check if this is a job created event by inspecting the job object.
    metadata = job_obj.get("metadata", {})
    # Use annotations instead of labels for the "tuskr.io/launched-by" check
    annotations = metadata.get("annotations", {})
    if annotations.get("tuskr.io/launched-by") == "tuskr":
        logger.info(f"Starting job watch for {namespace}/{job_name}")
        t = threading.Thread(target=poll_job, args=(namespace, job_name))
        t.daemon = True
        t.start()
        logger.info(f"Started asynchronous poll_job for {namespace}/{job_name}")
    else:
        logger.info(f"Job {namespace}/{job_name} not launched by tuskr; skipping poll_job")
