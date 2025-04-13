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

# MIT License
#
# (License text omitted for brevity)
#
"""Polling job helper module with callback support."""

import json
import logging
import threading
import time
import traceback
from typing import Any, Dict, List

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
    """Fetch and store a 'describe'-like output for the Job in Redis.

    Stores JSON of the shape:
    {
      "job": <jobObjDict>,
      "pods": [<podObj>, ...],
      "events_for_job": [<jobEventObj>, ...],
      "events_for_pods": [
         {"podNameA": [<podEvent>, ...]},
         {"podNameB": [<podEvent>, ...]},
         ...
      ]
    }
    """
    try:
        batch_api = kubernetes.client.BatchV1Api()
        core_api = kubernetes.client.CoreV1Api()

        # Read the job itself
        job_obj = batch_api.read_namespaced_job(job_name, namespace)

        # Find Pods that belong to this Job
        label_selector = f"job-name={job_name}"
        pods_list = core_api.list_namespaced_pod(namespace, label_selector=label_selector)

        # Retrieve events for the Job
        events_for_job = core_api.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.kind=Job,involvedObject.name={job_name}",
        )

        # Retrieve events for each Pod
        events_for_pods = []
        for pod in pods_list.items:
            pod_name = pod.metadata.name
            pod_events = core_api.list_namespaced_event(
                namespace=namespace,
                field_selector=f"involvedObject.kind=Pod,involvedObject.name={pod_name}",
            )
            events_for_pods.append({pod_name: [e.to_dict() for e in pod_events.items]})

        # Build the "describe" output dictionary
        describe_output = {
            "job": job_obj.to_dict(),
            "pods": [p.to_dict() for p in pods_list.items],
            "events_for_job": [e.to_dict() for e in events_for_job.items],
            "events_for_pods": events_for_pods,
        }

        # Store in Redis
        describe_key = redis_key_for_job_describe(namespace, job_name)
        redis_client.setex(
            describe_key,
            3600,  # TTL
            json.dumps(describe_output, cls=CustomJsonEncoder),
        )
        logger.info(f"Job 'describe' data stored for {namespace}/{job_name}")
    except Exception as e:
        if "404" in str(e) or "NotFound" in str(e):
            logger.info(f"Job {namespace}/{job_name} not found (fetch_job_description). Exiting.")
            raise
        logger.warning(f"Failed to gather description data for {namespace}/{job_name}: {str(e)}")


def poll_job_state(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Continuously poll the job state and store state information in Redis.

    Polls for state changes and updates Redis keys:
    1) A simple "tuskr_state" string in redis_key_for_job_state(...).
    2) The entire Job object with a "tuskr_state" field in redis_key_for_job_data(...).
    """
    logger.info(f"Started poll_job_state for {namespace}/{job_name}")
    state_key = redis_key_for_job_state(namespace, job_name)
    data_key = redis_key_for_job_data(namespace, job_name)
    batch_api = kubernetes.client.BatchV1Api()

    while not stop_event.is_set():
        logger.debug(f"Polling job state for {namespace}/{job_name}")
        try:
            job_obj = batch_api.read_namespaced_job(job_name, namespace)
            status = job_obj.status
            conditions = status.conditions if status and status.conditions else []

            # Default to "Unknown"
            current_state = "Unknown"
            failure_reason = None

            # Look for terminal conditions
            for condition in conditions:
                if condition.type == "Complete" and condition.status == "True":
                    current_state = "Succeeded"
                    break
                elif condition.type == "Failed" and condition.status == "True":
                    current_state = "Failed"
                    failure_reason = condition.message
                    break

            # If not terminal, check if it's Running or Pending
            if current_state == "Unknown":
                if status.active and status.active > 0:
                    current_state = "Running"
                elif not conditions and not status.active:
                    current_state = "Pending"

            # Attach tuskr_state to the job object
            job_dict = job_obj.to_dict()
            job_dict["tuskr_state"] = current_state
            if failure_reason:
                job_dict["tuskr_failure_reason"] = failure_reason

            # Store the simplified state and the entire job data
            redis_client.setex(state_key, 3600, current_state)
            redis_client.setex(data_key, 3600, json.dumps(job_dict, cls=CustomJsonEncoder))
            logger.info(f"Updated job state for {namespace}/{job_name}: {current_state}")

            # Exit if in terminal state
            if current_state in ("Succeeded", "Failed"):
                logger.info(f"Terminal state reached for {namespace}/{job_name} => {current_state}. Exiting.")
                return
        except Exception as e:
            if "404" in str(e) or "NotFound" in str(e):
                logger.info(f"Job {namespace}/{job_name} not found. Exiting poll_job_state.")
                redis_client.setex(state_key, 3600, "NotFound")
                return
            logger.warning(f"Error polling state for {namespace}/{job_name}: {str(e)}")

        time.sleep(poll_interval)


def poll_job_description(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Periodically call fetch_job_description(...) and store it in Redis.

    Polls periodically until the stop event is set.
    """
    logger.info(f"Started poll_job_description for {namespace}/{job_name}")
    while not stop_event.is_set():
        logger.debug(f"Polling job description for {namespace}/{job_name}")
        try:
            fetch_job_description(namespace, job_name)
        except Exception as e:
            if "404" in str(e) or "NotFound" in str(e):
                logger.info(f"Job {namespace}/{job_name} not found during description polling. Exiting.")
                return
            logger.warning(f"Error polling description for {namespace}/{job_name}: {str(e)}")
        time.sleep(poll_interval)

    logger.info(f"Exiting poll_job_description for {namespace}/{job_name}")


def poll_job_logs(namespace: str, job_name: str, poll_interval: int, stop_event: threading.Event) -> None:
    """Periodically fetch logs for the Job's pods and store them as a list of dicts."""
    logger.info(f"Started poll_job_logs for {namespace}/{job_name}")
    core_api = kubernetes.client.CoreV1Api()
    logs_key = redis_key_for_job_logs(namespace, job_name)

    # We'll repeatedly update aggregated_logs
    aggregated_logs: List[Dict[str, str]] = []

    state_key = redis_key_for_job_state(namespace, job_name)

    while not stop_event.is_set():
        logger.debug(f"Polling job logs for {namespace}/{job_name}")
        try:
            # Only fetch logs if the Job is at least Running or in a terminal state
            job_state_bytes = redis_client.get(state_key)
            current_state = job_state_bytes.decode("utf-8") if job_state_bytes else "Pending"
            if current_state not in ("Running", "Succeeded", "Failed"):
                time.sleep(poll_interval)
                continue

            # Find all pods for this job
            pods = core_api.list_namespaced_pod(namespace, label_selector=f"job-name={job_name}").items

            aggregated_logs = []
            for pod in pods:
                pod_name = pod.metadata.name
                # A pod can have multiple containers
                for container in pod.spec.containers:
                    # Example: skip sidecar or init containers if you do not want them
                    # if container.name in ("playout-init", "playout-sidecar"):
                    #     continue

                    try:
                        logs = core_api.read_namespaced_pod_log(
                            name=pod_name,
                            namespace=namespace,
                            container=container.name,
                            # You can adjust or remove tail_lines as needed
                            tail_lines=200,
                            _preload_content=True,
                        )
                        aggregated_logs.append(
                            {
                                "pod_name": pod_name,
                                "container_name": container.name,
                                "logs": logs,
                            }
                        )
                    except Exception as inner_e:
                        logger.warning(f"Error reading logs from {pod_name}/{container.name}: {str(inner_e)}")
                        aggregated_logs.append(
                            {
                                "pod_name": pod_name,
                                "container_name": container.name,
                                "logs": f"Error: {str(inner_e)}",
                            }
                        )

            # Store logs as a list of dicts
            redis_client.setex(logs_key, 3600, json.dumps(aggregated_logs, cls=CustomJsonEncoder))
            logger.info(f"Updated logs for {namespace}/{job_name}")
        except Exception as e:
            logger.warning(f"Error polling logs for {namespace}/{job_name}: {str(e)}")

        time.sleep(poll_interval)

    logger.info(f"Exiting poll_job_logs for {namespace}/{job_name}")
    # On exit, optionally store the final logs snapshot again
    redis_client.setex(logs_key, 3600, json.dumps(aggregated_logs, cls=CustomJsonEncoder))


def poll_job(namespace: str, job_name: str, poll_interval: int = 2) -> None:
    """Poll job description, state, and logs concurrently until a terminal state is reached.

    1) Spawns a thread for poll_job_state (updates job state + job object).
    2) Spawns a thread for poll_job_description (updates 'describe'-like data).
    3) Once the job is Running (or terminal), spawns poll_job_logs for logs.
    """
    logger.info(f"Started poll_job for {namespace}/{job_name}")
    stop_event = threading.Event()

    # 1) Start state and description polling right away
    state_thread = threading.Thread(target=poll_job_state, args=(namespace, job_name, poll_interval, stop_event))
    description_thread = threading.Thread(
        target=poll_job_description, args=(namespace, job_name, poll_interval, stop_event)
    )
    state_thread.start()
    description_thread.start()

    # 2) Store an initial job object
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

    # 3) Wait until the job is "Running" or in a terminal state to start logs polling
    while True:
        try:
            job_state_bytes = redis_client.get(redis_key_for_job_state(namespace, job_name))
            if job_state_bytes:
                current_state = job_state_bytes.decode("utf-8")
                if current_state == "Running":
                    logs_thread = threading.Thread(
                        target=poll_job_logs, args=(namespace, job_name, poll_interval, stop_event)
                    )
                    logs_thread.start()
                    break
                elif current_state in ("Succeeded", "Failed", "NotFound"):
                    # No need to poll logs if never started or already done
                    break
            logger.debug(f"Waiting for job to enter 'Running' or terminal: {namespace}/{job_name}")
        except Exception as e:
            logger.warning(f"Error while waiting for job state: {str(e)}")
        time.sleep(poll_interval)

    # 4) Wait for the state polling thread to detect a terminal state
    state_thread.join()

    # 5) Signal all polling threads to stop
    stop_event.set()
    description_thread.join()
    if logs_thread:
        logs_thread.join()

    logger.info(f"Completed polling threads for {namespace}/{job_name}")

    # Retrieve final job data if desired
    try:
        job_obj = batch_api.read_namespaced_job(job_name, namespace)
        logger.info(f"Final job data retrieved for {namespace}/{job_name}")
    except Exception as e:
        logger.error(f"Failed to retrieve final job data for {namespace}/{job_name}: {str(e)}")


def send_callback(namespace: str, job_name: str, state: str, job_obj: Any) -> None:
    """Send a callback with the current job state, including 'tuskr_state' or 'tuskr_failure_reason' if relevant."""
    logger.info(f"Callback: job {namespace}/{job_name} in {state}")

    state_key = redis_key_for_job_state(namespace, job_name)
    final_state = redis_client.get(state_key)
    if isinstance(final_state, bytes):
        final_state = final_state.decode("utf-8")

    job_dict = job_obj.to_dict()
    job_dict["tuskr_state"] = final_state

    if final_state == "Failed":
        conditions = job_dict.get("status", {}).get("conditions", [])
        for condition in conditions:
            if condition.get("type") == "Failed" and condition.get("status") == "True":
                job_dict["tuskr_failure_reason"] = condition.get("message")
                break

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
                # Remove the callback registration for terminal jobs
                if final_state in ("Succeeded", "Failed"):
                    redis_client.delete(callback_key)
                log_msg = f"Callback for {namespace}/{job_name} => {final_state}"
                if "tuskr_failure_reason" in job_dict:
                    log_msg += f"; reason: {job_dict['tuskr_failure_reason']}"
                logger.info(log_msg)
            except Exception as e:
                traceback.print_exc()
                logger.error(f"Callback failed for {namespace}/{job_name}: {str(e)}")


def handle_create_job(job_obj: Dict[str, Any], logger: logging.Logger, **kwargs: Any) -> None:
    """Start a new thread to poll job state, description, and logs when a job is created."""
    namespace = job_obj["metadata"]["namespace"]
    job_name = job_obj["metadata"]["name"]
    if not namespace or not job_name:
        logger.error("Namespace and job name are required.")
        return

    metadata = job_obj.get("metadata", {})
    annotations = metadata.get("annotations", {})

    if annotations.get("tuskr.io/launched-by") == "tuskr":
        logger.info(f"Job {namespace}/{job_name} launched by tuskr; starting poll_job thread.")
        t = threading.Thread(target=poll_job, args=(namespace, job_name))
        t.daemon = True
        t.start()
        logger.info(f"poll_job thread started for {namespace}/{job_name}")
    else:
        logger.info(f"Job {namespace}/{job_name} not launched by tuskr; skipping.")


def handle_delete_job(job_obj: Dict[str, Any], logger: logging.Logger, **kwargs: Any) -> None:
    """Handle job deletion events."""
    namespace = job_obj["metadata"]["namespace"]
    job_name = job_obj["metadata"]["name"]
    if not namespace or not job_name:
        logger.error("Namespace and job name are required.")
        return

    metadata = job_obj.get("metadata", {})
    annotations = metadata.get("annotations", {})

    if annotations.get("tuskr.io/launched-by") == "tuskr":
        logger.info(f"Handling delete event for Job {namespace}/{job_name}, launched by tuskr.")
        # Send callback for the deleted job.
        state = redis_client.get(redis_key_for_job_state(namespace, job_name))
        if state:
            current_state = state.decode("utf-8")
            send_callback(namespace, job_name, current_state, job_obj)
        else:
            logger.info(f"No state found in redis for {namespace}/{job_name} to send callback.")
    else:
        logger.info(f"Job {namespace}/{job_name} not launched by tuskr; skipping delete handling.")


def handle_update_job(job_obj: Dict[str, Any], logger: logging.Logger, **kwargs: Any) -> None:
    """Handle job update events."""
    namespace = job_obj["metadata"]["namespace"]
    job_name = job_obj["metadata"]["name"]
    if not namespace or not job_name:
        logger.error("Namespace and job name are required.")
        return

    metadata = job_obj.get("metadata", {})
    annotations = metadata.get("annotations", {})

    if annotations.get("tuskr.io/launched-by") == "tuskr":
        logger.info(f"Handling update event for Job {namespace}/{job_name}, launched by tuskr.")
        # Possibly send a callback for the updated job
        state = redis_client.get(redis_key_for_job_state(namespace, job_name))
        if state:
            current_state = state.decode("utf-8")
            send_callback(namespace, job_name, current_state, job_obj)
        else:
            logger.info(f"No state found for {namespace}/{job_name} to send callback.")
    else:
        logger.info(f"Job {namespace}/{job_name} not launched by tuskr; skipping update handling.")
