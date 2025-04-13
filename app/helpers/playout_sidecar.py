#!/usr/bin/env python3
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
"""Sidecar for playout jobs."""

import base64
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx
from kubernetes import client, config
from kubernetes.client import V1PodStatus  # new import for type annotation
from kubernetes.client.rest import ApiException


def all_user_containers_terminated(pod_status: V1PodStatus) -> bool:
    """Return True if all containers that do NOT start with 'playout-' have a 'terminated' state."""
    cstatuses = pod_status.container_statuses
    if not cstatuses:
        return False

    for cs in cstatuses:
        if not cs.name.startswith("playout-"):
            if not cs.state or not cs.state.terminated:
                return False
    return True


def main() -> None:
    """Run the sidecar."""
    namespace = os.environ.get("NAMESPACE")
    job_name = os.environ.get("JOB_NAME")
    token = os.environ.get("TUSKR_JOB_TOKEN")
    pod_name = os.environ.get("POD_NAME")

    # If running in-cluster, use config.load_incluster_config()
    try:
        config.load_incluster_config()
    except Exception:  # pylint: disable=broad-except
        print("[sidecar] Unable to load in-cluster config; trying default config (dev mode).")
        config.load_kube_config()

    core_api = client.CoreV1Api()

    print("[sidecar] Sidecar started. Polling for main containers to terminate...")

    while True:
        try:
            pod = core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
            if all_user_containers_terminated(pod.status):
                print("[sidecar] All non-playout containers have terminated.")
                break
        except ApiException as e:
            print(f"[sidecar] Error reading pod: {e}", file=sys.stderr)
        except Exception as exc:
            print(f"[sidecar] Unexpected error: {exc}", file=sys.stderr)

        # Sleep a bit before re-checking
        time.sleep(2)

    # Gather outputs
    outputs_dir = Path("/mnt/data/outputs")
    out_files = list(outputs_dir.glob("*"))
    if not out_files:
        # No outputs at all
        post_data: dict[str, Any] = {"outputs": {}}
    else:
        outputs_dict = {}
        for out_file in out_files:
            if out_file.name == "done":
                # ignoring "done" sentinel
                continue
            with out_file.open("rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")
            outputs_dict[out_file.name] = encoded
        post_data = {"outputs": outputs_dict}

    # Post outputs to the controller
    context_url = (
        f"http://tuskr-controller.tuskr.svc.cluster.local:8080/" f"jobs/{namespace}/{job_name}/context?token={token}"
    )
    try:
        with httpx.Client(timeout=30.0) as httpx_client:
            r = httpx_client.post(context_url, json=post_data)
            r.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"[sidecar] Failed to post outputs: {exc}", file=sys.stderr)
        sys.exit(1)

    print("[sidecar] Successfully posted outputs. Exiting...")
    sys.exit(0)


if __name__ == "__main__":
    main()
