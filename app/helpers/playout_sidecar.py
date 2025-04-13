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

# MIT License
#
# (Copyright notice...)

"""Sidecar for playout jobs using shareProcessNamespace=true."""

import base64
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import psutil


def get_container_cgroup_path(pid: int) -> str:
    """Get the cgroup path for a given PID."""
    cgroup_path = ""
    cgroup_file = f"/proc/{pid}/cgroup"
    try:
        with open(cgroup_file, encoding="utf-8") as f:
            for line in f:
                # Typically lines look like: 1:name=systemd:/kubepods/...
                parts = line.strip().split(":")
                if len(parts) == 3:
                    _, _, path = parts
                    if "kubepods" in path:  # a common substring in K8s cgroups
                        cgroup_path = path
                        break
    except FileNotFoundError:
        pass
    return cgroup_path


def all_other_containers_exited(sidecar_cgroup: str) -> bool:
    """Check if all other containers have exited."""
    for proc in psutil.process_iter(["pid", "name"]):
        pid = proc.info["pid"]
        if pid == 1:
            # Usually the Pod has a 'pause' or 'init' container as PID 1, skip it if needed
            continue

        cgroup_path = get_container_cgroup_path(pid)
        if cgroup_path and cgroup_path != sidecar_cgroup:
            # We found a process that belongs to a different container
            return False

    return True


def main() -> None:
    """Run the sidecar."""
    namespace = os.environ.get("NAMESPACE")
    job_name = os.environ.get("JOB_NAME")
    token = os.environ.get("TUSKR_JOB_TOKEN")
    # This container's cgroup path
    sidecar_cgroup = get_container_cgroup_path(os.getpid())

    print("[sidecar] shareProcessNamespace sidecar started. Polling for other containers to exit...")

    while True:
        if all_other_containers_exited(sidecar_cgroup):
            print("[sidecar] All non-sidecar containers have terminated.")
            break
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
