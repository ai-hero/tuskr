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
"""Initialization script for fetching job context in Tuskr."""

import os
import sys
from pathlib import Path

import httpx


def main() -> None:
    """Fetch job context and write input files."""
    namespace = os.environ.get("NAMESPACE")
    job_name = os.environ.get("JOB_NAME")
    token = os.environ.get("TUSKR_JOB_TOKEN")

    context_url = (
        f"http://tuskr-controller.tuskr.svc.cluster.local:8080/jobs/{namespace}/{job_name}/context?token={token}"
    )

    try:
        # We can add timeouts, retries, etc. with httpx
        with httpx.Client(timeout=30.0) as client:
            r = client.get(context_url)
            r.raise_for_status()
            context = r.json()
    except httpx.HTTPError as exc:
        print(f"[init_fetch] Failed to fetch job context: {exc}", file=sys.stderr)
        sys.exit(1)

    # Prepare input directory
    input_dir = Path("/mnt/data/inputs")
    input_dir.mkdir(parents=True, exist_ok=True)

    # Write each input file
    inputs = context.get("inputs", {})
    for filename, content in inputs.items():
        file_path = input_dir / filename
        with file_path.open("w") as f:
            f.write(content)
        print(f"[init_fetch] Wrote input file: {filename}")

    # Create a .env file for environment variables
    env_vars = context.get("env_vars", {})
    env_file_path = input_dir / ".env"
    with env_file_path.open("w") as env_file:
        for varname, value in env_vars.items():
            # Quote or escape if needed
            line = f'export {varname}="{value}"\n'
            env_file.write(line)

    print("[init_fetch] Successfully fetched and wrote context data.")
    sys.exit(0)


if __name__ == "__main__":
    main()
