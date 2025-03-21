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
"""Redis configuration and helpers."""

import os

import redis  # type: ignore

# Environment-based configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Initialize Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Time to live in seconds (60 minutes)
REDIS_TTL = 60 * 60


# ------------------------------------------------------------------------------
# Helpers for Redis Keys
# ------------------------------------------------------------------------------
def job_redis_key(namespace: str, job_name: str) -> str:
    """Return the Redis key for a job."""
    return f"jobs::{namespace}::{job_name}"


def pod_redis_key(namespace: str, pod_name: str) -> str:
    """Return the Redis key for a pod."""
    return f"pods::{namespace}::{pod_name}"


def event_redis_key(namespace: str, kind: str, obj_name: str) -> str:
    """Return the Redis key for an event."""
    return f"events::{namespace}::{kind.lower()}::{obj_name}"


def job_pod_set_key(namespace: str, job_name: str) -> str:
    """Return the Redis key for a set of pods for a job."""
    return f"job_pods::{namespace}::{job_name}"


# Example: If you store logs under this key
def job_logs_key(namespace: str, job_name: str) -> str:
    """Return the Redis key for logs for a job."""
    return f"job_logs::{namespace}::{job_name}"


# Access token key for a given job
def job_access_token_key(namespace: str, job_name: str) -> str:
    """Return the Redis key for an access token for a job."""
    return f"job_access_token::{namespace}::{job_name}"


# Inputs key
def job_inputs_key(namespace: str, job_name: str) -> str:
    """Return the Redis key for inputs for a job."""
    return f"job_inputs::{namespace}::{job_name}"
