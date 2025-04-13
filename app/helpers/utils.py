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

"""Utility functions for the Tuskr application."""

import json
import random
import string
from typing import Optional, Tuple

from helpers.constants import (
    JOB_DATA_PREFIX,
    JOB_DESCRIBE_PREFIX,
    JOB_LOGS_PREFIX,
    JOB_STATE_PREFIX,
    TOKEN_PREFIX,
)


def generate_random_suffix(length: int = 5) -> str:
    """Generate a short random string of letters/digits."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choices(chars, k=length))


def redis_key_for_job_data(namespace: str, job_name: str) -> str:
    """Return the Redis key for job data given a namespace and job name."""
    return f"{JOB_DATA_PREFIX}:{namespace}:{job_name}"


def redis_key_for_job_state(namespace: str, job_name: str) -> str:
    """Return the Redis key for job state given a namespace and job name."""
    return f"{JOB_STATE_PREFIX}:{namespace}:{job_name}"


def redis_key_for_job_describe(namespace: str, job_name: str) -> str:
    """Return the Redis key for job description given a namespace and job name."""
    return f"{JOB_DESCRIBE_PREFIX}:{namespace}:{job_name}"


def redis_key_for_job_logs(namespace: str, job_name: str) -> str:
    """Return the Redis key for job logs given a namespace and job name."""
    return f"{JOB_LOGS_PREFIX}:{namespace}:{job_name}"


def parse_token(token: str) -> Tuple[Optional[str], Optional[str]]:
    """Given a token, look it up in Redis to retrieve its corresponding namespace and job name.

    Return:
    ------
        tuple: (namespace, job_name) if valid, otherwise (None, None).

    """
    from helpers.redis_client import redis_client

    token_key = f"{TOKEN_PREFIX}:{token}"
    raw = redis_client.get(token_key)
    if not raw:
        return (None, None)
    try:
        data = json.loads(raw)
        return (data["namespace"], data["job_name"])
    except Exception:
        return (None, None)
