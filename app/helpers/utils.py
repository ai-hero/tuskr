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

"""Utility functions for the application."""

import json
import logging
import random
import string
from typing import Any

# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ------------------------------------------------------------------------------
# Custom JSON Encoder/Decoder
# ------------------------------------------------------------------------------
class CustomJsonEncoder(json.JSONEncoder):
    """JsonEncoder with support for additional types."""

    def default(self, obj: Any) -> Any:
        """Serialize additional types."""
        return super().default(obj)


class CustomJsonDecoder(json.JSONDecoder):
    """JsonDecoder with support for additional types."""

    pass


# ------------------------------------------------------------------------------
# Helper to generate random suffix
# ------------------------------------------------------------------------------
def generate_random_suffix(length: int = 5) -> str:
    """Generate a random suffix of the given length."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choices(chars, k=length))
