"""Module for handling JobTemplate events."""
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

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_create_jobtemplate(body: Dict[str, Any], spec: Dict[str, Any], **kwargs: Any) -> Dict[str, str]:
    """Handle creation of a job template."""
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was created with spec: {spec}")
    return {"message": f"Created JobTemplate {name}"}


def handle_update_jobtemplate(body: Dict[str, Any], spec: Dict[str, Any], **kwargs: Any) -> Dict[str, str]:
    """Handle update of a job template."""
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was updated with spec: {spec}")
    return {"message": f"Updated JobTemplate {name}"}


def handle_delete_jobtemplate(body: Dict[str, Any], spec: Dict[str, Any], **kwargs: Any) -> Dict[str, str]:
    """Handle deletion of a job template."""
    name = body["metadata"]["name"]
    logger.info(f"JobTemplate {name} was deleted.")
    return {"message": f"Deleted JobTemplate {name}"}
