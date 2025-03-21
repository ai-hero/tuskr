"""Tuskr Controller entry point."""
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
from typing import Any

import kopf
import kubernetes

from helpers.job_watch import watch_jobs
from helpers.jobtemplate_watch import handle_create_jobtemplate, handle_delete_jobtemplate, handle_update_jobtemplate
from helpers.server import start_http_server

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@kopf.on.startup()  # type: ignore
def startup_fn(logger: logging.Logger, **kwargs: Any) -> None:
    """Kopf startup hook: load Kubernetes config, initialize things."""
    logger.info("Tuskr controller is starting up...")

    # If running in-cluster:
    kubernetes.config.load_incluster_config()
    # If testing locally:
    # kubernetes.config.load_kube_config()

    # Start the Falcon HTTP server in a separate thread
    start_http_server()
    logger.info("HTTP server started in background thread.")


#
# Register watchers for the JobTemplate CRD
#
@kopf.on.create("tuskr.io", "v1alpha1", "jobtemplates")  # type: ignore
def create_jobtemplate(body: dict[str, Any], spec: dict[str, Any], **kwargs: Any) -> Any:
    """Handle creation of a JobTemplate custom resource."""
    return handle_create_jobtemplate(body, spec, **kwargs)


@kopf.on.update("tuskr.io", "v1alpha1", "jobtemplates")  # type: ignore
def update_jobtemplate(body: dict[str, Any], spec: dict[str, Any], **kwargs: Any) -> Any:
    """Handle update of a JobTemplate custom resource."""
    return handle_update_jobtemplate(body, spec, **kwargs)


@kopf.on.delete("tuskr.io", "v1alpha1", "jobtemplates")  # type: ignore
def delete_jobtemplate(body: dict[str, Any], spec: dict[str, Any], **kwargs: Any) -> Any:
    """Handle deletion of a JobTemplate custom resource."""
    return handle_delete_jobtemplate(body, spec, **kwargs)


#
# Register event-based watcher for Jobs
#
@kopf.on.event("batch", "v1", "jobs")  # type: ignore
def on_job_event(event: dict[str, Any], **kwargs: Any) -> Any:
    """Handle events for Kubernetes Jobs."""
    return watch_jobs(event, **kwargs)
