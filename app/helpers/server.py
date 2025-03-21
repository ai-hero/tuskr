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

"""Starts the Falcon HTTP server and configures routes for Tuskr operations."""

import json
import logging
import threading
from functools import partial
from wsgiref.simple_server import make_server

import falcon
from falcon import media

from helpers.encoder import CustomJsonDecoder, CustomJsonEncoder
from helpers.job_context import JobContextResource
from helpers.job_description import JobDescribeResource
from helpers.job_launch import LaunchResource
from helpers.job_logs import JobLogsResource
from helpers.job_state import JobResource

logger = logging.getLogger(__name__)


def start_http_server(port: int = 8080) -> None:
    """Start a Falcon HTTP server in a separate thread, so Kopf can run concurrently."""
    app = falcon.App()

    # Setup JSON handlers
    json_handler = media.JSONHandler(
        dumps=partial(json.dumps, cls=CustomJsonEncoder, sort_keys=True),
        loads=partial(json.loads, cls=CustomJsonDecoder),
    )
    extra_handlers = {"application/json": json_handler}
    app.req_options.media_handlers.update(extra_handlers)
    app.resp_options.media_handlers.update(extra_handlers)

    # Add routes
    app.add_route("/launch", LaunchResource())  # Create a job from a JobTemplate
    app.add_route("/jobs/{namespace}/{job_name}", JobResource())  # Job state (GET/DELETE)
    app.add_route("/jobs/{namespace}/{job_name}/describe", JobDescribeResource())  # Describe
    app.add_route("/jobs/{namespace}/{job_name}/logs", JobLogsResource())  # Logs
    # New endpoint for job to retrieve env-vars, inputs, or post outputs
    app.add_route("/jobs/{namespace}/{job_name}/context", JobContextResource())

    def run_server() -> None:
        with make_server("", port, app) as httpd:
            logger.info(f"Falcon HTTP server running on port {port}...")
            httpd.serve_forever()

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
