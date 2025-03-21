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

"""Server module for the Falcon app."""

import threading
import traceback
from wsgiref.simple_server import make_server

import falcon
from falcon import media
from pydantic import ValidationError
from resources import JobDescribeResource, JobLogsResource, JobPayloadResource, JobResource, LaunchResource
from utils import CustomJsonDecoder, CustomJsonEncoder, logger


def handle_validation_error(_: falcon.Request, resp: falcon.Request, exception: falcon.HTTPError) -> None:
    """Handle validation errors."""
    logger.error(f"Validation error: {exception}")
    resp.status = falcon.HTTP_422
    resp.media = {
        "title": "Unprocessable Entity",
        "description": "The request contains invalid data.",
        "errors": exception.errors(),
    }


def custom_handle_uncaught_exception(_: falcon.Request, resp: falcon.Request, exception: falcon.HTTPError) -> None:
    """Handle uncaught exceptions."""
    traceback.print_exc()
    resp.status = falcon.HTTP_500
    resp.media = f"{exception}"


def create_app() -> falcon.App:
    """Create the Falcon app."""
    app = falcon.App()

    # Error handlers
    app.add_error_handler(ValidationError, handle_validation_error)
    app.add_error_handler(Exception, custom_handle_uncaught_exception)

    # JSON handlers
    json_handler = media.JSONHandler(
        # Provide a custom encoder/decoder if needed
        dumps=lambda obj: CustomJsonEncoder().encode(obj),
        loads=lambda s: CustomJsonDecoder().decode(s),
    )
    extra_handlers = {
        "application/json": json_handler,
    }
    app.req_options.media_handlers.update(extra_handlers)
    app.resp_options.media_handlers.update(extra_handlers)

    # Routes
    app.add_route("/launch", LaunchResource())
    app.add_route("/jobs/{namespace}/{job_name}", JobResource())
    app.add_route("/jobs/{namespace}/{job_name}/describe", JobDescribeResource())
    app.add_route("/jobs/{namespace}/{job_name}/logs", JobLogsResource())
    app.add_route("/jobs/{namespace}/{job_name}/payload", JobPayloadResource())

    return app


def start_http_server() -> None:
    """Run the Falcon server on port 8080 in a background thread."""
    app = create_app()

    def _run_server() -> None:
        with make_server("", 8080, app) as httpd:
            logger.info("Falcon HTTP server running on port 8080...")
            httpd.serve_forever()

    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()
    logger.info("Started Falcon HTTP server in the background.")
