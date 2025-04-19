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

# Copyright (c) 2024 A.I. Hero, Inc.
# All Rights Reserved.
#
# This software and associated documentation files (the "Software") are provided "as is", without warranty
# of any kind, express or implied, including but not limited to the warranties of merchantability, fitness
# for a particular purpose, and noninfringement. In no event shall the authors or copyright holders be
# liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise,
# arising from, out of, or in connection with the Software or the use or other dealings in the Software.
#
# Unauthorized copying of this file, via any medium, is strictly prohibited.

"""Helper functions for encoding data."""

import traceback
from datetime import date, datetime, time
from json import JSONDecoder, JSONEncoder
from typing import Any

from bson import ObjectId


class CustomJsonEncoder(JSONEncoder):
    """Custom JSON encoder for encoding data."""

    def default(self, o: Any) -> Any:
        """Encode the data."""
        if isinstance(o, datetime) or isinstance(o, date) or isinstance(o, time):
            return o.isoformat()
        if isinstance(o, ObjectId):
            return str(o)
        # Let the base class default method raise the TypeError
        return JSONEncoder.default(self, o)


class CustomJsonDecoder(JSONDecoder):
    """Custom JSON decoder for decoding data."""

    def __init__(self, *args: Any, **kwargs: Any):
        """Initialize the decoder."""
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj: Any) -> Any:
        """Decode the data."""
        try:
            for key, value in obj.items():
                if isinstance(value, str):
                    try:
                        # Try to parse datetime
                        obj[key] = datetime.fromisoformat(value)
                    except ValueError:
                        # Not a datetime string, continue
                        pass
                    # If you expect ObjectId strings to be in a specific format,
                    # you can decode them here as well. Example:
                    if ObjectId.is_valid(value):
                        obj[key] = ObjectId(value)
            return obj
        except Exception as e:
            traceback.print_exc()
            raise e
