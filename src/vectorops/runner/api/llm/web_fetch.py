"""Compatibility exports for HTTP tools."""

from vectorops.runner.api.builtin_tools import web_fetch as _implementation

TextBody = _implementation.TextBody
BinaryBody = _implementation.BinaryBody
WebFetchArguments = _implementation.WebFetchArguments
web_fetch_tool = _implementation.web_fetch_tool
