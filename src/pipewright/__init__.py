"""Pipewright: metadata-driven MLOps on Google Cloud."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("pipewright")
except PackageNotFoundError:  # running from a source checkout without install
    __version__ = "0.0.0"

API_VERSION = "pipewright/v1"
