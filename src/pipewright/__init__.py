"""Pipewright: configurable, governed MLOps on Google Cloud. Distribution name: pipewright-mlops."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("pipewright-mlops")
except PackageNotFoundError:  # running from a source checkout without install
    __version__ = "0.0.0"

API_VERSION = "pipewright/v1"
