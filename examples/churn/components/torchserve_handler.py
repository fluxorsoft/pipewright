"""TorchServe handler packaged into model.mar for Agent Platform prebuilt PyTorch serving.

Runs inside us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4 (Python 3.9,
TorchServe 0.12), so keep it 3.9-compatible and free of extra dependencies.

That image sets `service_envelope=json`: TorchServe unwraps the Agent Platform request

    {"instances": [[12, 70.5, 846.0, 1, 2, 1, 1, 64.3], ...]}

and calls the handler with one item per instance, each item being the bare
feature list, then wraps the returned list as {"predictions": [...]}. So the
handler must return exactly one value (the churn probability) per item.

Without the envelope (plain TorchServe), an item is {"body": <request JSON>};
that form is accepted too and yields a list of probabilities for that request.
Feature order is recorded in features.json (packaged alongside the model).
"""

import json

import torch
from ts.torch_handler.base_handler import BaseHandler


def _decode(value):
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    return value


class TabularChurnHandler(BaseHandler):
    def preprocess(self, data):
        # For each item: (number of rows, whether it was a single instance).
        self._layout = []
        rows = []
        for item in data:
            item = _decode(item)
            if isinstance(item, dict):
                item = _decode(item.get("data", item.get("body", item)))
            if isinstance(item, dict):
                item = item.get("instances", [])
            if item and isinstance(item[0], (list, tuple)):
                self._layout.append((len(item), False))
                rows.extend(item)
            else:
                self._layout.append((1, True))
                rows.append(item)
        return torch.tensor(rows, dtype=torch.float32)

    def inference(self, data, *args, **kwargs):
        with torch.no_grad():
            return self.model(data.to(self.device))

    def postprocess(self, data):
        probabilities = data.reshape(-1).cpu().tolist()
        out, start = [], 0
        for size, single in self._layout:
            chunk = probabilities[start : start + size]
            out.append(chunk[0] if single else chunk)
            start += size
        return out
