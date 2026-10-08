"""The churn model: a small PyTorch MLP. Shared by tuning trials and the final training step.

Scaling lives inside the model, so serving needs no preprocessing: input [N, len(features)] raw values (after
imputation) -> churn probability [N]. Hyperparameters (training.hyperparameters in usecase.yaml):
hidden_units, epochs, batch_size, learning_rate, dropout, weight_decay.
"""

from __future__ import annotations

import logging

import numpy as np

DEFAULTS = {"hidden_units": 32, "epochs": 40, "batch_size": 128, "learning_rate": 0.003, "dropout": 0.1,
            "weight_decay": 1e-4}


def build(mean, std, hidden: int, dropout: float):
    import torch
    from torch import nn

    class ChurnNet(nn.Module):
        def __init__(self, mean: torch.Tensor, std: torch.Tensor, hidden: int, dropout: float):
            super().__init__()
            self.register_buffer("mean", mean)
            self.register_buffer("std", std)
            self.net = nn.Sequential(
                nn.Linear(mean.numel(), hidden), nn.ReLU(), nn.Dropout(dropout),
                nn.Linear(hidden, max(hidden // 2, 2)), nn.ReLU(), nn.Linear(max(hidden // 2, 2), 1),
            )

        def logits(self, x: torch.Tensor) -> torch.Tensor:
            return self.net((x - self.mean) / self.std).squeeze(-1)

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            return torch.sigmoid(self.logits(x))

    return ChurnNet(mean, std, hidden, dropout)


def fit(x: np.ndarray, y: np.ndarray, hyperparameters: dict, seed: int):
    """Returns the trained model (eval mode) and the final training loss."""
    import torch
    from torch import nn

    hp = {**DEFAULTS, **hyperparameters}
    torch.manual_seed(seed)
    xt, yt = torch.tensor(x, dtype=torch.float32), torch.tensor(y, dtype=torch.float32)
    net = build(xt.mean(dim=0), xt.std(dim=0).clamp_min(1e-6), int(hp["hidden_units"]), float(hp["dropout"]))
    optimizer = torch.optim.AdamW(net.parameters(), lr=float(hp["learning_rate"]),
                                  weight_decay=float(hp["weight_decay"]))
    loss_fn = nn.BCEWithLogitsLoss()
    loader = torch.utils.data.DataLoader(torch.utils.data.TensorDataset(xt, yt), batch_size=int(hp["batch_size"]),
                                         shuffle=True, generator=torch.Generator().manual_seed(seed))
    epochs = int(hp["epochs"])
    loss = float("nan")
    for epoch in range(1, epochs + 1):
        net.train()
        total = 0.0
        for xb, yb in loader:
            optimizer.zero_grad()
            batch_loss = loss_fn(net.logits(xb), yb)
            batch_loss.backward()
            optimizer.step()
            total += batch_loss.item() * len(xb)
        loss = total / len(xt)
        if epoch == 1 or epoch % 10 == 0 or epoch == epochs:
            logging.info("epoch %d/%d loss=%.4f", epoch, epochs, loss)
    net.eval()
    return net, loss


def predict(net, x: np.ndarray) -> np.ndarray:
    import torch

    with torch.no_grad():
        return net(torch.tensor(np.asarray(x, dtype="float32"))).numpy()


def load(model_dir: str):
    import torch

    return torch.jit.load(f"{model_dir}/model.pt")
