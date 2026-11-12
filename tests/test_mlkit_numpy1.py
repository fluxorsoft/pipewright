"""GA QA (found live on KubeRay): the example's metrics run on the Ray image's numpy 1.x too; np.trapezoid (numpy 2
only) failed every tuning trial on Ray."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parents[1] / "examples/churn/components"))


def test_auc_needs_no_numpy_2_api():
    import mlkit

    src = (Path(__file__).parents[1] / "examples/churn/components/mlkit.py").read_text(encoding="utf-8")
    assert "np.trapezoid(" not in src and "np.trapz(" not in src
    y = np.array([0, 0, 1, 1])
    assert mlkit._auc(y, np.array([0.1, 0.4, 0.35, 0.8])) == 0.75
