"""The Ray job tune_on_ray runs on the cluster: scores every tuning trial in parallel, one Ray task per trial.

    python -m ray_tune_job gs://<trials artifact>     reads config.json there, writes results.json next to it

It runs on the cluster's nodes (the use case's Ray image), so it imports the same mlkit and churn_net the rest of
the pipeline uses: a trial here scores exactly as tune.py's tune_trial would.
"""

import io
import json
import sys


def _blob(uri: str):
    from google.cloud import storage

    bucket, _, name = uri.removeprefix("gs://").partition("/")
    return storage.Client().bucket(bucket).blob(name)


def score(data: dict, hyperparameters: dict, config: dict) -> dict:
    """One trial: train on the train split, score tuning.metric on validation."""
    from churn_net import fit, predict
    from mlkit import compute_metrics

    net, _ = fit(data["x_train"], data["y_train"], hyperparameters, config["seed"])
    value = compute_metrics(data["y_val"], predict(net, data["x_val"]), config["threshold"],
                            [config["metric"]])[config["metric"]]
    return {"hyperparameters": hyperparameters, "score": value}


def arrays(config: dict, train, validation) -> dict:
    features, label = config["features"], config["label"]
    return {"x_train": train[features].to_numpy("float32"), "y_train": train[label].to_numpy("float32"),
            "x_val": validation[features].to_numpy("float32"), "y_val": validation[label].to_numpy("float32")}


def main(base: str) -> None:
    import pandas as pd
    import ray

    config = json.loads(_blob(f"{base}/config.json").download_as_text())
    train = pd.read_parquet(io.BytesIO(_blob(config["train"]).download_as_bytes()))
    validation = pd.read_parquet(io.BytesIO(_blob(config["validation"]).download_as_bytes()))
    ray.init()                                   # inside a Ray job: joins the cluster
    data = ray.put(arrays(config, train, validation))

    @ray.remote(num_cpus=1)
    def trial(data: dict, hyperparameters: dict) -> dict:
        import torch

        torch.set_num_threads(1)                 # one CPU per trial, so trials spread over the nodes
        result = score(data, hyperparameters, config)
        result["node"] = ray.util.get_node_ip_address()
        return result

    results = ray.get([trial.remote(data, hp) for hp in config["trials"]])
    _blob(f"{base}/results.json").upload_from_string(json.dumps(results), content_type="application/json")
    nodes = sorted({r["node"] for r in results})
    print(f"{len(results)} trials on {len(nodes)} node(s): {nodes}")
    for r in sorted(results, key=lambda r: r["score"] or 0, reverse=True):
        print(f"  {config['metric']}={r['score']:.4f} {r['hyperparameters']}")


if __name__ == "__main__":
    main(sys.argv[1])
