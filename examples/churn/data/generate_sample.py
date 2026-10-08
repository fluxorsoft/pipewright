"""Generates synthetic customer-churn CSVs for the example.

The label is drawn from a logistic function of the features, so a model can genuinely learn it (AUC ~0.8)
with noise left over. Each customer is observed once, at a month-end `snapshot_date` across 12 months, which
gives the time split a real past and future. A small fraction of total_charges is blank to exercise the data
contract and imputation.

    python data/generate_sample.py                                   # training data, 5000 rows
    python data/generate_sample.py --scoring --out data/scoring_customers.csv            # next month, no label
    python data/generate_sample.py --scoring --drift 1.0 --out data/scoring_drifted.csv  # shifted population
"""

import argparse
import calendar
import csv
import math
import random
from datetime import date


def month_ends(start: date, count: int) -> list[date]:
    out, y, m = [], start.year, start.month
    for _ in range(count):
        out.append(date(y, m, calendar.monthrange(y, m)[1]))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def generate(rows: int, seed: int, months: list[date], drift: float = 0.0, first_id: int = 100000) -> list[dict]:
    """drift > 0 moves the population: shorter contracts, higher charges, more support tickets."""
    rng = random.Random(seed)
    records = []
    for i in range(rows):
        contract = rng.choices([1, 12, 24], weights=[0.5 + 0.3 * drift, 0.3, max(0.2 - 0.15 * drift, 0.02)])[0]
        tenure = min(max(1, int(rng.expovariate(1 / (6 if contract == 1 else 24)))), 72)
        products = rng.choices([1, 2, 3, 4], weights=[0.45, 0.3, 0.17, 0.08])[0]
        monthly = round(rng.uniform(18, 35) + 14 * products + rng.gauss(12 * drift, 6), 2)
        tickets = min(int(rng.expovariate(1 / (1.5 + 2 * drift))), 12)
        paperless = int(rng.random() < 0.6)
        usage = round(max(0.5, rng.gauss(40 + 25 * products, 20)), 1)
        logit = (-1.1 + 1.6 * (contract == 1) - 0.9 * (contract == 24) - 0.045 * tenure + 0.022 * (monthly - 60)
                 + 0.38 * tickets - 0.30 * (products - 1) + 0.35 * paperless - 0.006 * (usage - 80))
        churned = int(rng.random() < 1 / (1 + math.exp(-logit)))
        total = round(monthly * tenure * rng.uniform(0.95, 1.05), 2)
        records.append({
            "customer_id": f"C{first_id + i}",
            "snapshot_date": rng.choice(months).isoformat(),
            "tenure_months": tenure,
            "monthly_charges": monthly,
            "total_charges": "" if rng.random() < 0.02 else total,
            "contract_length_months": contract,
            "num_support_tickets": tickets,
            "num_products": products,
            "has_paperless_billing": paperless,
            "avg_monthly_usage_gb": usage,
            "churned": churned,
        })
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scoring", action="store_true", help="next month's customers, without the label")
    parser.add_argument("--drift", type=float, default=0.0, help="0 = same population, 1 = clearly shifted")
    parser.add_argument("--out", default="data/sample_customers.csv")
    args = parser.parse_args()

    if args.scoring:
        records = generate(args.rows if args.rows != 5000 else 1000, args.seed + 1, month_ends(date(2026, 10, 1), 1),
                           args.drift, first_id=900000)
        for r in records:
            del r["churned"]
    else:
        records = generate(args.rows, args.seed, month_ends(date(2025, 10, 1), 12), args.drift)
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    print(f"wrote {len(records)} rows to {args.out}")


if __name__ == "__main__":
    main()
