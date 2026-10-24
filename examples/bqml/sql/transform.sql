-- The TRANSFORM clause's expressions: BigQuery ML learns their statistics from the training rows and applies them in
-- ML.PREDICT and at a Model Registry endpoint, so callers send raw values. Name every input the model uses, and the
-- label. Functions outside BigQuery ML's export list keep the model out of exports (see the BigQuery ML docs).
ML.STANDARD_SCALER(tenure_months) OVER () AS tenure_months,
ML.STANDARD_SCALER(monthly_charges) OVER () AS monthly_charges,
ML.STANDARD_SCALER(charges_per_month) OVER () AS charges_per_month,
contract_length_months,
num_products,
has_paperless_billing,
num_support_tickets,
avg_monthly_usage_gb,
churned
