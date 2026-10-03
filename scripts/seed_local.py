"""Seeds a local SQS emulator with realistic dead-lettered messages.

Usage: python scripts/seed_local.py
Needs: pip install boto3, and an SQS emulator such as moto_server -p 5055.
"""
import json
import os

import boto3

ENDPOINT = os.environ.get("DLQ_TRIAGE_SQS_ENDPOINT", "http://127.0.0.1:5055")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

sqs = boto3.client("sqs", region_name="us-east-1", endpoint_url=ENDPOINT)
dlq = sqs.create_queue(QueueName="loan-onboarding-dlq")["QueueUrl"]
sqs.create_queue(QueueName="loan-onboarding")
for url in (dlq, sqs.get_queue_url(QueueName="loan-onboarding")["QueueUrl"]):
    sqs.purge_queue(QueueUrl=url)


def fail(body, error, **extra):
    attrs = {"ErrorMessage": {"DataType": "String", "StringValue": error}}
    for name, (kind, value) in extra.items():
        attrs[name] = {"DataType": kind, "StringValue": value}
    sqs.send_message(QueueUrl=dlq, MessageBody=json.dumps(body), MessageAttributes=attrs)


for i in range(1, 15):
    fail(
        {"loanId": f"LN-{1000 + i}", "borrower": {"id": f"B{i}", "name": f"Borrower {i}"}, "amount": 150000 + i * 1000},
        f"Loan LN-{1000 + i} not found in servicing system (request 3f2b8c1e-aaaa-bbbb-cccc-{i:012d})",
        Amount=("Number", str(150000 + i * 1000)),
    )
for i in range(1, 8):
    fail(
        {"loanId": f"LN-{2000 + i}", "documents": [{"type": "W2", "pages": 2}]},
        f"Timeout after {30 * i} seconds calling document-verification",
    )
for i in range(1, 4):
    fail({"loanId": f"LN-{3000 + i}", "amount": "not-a-number"}, "Schema validation failed: amount must be a number")

print("seeded 24 messages into", dlq)
