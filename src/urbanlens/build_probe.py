"""On-demand dbt/backup feasibility entry point for a normal Linux build container."""

import importlib
import json
import os
from typing import Any

from urbanlens.lambda_handlers import cloud_database


def main() -> None:
    metrics = cloud_database()
    s3: Any = importlib.import_module("boto3").client("s3")
    s3.put_object(
        Bucket=os.environ["URBANLENS_PILOT_BUCKET"],
        Key="results/codebuild-dbt.json",
        Body=json.dumps(metrics).encode(),
        ContentType="application/json",
    )
    print(json.dumps(metrics))


if __name__ == "__main__":
    main()
