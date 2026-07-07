# resources/export_model_for_flink.py
import os
import re
import sys
import json
from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer, StringIndexerModel
from pyspark.ml.classification import RandomForestClassificationModel


APP_NAME = "export_model_for_flink.py"


def build_spark():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .getOrCreate()
    )
    return spark


def load_transformers_and_model():
    bucketizer = Bucketizer.load("s3a://lakehouse/models/arrival_bucketizer_2.0.bin")

    indexers = {}
    for col in ["Carrier", "Origin", "Dest", "Route"]:
        indexers[col] = StringIndexerModel.load(
            f"s3a://lakehouse/models/string_indexer_model_{col}.bin"
        )

    rf_model = RandomForestClassificationModel.load(
        "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    )

    return bucketizer, indexers, rf_model


def get_indexer_labels(indexer_model):
    labels = []
    try:
        labels = list(indexer_model.labels)
    except Exception:
        try:
            labels = list(indexer_model.labelsArray[0])
        except Exception:
            labels = []
    return labels


def count_leading_spaces(s):
    return len(s) - len(s.lstrip(" "))


def parse_tree_block(lines, start_idx):
    """
    Parses Spark tree debug text recursively.
    Expected patterns like:
      If (feature 3 <= 12.5)
      Else (feature 3 <= 12.5)
      Predict: 2.0

    Returns (node, next_index)
    """
    line = lines[start_idx].rstrip()
    stripped = line.strip()

    if stripped.startswith("Predict:"):
        pred = float(stripped.split("Predict:")[1].strip())
        return {"type": "leaf", "prediction": pred}, start_idx + 1

    m = re.match(r"(If|Else) \(feature (\d+) <= ([\-0-9eE\.]+)\)", stripped)
    if not m:
        raise ValueError(f"Cannot parse node line: {line}")

    feature_index = int(m.group(2))
    threshold = float(m.group(3))
    base_indent = count_leading_spaces(line)

    # left child must be next
    left_node, next_idx = parse_tree_block(lines, start_idx + 1)

    # skip blank lines if any
    while next_idx < len(lines) and not lines[next_idx].strip():
        next_idx += 1

    if next_idx >= len(lines):
        raise ValueError("Missing Else branch")

    else_line = lines[next_idx].rstrip()
    else_stripped = else_line.strip()

    m_else = re.match(r"Else \(feature (\d+) <= ([\-0-9eE\.]+)\)", else_stripped)
    if not m_else:
        raise ValueError(f"Expected Else branch, got: {else_line}")

    else_indent = count_leading_spaces(else_line)
    if else_indent != base_indent:
        raise ValueError(f"Else indent mismatch: {else_line}")

    right_node, final_idx = parse_tree_block(lines, next_idx + 1)

    node = {
        "type": "node",
        "featureIndex": feature_index,
        "threshold": threshold,
        "left": left_node,
        "right": right_node
    }
    return node, final_idx


def split_forest_debug_string(debug_str):
    """
    Splits RandomForestClassificationModel.toDebugString into per-tree blocks.
    """
    lines = debug_str.splitlines()

    tree_blocks = []
    current = []
    inside_tree = False

    for line in lines:
        if re.match(r"^\s*Tree \d+ \(weight", line):
            if current:
                tree_blocks.append(current)
                current = []
            inside_tree = True
            continue

        if inside_tree:
            if line.strip():
                current.append(line)

    if current:
        tree_blocks.append(current)

    return tree_blocks


def parse_forest(debug_str):
    blocks = split_forest_debug_string(debug_str)
    forest = []

    for i, block in enumerate(blocks):
        # Spark normally starts each tree block with the root node directly:
        # "  If (feature ...)"
        # remove leading non-If/Predict junk if any
        start_idx = 0
        while start_idx < len(block):
            st = block[start_idx].strip()
            if st.startswith("If ") or st.startswith("Predict:"):
                break
            start_idx += 1

        if start_idx >= len(block):
            raise ValueError(f"Could not find root in tree {i}")

        root, _ = parse_tree_block(block, start_idx)
        forest.append({
            "treeId": i,
            "root": root
        })

    return forest


def main(project_root):
    spark = build_spark()

    try:
        bucketizer, indexers, rf_model = load_transformers_and_model()

        bucketizer_info = {
            "splits": list(bucketizer.getSplits())
        }

        indexers_info = {
            name: get_indexer_labels(model)
            for name, model in indexers.items()
        }

        features_order = [
            "DepDelay",
            "Distance",
            "DayOfMonth",
            "DayOfWeek",
            "DayOfYear",
            "Carrier_index",
            "Origin_index",
            "Dest_index",
            "Route_index"
        ]

        debug_str = rf_model.toDebugString
        forest = parse_forest(debug_str)

        export = {
            "metadata": {
                "modelType": "SparkRandomForestExportForFlink",
                "numTrees": len(forest),
                "numFeatures": len(features_order),
                "sparkVersion": spark.version
            },
            "bucketizer": bucketizer_info,
            "indexers": indexers_info,
            "featuresOrder": features_order,
            "forest": forest
        }

        out_dir = os.path.join(project_root, "data")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "flink_model.json")

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2)

        print(f"Exported model to {out_path}")
        print(f"Trees: {len(forest)}")

    finally:
        spark.stop()


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    main(root)
