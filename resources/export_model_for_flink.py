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
    try:
        return list(indexer_model.labels)
    except Exception:
        try:
            return list(indexer_model.labelsArray[0])
        except Exception:
            return []


def split_forest_debug_string(debug_str):
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

        if inside_tree and line.strip():
            current.append(line.rstrip("\n"))

    if current:
        tree_blocks.append(current)

    return tree_blocks


def indent_of(line):
    return len(line) - len(line.lstrip(" "))


def parse_if_condition(stripped):
    m_num = re.match(r"If \(feature (\d+) <= ([\-0-9eE\.]+)\)", stripped)
    if m_num:
        return {
            "splitType": "continuous",
            "featureIndex": int(m_num.group(1)),
            "threshold": float(m_num.group(2))
        }

    m_cat = re.match(r"If \(feature (\d+) in \{(.*)\}\)", stripped)
    if m_cat:
        values_raw = m_cat.group(2).strip()
        values = [] if values_raw == "" else [float(x.strip()) for x in values_raw.split(",") if x.strip()]
        return {
            "splitType": "categorical",
            "featureIndex": int(m_cat.group(1)),
            "categories": values
        }

    return None


def parse_else_condition(stripped):
    m_num = re.match(r"Else \(feature (\d+) > ([\-0-9eE\.]+)\)", stripped)
    if m_num:
        return {
            "splitType": "continuous",
            "featureIndex": int(m_num.group(1)),
            "threshold": float(m_num.group(2))
        }

    m_num_alt = re.match(r"Else \(feature (\d+) <= ([\-0-9eE\.]+)\)", stripped)
    if m_num_alt:
        return {
            "splitType": "continuous",
            "featureIndex": int(m_num_alt.group(1)),
            "threshold": float(m_num_alt.group(2))
        }

    m_cat = re.match(r"Else \(feature (\d+) in \{(.*)\}\)", stripped)
    if m_cat:
        values_raw = m_cat.group(2).strip()
        values = [] if values_raw == "" else [float(x.strip()) for x in values_raw.split(",") if x.strip()]
        return {
            "splitType": "categorical",
            "featureIndex": int(m_cat.group(1)),
            "categories": values
        }

    m_cat_not = re.match(r"Else \(feature (\d+) not in \{(.*)\}\)", stripped)
    if m_cat_not:
        values_raw = m_cat_not.group(2).strip()
        values = [] if values_raw == "" else [float(x.strip()) for x in values_raw.split(",") if x.strip()]
        return {
            "splitType": "categorical",
            "featureIndex": int(m_cat_not.group(1)),
            "categories": values
        }

    return None


def parse_tree_block(lines, start_idx):
    line = lines[start_idx].rstrip()
    stripped = line.strip()

    if stripped.startswith("Predict:"):
        pred = float(stripped.split("Predict:")[1].strip())
        return {"type": "leaf", "prediction": pred}, start_idx + 1

    cond = parse_if_condition(stripped)
    if cond is None:
        raise ValueError(f"Cannot parse node line: {line}")

    base_indent = indent_of(line)

    left_node, next_idx = parse_tree_block(lines, start_idx + 1)

    while next_idx < len(lines) and not lines[next_idx].strip():
        next_idx += 1

    if next_idx >= len(lines):
        raise ValueError("Missing Else branch")

    else_line = lines[next_idx].rstrip()
    else_stripped = else_line.strip()

    else_cond = parse_else_condition(else_stripped)
    if else_cond is None:
        raise ValueError(f"Expected Else branch, got: {else_line}")

    else_indent = indent_of(else_line)
    if else_indent != base_indent:
        raise ValueError(f"Else indent mismatch: {else_line}")

    if cond["featureIndex"] != else_cond["featureIndex"]:
        raise ValueError(f"If/Else feature mismatch: {line} / {else_line}")

    if cond["splitType"] != else_cond["splitType"]:
        raise ValueError(f"If/Else split type mismatch: {line} / {else_line}")

    right_node, final_idx = parse_tree_block(lines, next_idx + 1)

    node = {
        "type": "node",
        "splitType": cond["splitType"],
        "featureIndex": cond["featureIndex"],
        "left": left_node,
        "right": right_node
    }

    if cond["splitType"] == "continuous":
        node["threshold"] = cond["threshold"]
    else:
        node["categories"] = cond["categories"]

    return node, final_idx


def parse_forest(debug_str):
    blocks = split_forest_debug_string(debug_str)
    forest = []

    for i, block in enumerate(blocks):
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

        export = {
            "metadata": {
                "modelType": "SparkRandomForestExportForFlink",
                "numTrees": len(rf_model.trees),
                "numFeatures": 9,
                "sparkVersion": spark.version
            },
            "bucketizer": {
                "splits": list(bucketizer.getSplits())
            },
            "indexers": {
                name: get_indexer_labels(model)
                for name, model in indexers.items()
            },
            "featuresOrder": [
                "DepDelay",
                "Distance",
                "DayOfMonth",
                "DayOfWeek",
                "DayOfYear",
                "Carrier_index",
                "Origin_index",
                "Dest_index",
                "Route_index"
            ],
            "forest": parse_forest(rf_model.toDebugString)
        }

        out_dir = os.path.join(project_root, "data")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "flink_model.json")

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2)

        print(f"Exported model to {out_path}")
        print(f"Trees: {len(export['forest'])}")

    finally:
        spark.stop()


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    main(root)