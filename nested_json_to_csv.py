#!/usr/bin/env python3
"""Convert nested JSON into flattened table data, CSV, or Excel."""

from __future__ import annotations

import argparse
import csv
import json
from io import BytesIO, StringIO
from pathlib import Path
import re
from typing import Any

from openpyxl import Workbook


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert nested JSON into flattened CSV rows."
    )
    parser.add_argument("input", help="Path to input JSON file")
    parser.add_argument("output", help="Path to output CSV file")
    parser.add_argument(
        "--records-path",
        default="",
        help=(
            "Dot-separated path to the list/object to export, "
            "e.g. 'data.items' or 'results.0.records'"
        ),
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter character (default: ',')",
    )
    return parser.parse_args()


def is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def to_cell_value(value: Any) -> Any:
    if is_scalar(value):
        return "" if value is None else value
    return json.dumps(value, ensure_ascii=False)


def flatten_json(value: Any, parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    items: dict[str, Any] = {}

    if isinstance(value, dict):
        if not value and parent_key:
            items[parent_key] = ""
        for key, child in value.items():
            key_str = str(key)
            new_key = f"{parent_key}{sep}{key_str}" if parent_key else key_str
            items.update(flatten_json(child, new_key, sep))
        return items

    if isinstance(value, list):
        if not value:
            items[parent_key] = ""
            return items
        for idx, child in enumerate(value):
            list_key = f"{parent_key}[{idx}]" if parent_key else f"[{idx}]"
            items.update(flatten_json(child, list_key, sep))
        return items

    items[parent_key or "value"] = value if value is not None else ""
    return items


def resolve_path(data: Any, path: str) -> Any:
    if not path:
        return data

    node = data
    for token in path.split("."):
        if isinstance(node, list):
            try:
                index = int(token)
            except ValueError as exc:
                raise ValueError(
                    f"Expected numeric index for list segment '{token}' in path '{path}'"
                ) from exc
            try:
                node = node[index]
            except IndexError as exc:
                raise ValueError(f"Index '{index}' out of range in path '{path}'") from exc
        elif isinstance(node, dict):
            if token not in node:
                raise ValueError(f"Key '{token}' not found in path '{path}'")
            node = node[token]
        else:
            raise ValueError(f"Cannot traverse segment '{token}' in path '{path}'")
    return node


def iter_candidate_paths(data: Any, prefix: str = "") -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = [(prefix, data)]

    if isinstance(data, dict):
        for key, value in data.items():
            key_str = str(key)
            child_prefix = f"{prefix}.{key_str}" if prefix else key_str
            candidates.extend(iter_candidate_paths(value, child_prefix))
    elif isinstance(data, list):
        for index, value in enumerate(data):
            child_prefix = f"{prefix}.{index}" if prefix else str(index)
            candidates.extend(iter_candidate_paths(value, child_prefix))

    return candidates


def tokenize_text(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", value.lower()) if token}


def path_to_search_text(path: str) -> str:
    return path.replace(".", " ").replace("_", " ").replace("-", " ")


def score_candidate_path(path: str, value: Any, request: str) -> float:
    request_tokens = tokenize_text(request)
    path_tokens = tokenize_text(path_to_search_text(path))

    if not request_tokens:
        return 0.0

    score = float(len(request_tokens & path_tokens) * 4)

    if isinstance(value, list):
        score += 2
        if value and all(isinstance(item, dict) for item in value[:5]):
            score += 3
        elif value and all(is_scalar(item) for item in value[:5]):
            score += 1
    elif isinstance(value, dict):
        score += 1

    if path and any(path.lower().endswith(token) for token in request_tokens):
        score += 2

    return score


def suggest_records_path(data: Any, request: str) -> str:
    scored: list[tuple[float, str]] = []
    for path, value in iter_candidate_paths(data):
        if not path:
            continue
        score = score_candidate_path(path, value, request)
        if score > 0:
            scored.append((score, path))

    if not scored:
        return ""

    scored.sort(key=lambda item: (-item[0], len(item[1])))
    return scored[0][1]


def normalize_rows(target: Any) -> list[dict[str, Any]]:
    if isinstance(target, list):
        return [flatten_json(item) for item in target]
    return [flatten_json(target)]


def collect_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    seen: dict[str, None] = {}
    for row in rows:
        for key in row:
            if key not in seen:
                seen[key] = None
    return list(seen.keys())


def json_to_rows(data: Any, records_path: str = "") -> tuple[list[dict[str, Any]], list[str]]:
    target = resolve_path(data, records_path)
    rows = normalize_rows(target)
    fieldnames = collect_fieldnames(rows)
    return rows, fieldnames


def load_json_file(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8-sig") as src:
        return json.load(src)


def rows_to_csv_text(
    rows: list[dict[str, Any]], fieldnames: list[str], delimiter: str = ","
) -> str:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: to_cell_value(row.get(key, "")) for key in fieldnames})
    return output.getvalue()


def rows_to_excel_bytes(rows: list[dict[str, Any]], fieldnames: list[str]) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "FlattenedData"
    sheet.append(fieldnames)
    for row in rows:
        sheet.append([to_cell_value(row.get(key, "")) for key in fieldnames])

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def convert_json_file_to_csv(
    input_path: str | Path,
    output_path: str | Path,
    records_path: str = "",
    delimiter: str = ",",
) -> int:
    data = load_json_file(input_path)
    rows, fieldnames = json_to_rows(data, records_path)
    csv_text = rows_to_csv_text(rows, fieldnames, delimiter=delimiter)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as out:
        out.write(csv_text)
    return len(rows)


def main() -> None:
    args = parse_args()
    row_count = convert_json_file_to_csv(
        args.input,
        args.output,
        records_path=args.records_path,
        delimiter=args.delimiter,
    )
    print(f"Wrote {row_count} row(s) to '{args.output}'")


if __name__ == "__main__":
    main()
