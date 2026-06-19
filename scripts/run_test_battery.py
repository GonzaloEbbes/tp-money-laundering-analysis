#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT_DIR / "docker-compose.yaml"
RESULTS_DIR = ROOT_DIR / "output" / "results"
LOG_DIR = ROOT_DIR / "output" / "test-runs"


@dataclass(frozen=True)
class ComposeTestCase:
    name: str
    env: dict[str, str]
    expected_eofs: int
    expected_counts_by_type: dict[str, int]
    expected_csv_rows: dict[str, list[list[str]]] = field(default_factory=dict)
    forbidden_log_patterns: tuple[str, ...] = (
        r"\bERROR\b",
        r"\bException\b",
        r"\bTraceback\b",
        r"\bfailed\b",
        r"\bfatal\b",
        r"\bpanic\b",
        r"\btimeout\b",
        r"exited with code [1-9]",
    )


TEST_CASES = [
    ComposeTestCase(
        name="q5-smoke-toxic-rabbitmq",
        env={
            "MIDDLEWARE_IMPL": "toxic_rabbitmq",
            "DATA_PATH": "/data/q5_smoke_dataset.csv",
            "DATA_PATH_ACCOUNTS": "/data/q5_smoke_accounts.csv",
            "CONVERSION_PROVIDER": "static",
            "EXPECTED_RESULT_EOFS": "5",
            "RESULTS_IDLE_TIMEOUT": "120",
        },
        expected_eofs=5,
        expected_counts_by_type={
            "QUERY_1_RESULT": 3,
            "QUERY_5_RESULT": 1,
        },
        expected_csv_rows={
            "query_1.csv": [
                ["800Q50001", "800Q50002", "0.5"],
                ["800Q50003", "800Q50004", "1.25"],
                ["800Q50005", "800Q50006", "0.1"],
            ],
            "query_5.csv": [
                ["2"],
            ],
        },
    ),
]


def run_command(args, *, env=None, capture=False, check=False):
    completed = subprocess.run(
        args,
        cwd=ROOT_DIR,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        check=False,
    )
    if check and completed.returncode != 0:
        output = completed.stdout or ""
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}: {' '.join(args)}\n{output}"
        )
    return completed


def list_result_dirs():
    if not RESULTS_DIR.exists():
        return set()
    return {path for path in RESULTS_DIR.iterdir() if path.is_dir()}


def read_csv_body(path):
    with path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.reader(file))
    return rows[1:] if rows else []


def find_new_result_dir(before):
    after = list_result_dirs()
    created = sorted(after - before, key=lambda path: path.stat().st_mtime)
    if created:
        return created[-1]

    client_dirs = sorted(
        (path for path in after if path.name.startswith("client-")),
        key=lambda path: path.stat().st_mtime,
    )
    return client_dirs[-1] if client_dirs else None


def collect_logs(test_name):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{test_name}.log"
    completed = run_command(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "logs", "--no-color"],
        capture=True,
    )
    log_path.write_text(completed.stdout or "", encoding="utf-8")
    return log_path


def validate_summary(test_case, result_dir):
    failures = []
    summary_path = result_dir / "summary.json"
    if not summary_path.exists():
        return [f"Missing summary.json in {result_dir}"]

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    eof_count = summary.get("eof_count")
    if eof_count != test_case.expected_eofs:
        failures.append(f"Expected eof_count={test_case.expected_eofs}, got {eof_count}")

    counts = summary.get("result_counts_by_type", {})
    if counts != test_case.expected_counts_by_type:
        failures.append(
            "Expected result_counts_by_type="
            f"{test_case.expected_counts_by_type}, got {counts}"
        )

    return failures


def validate_csv_outputs(test_case, result_dir):
    failures = []
    for file_name, expected_rows in test_case.expected_csv_rows.items():
        result_path = result_dir / file_name
        if not result_path.exists():
            failures.append(f"Missing result file: {result_path}")
            continue

        actual_rows = sorted(read_csv_body(result_path))
        expected_sorted = sorted(expected_rows)
        if actual_rows != expected_sorted:
            failures.append(
                f"{file_name} mismatch. Expected rows={expected_sorted}, got rows={actual_rows}"
            )

    return failures


def validate_logs(test_case, log_path):
    failures = []
    logs = log_path.read_text(encoding="utf-8", errors="replace")
    for pattern in test_case.forbidden_log_patterns:
        match = re.search(pattern, logs, flags=re.IGNORECASE)
        if match:
            line = logs.count("\n", 0, match.start()) + 1
            failures.append(f"Forbidden log pattern {pattern!r} found at {log_path}:{line}")
    return failures


def run_test_case(test_case, keep_stack):
    print(f"\n== {test_case.name} ==")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Stopping previous compose stack")
    run_command(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "--volumes", "--remove-orphans"],
        check=True,
    )

    before_results = list_result_dirs()
    env = os.environ.copy()
    env.update(test_case.env)

    try:
        print("Starting compose stack")
        run_command(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "up",
                "--build",
                "--detach",
                "--remove-orphans",
            ],
            env=env,
            check=True,
        )

        print("Waiting for client container")
        client_exit = run_command(["docker", "wait", "client"], capture=True, check=True)
        client_exit_code = (client_exit.stdout or "").strip()

        log_path = collect_logs(test_case.name)
        result_dir = find_new_result_dir(before_results)

        failures = []
        if client_exit_code != "0":
            failures.append(f"Client exited with code {client_exit_code}")

        if result_dir is None:
            failures.append("Client did not create a result directory")
        else:
            failures.extend(validate_summary(test_case, result_dir))
            failures.extend(validate_csv_outputs(test_case, result_dir))

        failures.extend(validate_logs(test_case, log_path))

        if failures:
            print("FAIL")
            print(f"Logs: {log_path}")
            if result_dir is not None:
                print(f"Results: {result_dir}")
            for failure in failures:
                print(f"  - {failure}")
            return 1

        print("OK")
        print(f"Logs: {log_path}")
        print(f"Results: {result_dir}")
        return 0
    finally:
        if keep_stack:
            print("Keeping compose stack running")
        else:
            print("Stopping compose stack")
            run_command(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(COMPOSE_FILE),
                    "down",
                    "--volumes",
                    "--remove-orphans",
                ],
            )


def main():
    parser = argparse.ArgumentParser(description="Run reusable end-to-end test battery.")
    parser.add_argument(
        "--case",
        choices=[test_case.name for test_case in TEST_CASES],
        help="Run only one test case.",
    )
    parser.add_argument(
        "--keep-stack",
        action="store_true",
        help="Leave compose services running after the test.",
    )
    args = parser.parse_args()

    selected = TEST_CASES
    if args.case:
        selected = [test_case for test_case in TEST_CASES if test_case.name == args.case]

    failures = 0
    for test_case in selected:
        failures += run_test_case(test_case, keep_stack=args.keep_stack)

    print(f"\nRan {len(selected)} test case(s), failures={failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
