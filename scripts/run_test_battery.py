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
DATA_DIR = ROOT_DIR / "data"
RESULTS_DIR = ROOT_DIR / "output" / "results"
LOG_DIR = ROOT_DIR / "output" / "test-runs"
DEDUP_ALL_QUERIES_RECORD_COUNT = 20_000
DEDUP_ALL_QUERIES_Q3_RECORD_COUNT = 11
DEDUP_ALL_QUERIES_Q5_MATCH_COUNT = 7
DEDUP_ALL_QUERIES_Q4_PATTERN_RECORD_COUNT = 10
DEDUP_ALL_QUERIES_DATASET = DATA_DIR / "dedup_all_queries_20k_dataset.csv"
DEDUP_ALL_QUERIES_ACCOUNTS = DATA_DIR / "dedup_all_queries_20k_accounts.csv"


@dataclass(frozen=True)
class ComposeTestCase:
    name: str
    env: dict[str, str]
    expected_eofs: int
    expected_counts_by_type: dict[str, int]
    expected_csv_rows: dict[str, list[list[str]]] = field(default_factory=dict)
    expected_csv_row_counts: dict[str, int] = field(default_factory=dict)
    require_unique_csv_rows: tuple[str, ...] = ()
    allowed_log_patterns: tuple[str, ...] = (
        r"rabbitmq\s+\| .* \[error\].*(supervisor:|errorContext: shutdown_error|reason: shutdown|offender:|id,channel_sup|mfargs,\{rabbit_channel_sup|restart_type,temporary|shutdown,infinity|child_type,supervisor)",
    )
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


@dataclass(frozen=True)
class TestRunResult:
    test_name: str
    failed: bool
    expected_output_path: Path | None = None
    log_path: Path | None = None
    compose_state_path: Path | None = None
    result_dir: Path | None = None
    result_files: tuple[Path, ...] = ()
    failures: list[str] = field(default_factory=list)


TEST_CASES = [
    ComposeTestCase(
        name="q5-smoke-toxic-rabbitmq",
        env={
            "MIDDLEWARE_IMPL": "toxic_rabbitmq",
            "TOXIC_RABBIT_CONFIG_PATH": "/common/middleware/testing/toxic-rabbit-q5-smoke.json",
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


def ensure_dedup_all_queries_20k_fixture():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    DEDUP_ALL_QUERIES_ACCOUNTS.write_text(
        "\n".join(
            [
                "Bank Name,Bank ID,Account Number,Entity ID,Entity Name",
                "Bank Alpha,001,SG_SOURCE,entity-1,Entity One",
                "Bank Beta,002,SG_DEST,entity-2,Entity Two",
                "",
            ]
        ),
        encoding="utf-8",
    )

    rows = []

    middle_accounts = [f"SG_MID_{index}" for index in range(1, 6)]
    for middle_account in middle_accounts:
        rows.append(
            [
                "2022/09/01 00:00",
                "001",
                "SG_SOURCE",
                "002",
                middle_account,
                "100.00",
                "US Dollar",
                "100.00",
                "US Dollar",
                "ACH",
                "0",
            ]
        )
        rows.append(
            [
                "2022/09/01 00:01",
                "001",
                middle_account,
                "002",
                "SG_DEST",
                "100.00",
                "US Dollar",
                "100.00",
                "US Dollar",
                "ACH",
                "0",
            ]
        )

    for index in range(1, DEDUP_ALL_QUERIES_Q5_MATCH_COUNT + 1):
        rows.append(
            [
                "2022/09/02 00:00",
                "001",
                f"Q5_LOW_{index:02d}O",
                "002",
                f"Q5_LOW_{index:02d}D",
                "100.00",
                "US Dollar",
                "0.50",
                "US Dollar",
                "ACH" if index % 2 else "Wire",
                "0",
            ]
        )

    for index in range(1, DEDUP_ALL_QUERIES_Q3_RECORD_COUNT + 1):
        rows.append(
            [
                "2022/09/06 00:00",
                "001",
                f"Q3_LOW_{index:02d}O",
                "002",
                f"Q3_LOW_{index:02d}D",
                "0.50",
                "US Dollar",
                "0.50",
                "US Dollar",
                "ACH",
                "0",
            ]
        )

    filler_count = DEDUP_ALL_QUERIES_RECORD_COUNT - len(rows)
    for index in range(1, filler_count + 1):
        rows.append(
            [
                f"2022/10/{(index % 28) + 1:02d} 00:00",
                "001",
                f"DEDUP{index:05d}O",
                "002",
                f"DEDUP{index:05d}D",
                "1.00",
                "US Dollar",
                "1.00",
                "US Dollar",
                "ACH",
                "0",
            ]
        )

    with DEDUP_ALL_QUERIES_DATASET.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "Timestamp",
                "From Bank",
                "Account",
                "To Bank",
                "Account",
                "Amount Received",
                "Receiving Currency",
                "Amount Paid",
                "Payment Currency",
                "Payment Format",
                "Is Laundering",
            ]
        )
        writer.writerows(rows)


TEST_CASES.append(
    ComposeTestCase(
        name="all-queries-dedup-20k-toxic-rabbitmq",
        env={
            "MIDDLEWARE_IMPL": "toxic_rabbitmq",
            "TOXIC_RABBIT_CONFIG_PATH": "/common/middleware/testing/toxic-rabbit-all-data-messages.json",
            "DATA_PATH": "/data/dedup_all_queries_20k_dataset.csv",
            "DATA_PATH_ACCOUNTS": "/data/dedup_all_queries_20k_accounts.csv",
            "CONVERSION_PROVIDER": "static",
            "EXPECTED_RESULT_EOFS": "5",
            "RESULTS_IDLE_TIMEOUT": "180",
            "RESULTS_WAIT_LOG_INTERVAL": "30",
        },
        expected_eofs=5,
        expected_counts_by_type={
            "QUERY_1_RESULT": (
                DEDUP_ALL_QUERIES_RECORD_COUNT
                - DEDUP_ALL_QUERIES_Q4_PATTERN_RECORD_COUNT
                - DEDUP_ALL_QUERIES_Q5_MATCH_COUNT
            ),
            "QUERY_2_RESULT": 1,
            "QUERY_3_RESULT": DEDUP_ALL_QUERIES_Q3_RECORD_COUNT,
            "QUERY_4_RESULT": 1,
            "QUERY_5_RESULT": 1,
        },
        expected_csv_row_counts={
            "query_1.csv": (
                DEDUP_ALL_QUERIES_RECORD_COUNT
                - DEDUP_ALL_QUERIES_Q4_PATTERN_RECORD_COUNT
                - DEDUP_ALL_QUERIES_Q5_MATCH_COUNT
            ),
            "query_2.csv": 1,
            "query_3.csv": DEDUP_ALL_QUERIES_Q3_RECORD_COUNT,
            "query_4.csv": 2,
        },
        expected_csv_rows={
            "query_5.csv": [
                [str(DEDUP_ALL_QUERIES_Q5_MATCH_COUNT)],
            ],
        },
        require_unique_csv_rows=(
            "query_1.csv",
            "query_2.csv",
            "query_3.csv",
            "query_4.csv",
        ),
    ),
)


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


def artifact_dir(test_name):
    return LOG_DIR / test_name


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


def list_result_files(result_dir):
    if result_dir is None or not result_dir.exists():
        return ()
    return tuple(sorted(path for path in result_dir.iterdir() if path.is_file()))


def write_artifact(test_name, file_name, content):
    test_artifact_dir = artifact_dir(test_name)
    test_artifact_dir.mkdir(parents=True, exist_ok=True)
    path = test_artifact_dir / file_name
    path.write_text(content, encoding="utf-8")
    return path


def capture_command_output(args, *, env=None):
    completed = run_command(args, env=env, capture=True, check=False)
    return completed.returncode, completed.stdout or ""


def collect_logs(test_name):
    _, output = capture_command_output(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "logs", "--no-color"],
    )
    return write_artifact(test_name, "compose.log", output)


def collect_compose_state(test_name):
    exit_code, output = capture_command_output(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "ps", "-a"]
    )
    header = f"exit_code={exit_code}\ncommand=docker compose -f {COMPOSE_FILE} ps -a\n\n"
    return write_artifact(test_name, "compose_ps.txt", header + output)


def collect_result_dir_snapshot(test_name, before_results):
    after_results = sorted(list_result_dirs(), key=lambda path: path.stat().st_mtime)
    created_results = sorted(
        set(after_results) - before_results,
        key=lambda path: path.stat().st_mtime,
    )
    lines = [
        "Created result directories since test start:",
        *([str(path) for path in created_results] or ["<none>"]),
        "",
        "All result directories after test:",
        *([str(path) for path in after_results] or ["<none>"]),
    ]
    return write_artifact(test_name, "result_dirs.txt", "\n".join(lines) + "\n")


def expected_output_path(test_name):
    return artifact_dir(test_name) / "expected_output.json"


def write_expected_output(test_case):
    output_path = expected_output_path(test_case.name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    expected_output = {
        "test_name": test_case.name,
        "summary": {
            "eof_count": test_case.expected_eofs,
            "result_counts_by_type": test_case.expected_counts_by_type,
        },
        "csv": {
            "expected_rows": test_case.expected_csv_rows,
            "expected_row_counts": test_case.expected_csv_row_counts,
            "require_unique_rows": list(test_case.require_unique_csv_rows),
        },
    }
    output_path.write_text(
        json.dumps(expected_output, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


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

    for file_name, expected_count in test_case.expected_csv_row_counts.items():
        result_path = result_dir / file_name
        if not result_path.exists():
            failures.append(f"Missing result file: {result_path}")
            continue

        actual_rows = read_csv_body(result_path)
        if len(actual_rows) != expected_count:
            failures.append(
                f"{file_name} row count mismatch. Expected {expected_count}, got {len(actual_rows)}"
            )

    for file_name in test_case.require_unique_csv_rows:
        result_path = result_dir / file_name
        if not result_path.exists():
            failures.append(f"Missing result file: {result_path}")
            continue

        actual_rows = read_csv_body(result_path)
        unique_rows = {tuple(row) for row in actual_rows}
        if len(unique_rows) != len(actual_rows):
            failures.append(
                f"{file_name} contains duplicate rows. rows={len(actual_rows)}, unique_rows={len(unique_rows)}"
            )

    return failures


def validate_logs(test_case, log_path):
    failures = []
    logs = log_path.read_text(encoding="utf-8", errors="replace")
    for pattern in test_case.forbidden_log_patterns:
        for match in re.finditer(pattern, logs, flags=re.IGNORECASE):
            line = logs.count("\n", 0, match.start()) + 1
            line_start = logs.rfind("\n", 0, match.start()) + 1
            line_end = logs.find("\n", match.end())
            if line_end == -1:
                line_end = len(logs)
            log_line = logs[line_start:line_end]
            if any(
                re.search(allowed_pattern, log_line, flags=re.IGNORECASE)
                for allowed_pattern in test_case.allowed_log_patterns
            ):
                continue
            failures.append(f"Forbidden log pattern {pattern!r} found at {log_path}:{line}")
            break
    return failures


def run_test_case(test_case, keep_stack, expected_output):
    print(f"\n== {test_case.name} ==")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = None
    compose_state_path = None
    result_dir = None
    result_files = ()
    print(f"Expected: {expected_output}")

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
        compose_state_path = collect_compose_state(test_case.name)
        collect_result_dir_snapshot(test_case.name, before_results)
        result_dir = find_new_result_dir(before_results)
        result_files = list_result_files(result_dir)

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
            print(f"Expected: {expected_output}")
            print(f"Logs: {log_path}")
            print(f"Compose state: {compose_state_path}")
            if result_dir is not None:
                print(f"Results: {result_dir}")
                print(
                    "Result files: "
                    + (", ".join(str(path) for path in result_files) if result_files else "<none>")
                )
            for failure in failures:
                print(f"  - {failure}")
            return TestRunResult(
                test_name=test_case.name,
                failed=True,
                expected_output_path=expected_output,
                log_path=log_path,
                compose_state_path=compose_state_path,
                result_dir=result_dir,
                result_files=result_files,
                failures=failures,
            )

        print("OK")
        print(f"Expected: {expected_output}")
        print(f"Logs: {log_path}")
        print(f"Compose state: {compose_state_path}")
        print(f"Results: {result_dir}")
        print(
            "Result files: "
            + (", ".join(str(path) for path in result_files) if result_files else "<none>")
        )
        return TestRunResult(
            test_name=test_case.name,
            failed=False,
            expected_output_path=expected_output,
            log_path=log_path,
            compose_state_path=compose_state_path,
            result_dir=result_dir,
            result_files=result_files,
        )
    except Exception as error:
        log_path = log_path or collect_logs(test_case.name)
        compose_state_path = compose_state_path or collect_compose_state(test_case.name)
        collect_result_dir_snapshot(test_case.name, before_results)
        result_dir = result_dir or find_new_result_dir(before_results)
        result_files = list_result_files(result_dir)
        failures = [str(error)]

        print("FAIL")
        print(f"Expected: {expected_output}")
        print(f"Logs: {log_path}")
        print(f"Compose state: {compose_state_path}")
        if result_dir is not None:
            print(f"Results: {result_dir}")
            print(
                "Result files: "
                + (", ".join(str(path) for path in result_files) if result_files else "<none>")
            )
        for failure in failures:
            print(f"  - {failure}")
        return TestRunResult(
            test_name=test_case.name,
            failed=True,
            expected_output_path=expected_output,
            log_path=log_path,
            compose_state_path=compose_state_path,
            result_dir=result_dir,
            result_files=result_files,
            failures=failures,
        )
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

    if any(test_case.name == "all-queries-dedup-20k-toxic-rabbitmq" for test_case in selected):
        ensure_dedup_all_queries_20k_fixture()

    results = []
    for test_case in selected:
        expected_output = write_expected_output(test_case)
        try:
            results.append(
                run_test_case(
                    test_case,
                    keep_stack=args.keep_stack,
                    expected_output=expected_output,
                )
            )
        except Exception as error:
            print("FAIL")
            print(f"  - {error}")
            results.append(
                TestRunResult(
                    test_name=test_case.name,
                    failed=True,
                    expected_output_path=expected_output,
                    failures=[str(error)],
                )
            )

    failed_results = [result for result in results if result.failed]
    failures = len(failed_results)
    print(f"\nRan {len(selected)} test case(s), failures={failures}")

    if failed_results:
        print("\nFailed test artifacts:")
        for result in failed_results:
            print(f"- {result.test_name}")
            print(
                "  Expected: "
                f"{result.expected_output_path if result.expected_output_path is not None else 'not created'}"
            )
            print(f"  Logs: {result.log_path if result.log_path is not None else 'not collected'}")
            print(
                "  Compose state: "
                f"{result.compose_state_path if result.compose_state_path is not None else 'not collected'}"
            )
            print(
                "  Results: "
                f"{result.result_dir if result.result_dir is not None else 'not created'}"
            )
            print(
                "  Result files: "
                + (
                    ", ".join(str(path) for path in result.result_files)
                    if result.result_files
                    else "not collected"
                )
            )

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
