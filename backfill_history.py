import argparse
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from history_gen import (
    HISTORY_ROOT,
    create_history_document,
    history_path_for_department,
    normalize_timestamp,
    update_department_history,
    write_history_document,
)


ZERO_BLOB = "0" * 40
IGNORED_DATA_FILENAMES = {"metadata.json", "latest_sem.json", "departments.json"}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill published enrollment history files from git snapshots."
    )
    parser.add_argument(
        "strms",
        nargs="*",
        help="Optional list of semester STRMs to backfill. Defaults to every semester under data/.",
    )
    return parser.parse_args()


def discover_strms(cli_strms):
    if cli_strms:
        return {str(strm) for strm in cli_strms}

    return {
        path.name
        for path in Path("data").iterdir()
        if path.is_dir() and path.name.isdigit()
    }


def commit_timestamp_iso(commit_timestamp):
    dt = datetime.fromtimestamp(commit_timestamp, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def parse_metadata_path(path_text):
    path = Path(path_text)
    if len(path.parts) != 3 or path.parts[0] != "data" or path.name != "metadata.json":
        return None
    return path.parts[1]


def parse_department_path(path_text):
    path = Path(path_text)
    if len(path.parts) != 3 or path.parts[0] != "data":
        return None
    if path.name in IGNORED_DATA_FILENAMES or path.suffix != ".json":
        return None
    return path.parts[1], path.stem


class GitBlobReader:
    def __init__(self):
        self.process = None

    def __enter__(self):
        self.process = subprocess.Popen(
            ["git", "cat-file", "--batch"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.process and self.process.stdin:
            self.process.stdin.close()
        if self.process:
            self.process.terminate()
            self.process.wait()

    def read_blob(self, blob_id):
        if blob_id == ZERO_BLOB:
            return None

        assert self.process is not None and self.process.stdin is not None and self.process.stdout is not None
        self.process.stdin.write(f"{blob_id}\n".encode("utf-8"))
        self.process.stdin.flush()

        header = self.process.stdout.readline().decode("utf-8").strip()
        if not header.endswith("blob") and " blob " not in header:
            if header.endswith("missing"):
                return None
            raise RuntimeError(f"Unexpected git cat-file header: {header}")

        _, object_type, object_size = header.split()
        if object_type != "blob":
            raise RuntimeError(f"Expected blob object, received {object_type}")

        object_size = int(object_size)
        blob_bytes = self.process.stdout.read(object_size)
        self.process.stdout.read(1)
        return blob_bytes.decode("utf-8")


def iter_commit_changes(target_strms):
    data_paths = ["data"] if not target_strms else [f"data/{strm}" for strm in sorted(target_strms)]
    process = subprocess.Popen(
        ["git", "log", "--reverse", "--raw", "--format=__COMMIT__ %H %ct", "--no-abbrev", "--", *data_paths],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    current_commit = None
    current_timestamp = None
    current_changes = []

    assert process.stdout is not None
    for raw_line in process.stdout:
        line = raw_line.rstrip("\n")
        if line.startswith("__COMMIT__ "):
            if current_commit is not None:
                yield current_commit, current_timestamp, current_changes

            _, commit_hash, commit_timestamp = line.split()
            current_commit = commit_hash
            current_timestamp = int(commit_timestamp)
            current_changes = []
            continue

        if not line.startswith(":"):
            continue

        metadata, path_text = line.split("\t", 1)
        _, _, _, new_blob, status = metadata.split()
        current_changes.append((path_text, new_blob, status))

    if current_commit is not None:
        yield current_commit, current_timestamp, current_changes

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(process.stderr.read())


def clean_target_directories(target_strms):
    for strm in target_strms:
        target_dir = HISTORY_ROOT / strm
        if target_dir.exists():
            shutil.rmtree(target_dir)


def backfill_history(target_strms):
    histories = {}
    processed_commits = 0
    processed_departments = 0

    clean_target_directories(target_strms)

    with GitBlobReader() as blob_reader:
        for _, commit_timestamp, changes in iter_commit_changes(target_strms):
            processed_commits += 1
            metadata_timestamps = {}

            for path_text, new_blob, _ in changes:
                strm = parse_metadata_path(path_text)
                if not strm or strm not in target_strms or new_blob == ZERO_BLOB:
                    continue

                metadata_text = blob_reader.read_blob(new_blob)
                if metadata_text is None:
                    continue

                try:
                    metadata = json.loads(metadata_text)
                except json.JSONDecodeError:
                    continue
                metadata_timestamps[strm] = normalize_timestamp(
                    metadata.get("last_updated", commit_timestamp_iso(commit_timestamp))
                )

            for path_text, new_blob, _ in changes:
                department_info = parse_department_path(path_text)
                if department_info is None or new_blob == ZERO_BLOB:
                    continue

                strm, department = department_info
                if strm not in target_strms:
                    continue

                department_text = blob_reader.read_blob(new_blob)
                if department_text is None:
                    continue

                try:
                    department_snapshot = json.loads(department_text)
                except json.JSONDecodeError:
                    print(f"Skipping malformed historical snapshot for {path_text}.")
                    continue
                if not isinstance(department_snapshot, dict):
                    continue

                history_key = (strm, department)
                history_document = histories.setdefault(
                    history_key,
                    create_history_document(strm, department),
                )
                snapshot_timestamp = metadata_timestamps.get(
                    strm,
                    commit_timestamp_iso(commit_timestamp),
                )
                update_department_history(history_document, department_snapshot, snapshot_timestamp)
                processed_departments += 1

            if processed_commits % 250 == 0:
                print(
                    f"Processed {processed_commits} commits and {processed_departments} department snapshots..."
                )

    for (strm, department), history_document in histories.items():
        write_history_document(history_document, history_path_for_department(strm, department))

    print(
        f"Wrote {len(histories)} history files from {processed_departments} department snapshots across {processed_commits} commits."
    )


def main():
    args = parse_args()
    target_strms = discover_strms(args.strms)
    backfill_history(target_strms)


if __name__ == "__main__":
    main()
