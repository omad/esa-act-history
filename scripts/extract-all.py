#!/usr/bin/env python3
#
# Extract all the versions of feed.json into separate files using jj
# 

from concurrent.futures import ProcessPoolExecutor

from pathlib import Path
import subprocess

CMD = ["jj", "log", "--no-graph", "-r", 'root()..@', "-T", 'concat(commit_id, " ", self.author().timestamp().format("%s"), "\\n")']

res = subprocess.run(CMD, capture_output=True, text=True)

commits = [line.strip().split() for line in res.stdout.splitlines()]

# for commit_id, timestamp in commits:
    # print(f"XX: {commit_id} {timestamp}")

def extract_file(commit_id: str, timestamp: str, output_path: Path | None = None):
    if output_path is None:
        output_path = Path('/tmp/esa-feeds')
    cmd = ['jj', 'file', 'show', '-r', commit_id, 'root-file:"feed.json"']
    res = subprocess.run(cmd, capture_output=True, text=True)
    res.check_returncode()

    output_file = output_path / f"{timestamp}_{commit_id}.json"
    output_file.write_text(res.stdout)

# def run_command(cmd):
#     result = 
#     return result.stdout, result.stderr, result.returncode

with ProcessPoolExecutor(max_workers=50) as executor:
    futures = []
    for commit_id, timestamp in commits:
        futures.append(executor.submit(extract_file, commit_id, timestamp))
    results = [future.result() for future in futures]
#     futures = [executor.submit(run_command, cmd) for cmd in commands]
#     results = [future.result() for future in futures]

