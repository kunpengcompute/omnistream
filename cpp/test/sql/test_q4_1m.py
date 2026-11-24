# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# omnistream is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

import os
import subprocess
import re
import sys
import logging
from typing import List, Optional

try:
    import pytest
except ImportError:
    print("pytest is not installed, please install it first")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(name)s - %(funcName)-15s - %(levelname)-7s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_flink_pids() -> List[str]:
    """
    Get PIDs of running Flink processes. There are typically 2 processes:
    1. TaskManagerRunner
    2. StandaloneSessionClusterEntrypoint
    """
    try:
        jps_output = subprocess.check_output(['jps']).decode()
        pids = []
        for line in jps_output.splitlines():
            if 'TaskManagerRunner' in line or 'StandaloneSessionClusterEntrypoint' in line:
                pid = line.split()[0]
                pids.append(pid)
        return pids
    except subprocess.CalledProcessError:
        return []


def kill_processes(
    pid_l: List[str]
) -> None:
    """
    Kill processes by their PIDs
    """
    if not pid_l:
        return

    logger.warning(f"Stopping {len(pid_l)} process of previous cluster")
    for pid in pid_l:
        try:
            subprocess.run(['kill', '-9', pid], check=True)
        except subprocess.CalledProcessError:
            logger.error(f"Failed to kill process {pid}")


def is_sql_path_updated(
    sql_file_path: str
) -> bool:
    """
    Check if placeholder paths in SQL file have been replaced
    """
    with open(sql_file_path, 'r') as f:
        content = f.read()

    if '<path_to_input_genbid_csv>' in content or '<path_to_input_genauction_csv>' in content:
        return False

    return True


def start_cluster() -> bool:
    """
    Start Flink cluster
    """
    try:
        subprocess.run(['./bin/start-cluster.sh'],
                       check=True,
                       cwd=os.environ.get('FLINK_HOME'))
        return True
    except subprocess.CalledProcessError:
        logger.error("Failed to start cluster")
        return False


@pytest.fixture(scope="function")
def flink_cluster():
    """
    Fixture to manage Flink cluster lifecycle
    """
    # Check if `FLINK_HOME` environment variable is set
    if 'FLINK_HOME' not in os.environ:
        pytest.fail("FLINK_HOME environment variable is not set")

    # Kill existing Flink processes
    kill_processes(get_flink_pids())

    # Start cluster
    if not start_cluster():
        pytest.fail("Failed to start cluster")

    logger.info("Cluster started successfully")

    yield  # where the test runs

    # Cleanup after test
    kill_processes(get_flink_pids())
    logger.info("Cluster cleaned up")


def run_sql_query(
    sql_file_path: str
) -> Optional[str]:
    """
    Run query in the given file and return output
    """
    assert os.path.exists(sql_file_path), f"SQL file '{
        sql_file_path}' does not exist"

    logger.info(f"Start running SQL query from file: '{sql_file_path}'")

    try:
        flink_home = os.environ.get('FLINK_HOME')
        result = subprocess.run(
            [f"{flink_home}/bin/sql-client.sh", "-f", sql_file_path],
            cwd=flink_home,
            encoding='utf-8',
            timeout=60,
            check=True,
            capture_output=True,
            text=True
        )
        return result.stdout

    except Exception as e:
        logger.error(f"Failed to run SQL client: {e}")
        return None


def verify_output(
    output: str
) -> bool:
    """
    Verify if output matches expected format

    Find the table boarder at the end of the output, the output is in the following format:

    <some_unrelated_logs>
    +----+----------------------+----------------------+
    | op |             category |               EXPR$1 |
    +----+----------------------+----------------------+
    | +I |                   14 |                29123 |
    | .. |                   .. |                   .. |
    +----+----------------------+----------------------+
    <some_unrelated_logs>
    """
    # Get output table content
    table_content = re.split(r'\+-+\+-+\+-+\+', output)[2].strip()
    line_l = table_content.split('\n')

    # Track the latest value for each ID
    val_by_id_d = {}
    expected_val_by_id_d = {10: 29500750, 11: 28682231,
                            12: 29333115, 13: 29148336, 14: 28722320}

    needed_id_s = set(range(10, 15))  # IDs from 10 to 14
    found_id_s = set()

    for line in reversed(line_l): # iterate reversely to find the latest update for each id
        # Skip if we've found all needed IDs
        if found_id_s == needed_id_s:
            break

        # Parse line using string split and strip
        part_l = [part.strip() for part in line.strip('|').split('|')]
        if len(part_l) != 3:  # Should have 3 parts: 'op', 'category', 'EXPR$1'
            raise ValueError(f"Invalid line format: '{line}'")

        id_ = int(part_l[1])
        val = int(part_l[2])

        # Only process if this ID is needed and we haven't found it yet
        if id_ in needed_id_s and id_ not in found_id_s:
            val_by_id_d[id_] = val
            found_id_s.add(id_)

    # Verify we found all needed IDs
    if found_id_s != needed_id_s:
        logger.error(f"Missing IDs: {needed_id_s - found_id_s}")
        return False

    # Verify the values are correct
    for id_ in range(10, 15):
        if val_by_id_d[id_] != expected_val_by_id_d[id_]:
            logger.error(f"ID {id_} value mismatch: {
                         val_by_id_d[id_]} != {expected_val_by_id_d[id_]}")
            return False

    return True


def test_q4_1m(flink_cluster):
    """
    Test flink nexmark benchmark q4.sql

    There are 1000000 rows in the input file.

    The query is specified in the file `q4_1m.sql`

    The expected output is:
      category               EXPR$1
            10             29500750
            14             28722320
            13             29148336
            12             29333115
            11             28682231
        """
    # Get the directory of the current test file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dir, "q4_1m.sql")

    assert os.path.exists(sql_file_path), f"SQL file not found at {
        sql_file_path}"

    # Check and update SQL paths if needed
    if not is_sql_path_updated(sql_file_path):
        logger.error(
            "SQL file paths are not updated, please replace '<path_to_input_genauction_csv>' and '<path_to_input_genbid_csv>' with actual paths first")
        return

    # Run SQL client and get output
    output = run_sql_query(sql_file_path)
    assert output is not None, "SQL query execution failed"
    logger.info("SQL client run successfully")

    # Verify output
    assert verify_output(output), "Output verification failed"
    logger.info("Output verification successful")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
