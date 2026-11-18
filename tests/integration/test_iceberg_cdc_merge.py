import json

from spark.processing.cdc_transformations import build_changes
from spark.processing.repos_silver_incremental import merge_changes
from tests.integration.conftest import TEST_TABLE


def make_message(offset, repo_id, operation, after=None, before=None):
    return (
        json.dumps({"payload": {"repo_id": repo_id}}),
        json.dumps({
            "payload": {
                "op": operation,
                "after": after,
                "before": before,
                "ts_ms": 1705300000000,
            }
        }),
        0,
        offset,
    )


def make_changes(spark, rows):
    source = spark.createDataFrame(
        rows,
        [
            "message_key",
            "message_value",
            "partition",
            "offset",
        ],
    )
    return build_changes(source)


def get_repo(spark, repo_id):
    return (
        spark.table(TEST_TABLE)
        .filter(f"repo_id = {repo_id}")
        .first()
    )


def test_insert_repository(iceberg_spark, clean_test_table):
    repo = {
        "repo_id": 900001,
        "name": "integration-repo",
        "owner": "nexus-test",
        "stars": 10,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    changes = make_changes(
        iceberg_spark,
        [make_message(1, 900001, "c", after=repo)],
    )

    merge_changes(iceberg_spark, changes, TEST_TABLE)

    result = get_repo(iceberg_spark, 900001)

    assert result is not None
    assert result.name == "integration-repo"
    assert result.stars == 10
    assert result.is_deleted is False
    assert result.last_kafka_offset == 1


def test_update_repository(iceberg_spark, clean_test_table):
    original = {
        "repo_id": 900002,
        "name": "old-name",
        "owner": "nexus-test",
        "stars": 10,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    updated = {
        **original,
        "name": "new-name",
        "stars": 75,
        "updated_at": "2024-01-15T09:00:00Z",
    }

    merge_changes(
        iceberg_spark,
        make_changes(
            iceberg_spark,
            [make_message(2, 900002, "c", after=original)],
        ),
        TEST_TABLE,
    )

    merge_changes(
        iceberg_spark,
        make_changes(
            iceberg_spark,
            [make_message(3, 900002, "u", after=updated, before=original)],
        ),
        TEST_TABLE,
    )

    result = get_repo(iceberg_spark, 900002)

    assert result.name == "new-name"
    assert result.stars == 75
    assert result.last_kafka_offset == 3


def test_delete_repository_is_soft_delete(iceberg_spark, clean_test_table):
    original = {
        "repo_id": 900003,
        "name": "deleted-repo",
        "owner": "nexus-test",
        "stars": 20,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    merge_changes(
        iceberg_spark,
        make_changes(
            iceberg_spark,
            [make_message(4, 900003, "c", after=original)],
        ),
        TEST_TABLE,
    )

    merge_changes(
        iceberg_spark,
        make_changes(
            iceberg_spark,
            [make_message(5, 900003, "d", before=original)],
        ),
        TEST_TABLE,
    )

    result = get_repo(iceberg_spark, 900003)

    assert result is not None
    assert result.is_deleted is True
    assert result.name == "deleted-repo"
    assert result.last_kafka_offset == 5


def test_stale_offset_cannot_overwrite_newer_state(
    iceberg_spark,
    clean_test_table,
):
    newer = {
        "repo_id": 900004,
        "name": "newer-name",
        "owner": "nexus-test",
        "stars": 100,
        "is_archived": False,
        "updated_at": "2024-01-15T10:00:00Z",
    }

    stale = {
        **newer,
        "name": "stale-name",
        "stars": 1,
    }

    merge_changes(
        iceberg_spark,
        make_changes(
            iceberg_spark,
            [make_message(10, 900004, "c", after=newer)],
        ),
        TEST_TABLE,
    )

    merge_changes(
        iceberg_spark,
        make_changes(
            iceberg_spark,
            [make_message(9, 900004, "u", after=stale, before=newer)],
        ),
        TEST_TABLE,
    )

    result = get_repo(iceberg_spark, 900004)

    assert result.name == "newer-name"
    assert result.stars == 100
    assert result.last_kafka_offset == 10


def test_replaying_same_offset_is_idempotent(
    iceberg_spark,
    clean_test_table,
):
    repo = {
        "repo_id": 900005,
        "name": "replay-test",
        "owner": "nexus-test",
        "stars": 30,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    changes = make_changes(
        iceberg_spark,
        [make_message(20, 900005, "c", after=repo)],
    )

    merge_changes(iceberg_spark, changes, TEST_TABLE)
    merge_changes(iceberg_spark, changes, TEST_TABLE)

    results = (
        iceberg_spark.table(TEST_TABLE)
        .filter("repo_id = 900005")
        .collect()
    )

    assert len(results) == 1
    assert results[0].last_kafka_offset == 20
    assert results[0].stars == 30


def test_multiple_repositories_merge_independently(
    iceberg_spark,
    clean_test_table,
):
    repo_a = {
        "repo_id": 900006,
        "name": "repo-a",
        "owner": "alice",
        "stars": 10,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    repo_b = {
        "repo_id": 900007,
        "name": "repo-b",
        "owner": "bob",
        "stars": 20,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    changes = make_changes(
        iceberg_spark,
        [
            make_message(21, 900006, "c", after=repo_a),
            make_message(22, 900007, "c", after=repo_b),
        ],
    )

    merge_changes(iceberg_spark, changes, TEST_TABLE)

    assert get_repo(iceberg_spark, 900006).name == "repo-a"
    assert get_repo(iceberg_spark, 900007).name == "repo-b"
    assert iceberg_spark.table(TEST_TABLE).count() == 2
