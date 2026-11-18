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
                "ts_ms": 1705300000000 + offset,
            }
        }),
        0,
        offset,
    )


def make_changes(spark, messages):
    source = spark.createDataFrame(
        messages,
        ["message_key", "message_value", "partition", "offset"],
    )
    return build_changes(source)


def test_cdc_end_to_end_and_reconciliation(
    iceberg_spark,
    clean_test_table,
):
    repo_a_initial = {
        "repo_id": 990001,
        "name": "e2e-repo-a",
        "owner": "alice",
        "stars": 10,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    repo_b_initial = {
        "repo_id": 990002,
        "name": "e2e-repo-b",
        "owner": "bob",
        "stars": 20,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    # Batch 1: initial repository inserts.
    batch_one = make_changes(
        iceberg_spark,
        [
            make_message(1, 990001, "c", after=repo_a_initial),
            make_message(2, 990002, "c", after=repo_b_initial),
        ],
    )

    merge_changes(iceberg_spark, batch_one, TEST_TABLE)

    # Batch 2: update repository A and delete repository B.
    repo_a_updated = {
        **repo_a_initial,
        "name": "e2e-repo-a-updated",
        "stars": 150,
        "updated_at": "2024-01-15T09:00:00Z",
    }

    batch_two = make_changes(
        iceberg_spark,
        [
            make_message(
                3, 990001, "u",
                after=repo_a_updated,
                before=repo_a_initial,
            ),
            make_message(
                4, 990002, "d",
                before=repo_b_initial,
            ),
        ],
    )

    merge_changes(iceberg_spark, batch_two, TEST_TABLE)

    # Reconcile final Silver state against expected source state.
    actual = {
        row.repo_id: row
        for row in iceberg_spark.table(TEST_TABLE).collect()
    }

    expected = {
        990001: {
            "name": "e2e-repo-a-updated",
            "owner": "alice",
            "stars": 150,
            "is_deleted": False,
            "last_kafka_offset": 3,
        },
        990002: {
            "name": "e2e-repo-b",
            "owner": "bob",
            "stars": 20,
            "is_deleted": True,
            "last_kafka_offset": 4,
        },
    }

    assert set(actual) == set(expected)

    for repo_id, expected_values in expected.items():
        row = actual[repo_id]

        for field, expected_value in expected_values.items():
            assert getattr(row, field) == expected_value, (
                f"Mismatch for repo_id={repo_id}, field={field}: "
                f"expected {expected_value!r}, got {getattr(row, field)!r}"
            )

    # Aggregate reconciliation checks.
    assert len(actual) == 2
    assert sum(row.is_deleted for row in actual.values()) == 1
    assert sum(not row.is_deleted for row in actual.values()) == 1
    assert sum(row.stars for row in actual.values()) == 170
