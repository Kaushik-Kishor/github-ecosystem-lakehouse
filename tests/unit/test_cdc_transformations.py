import json

from spark.processing.cdc_transformations import build_changes


def make_message(
    offset,
    repo_id,
    operation,
    after=None,
    before=None,
    tombstone=False,
):
    key = json.dumps({
        "payload": {"repo_id": repo_id}
    })

    if tombstone:
        value = None
    else:
        value = json.dumps({
            "payload": {
                "op": operation,
                "after": after,
                "before": before,
                "ts_ms": 1705300000000,
            }
        })

    return key, value, 0, offset


def make_df(spark, rows):
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("message_key", StringType(), True),
        StructField("message_value", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
    ])

    return spark.createDataFrame(rows, schema=schema)


def test_insert_event(spark):
    after = {
        "repo_id": 101,
        "name": "test-repo",
        "owner": "alice",
        "stars": 10,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    df = make_df(
        spark,
        [make_message(10, 101, "c", after=after)],
    )

    result = build_changes(df).first()

    assert result.repo_id == 101
    assert result.name == "test-repo"
    assert result.owner == "alice"
    assert result.stars == 10
    assert result.is_archived is False
    assert result.is_deleted is False
    assert result.source_op == "c"
    assert result.last_kafka_offset == 10


def test_update_selects_latest_event(spark):
    original = {
        "repo_id": 101,
        "name": "old-name",
        "owner": "alice",
        "stars": 10,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    updated = {
        "repo_id": 101,
        "name": "new-name",
        "owner": "alice",
        "stars": 50,
        "is_archived": False,
        "updated_at": "2024-01-15T09:00:00Z",
    }

    rows = [
        make_message(10, 101, "c", after=original),
        make_message(11, 101, "u", after=updated, before=original),
    ]

    result = build_changes(make_df(spark, rows)).first()

    assert result.name == "new-name"
    assert result.stars == 50
    assert result.last_kafka_offset == 11
    assert result.source_op == "u"


def test_delete_preserves_before_values(spark):
    before = {
        "repo_id": 202,
        "name": "deleted-repo",
        "owner": "bob",
        "stars": 25,
        "is_archived": False,
        "updated_at": "2024-01-15T10:00:00Z",
    }

    df = make_df(
        spark,
        [make_message(20, 202, "d", before=before)],
    )

    result = build_changes(df).first()

    assert result.repo_id == 202
    assert result.name == "deleted-repo"
    assert result.owner == "bob"
    assert result.stars == 25
    assert result.is_deleted is True
    assert result.source_op == "d"


def test_tombstone_is_ignored(spark):
    df = make_df(
        spark,
        [make_message(21, 202, None, tombstone=True)],
    )

    assert build_changes(df).count() == 0


def test_latest_event_per_repository(spark):
    repo_a_old = {
        "repo_id": 101,
        "name": "old",
        "owner": "alice",
        "stars": 10,
        "is_archived": False,
        "updated_at": "2024-01-15T08:00:00Z",
    }

    repo_a_new = {
        "repo_id": 101,
        "name": "new",
        "owner": "alice",
        "stars": 99,
        "is_archived": False,
        "updated_at": "2024-01-15T09:00:00Z",
    }

    repo_b = {
        "repo_id": 202,
        "name": "another-repo",
        "owner": "bob",
        "stars": 5,
        "is_archived": False,
        "updated_at": "2024-01-15T10:00:00Z",
    }

    rows = [
        make_message(10, 101, "c", after=repo_a_old),
        make_message(11, 101, "u", after=repo_a_new, before=repo_a_old),
        make_message(12, 202, "c", after=repo_b),
    ]

    results = build_changes(make_df(spark, rows)).collect()
    by_id = {row.repo_id: row for row in results}

    assert len(by_id) == 2
    assert by_id[101].stars == 99
    assert by_id[101].last_kafka_offset == 11
    assert by_id[202].name == "another-repo"
