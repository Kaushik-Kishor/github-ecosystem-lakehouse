import pytest

from spark.processing.repos_silver_incremental import (
    advance_checkpoint,
    read_checkpoint,
)
from tests.integration.conftest import TEST_CHECKPOINT_TABLE


def test_read_checkpoint(checkpoint_db):
    assert read_checkpoint(
        checkpoint_db,
        TEST_CHECKPOINT_TABLE,
    ) == (100, 1000)


def test_checkpoint_advances_successfully(checkpoint_db):
    advance_checkpoint(
        checkpoint_db,
        old_offset=100,
        old_snapshot=1000,
        new_offset=150,
        new_snapshot=2000,
        control_table=TEST_CHECKPOINT_TABLE,
    )

    assert read_checkpoint(
        checkpoint_db,
        TEST_CHECKPOINT_TABLE,
    ) == (150, 2000)


def test_stale_compare_and_set_is_rejected(checkpoint_db):
    advance_checkpoint(
        checkpoint_db,
        old_offset=100,
        old_snapshot=1000,
        new_offset=125,
        new_snapshot=1500,
        control_table=TEST_CHECKPOINT_TABLE,
    )

    with pytest.raises(RuntimeError, match="compare-and-set failed"):
        advance_checkpoint(
            checkpoint_db,
            old_offset=100,
            old_snapshot=1000,
            new_offset=150,
            new_snapshot=2000,
            control_table=TEST_CHECKPOINT_TABLE,
        )

    checkpoint_db.rollback()

    assert read_checkpoint(
        checkpoint_db,
        TEST_CHECKPOINT_TABLE,
    ) == (125, 1500)
