from datetime import date, datetime

from spark.gold.transformations import (
    build_daily_metrics,
    build_hourly_metrics,
    build_repo_activity,
    build_developer_activity,
)


def sample_events(spark):
    rows = [
        ("e1", "PushEvent", datetime(2024, 1, 15, 8),
         date(2024, 1, 15), 1, "alice", 100, "org/repo-a"),

        ("e2", "PushEvent", datetime(2024, 1, 15, 8),
         date(2024, 1, 15), 2, "dependabot[bot]", 100, "org/repo-a"),

        ("e3", "IssuesEvent", datetime(2024, 1, 15, 9),
         date(2024, 1, 15), 1, "alice", 200, "org/repo-b"),
    ]

    columns = [
        "event_id", "event_type", "event_timestamp", "event_date",
        "actor_id", "actor_login", "repo_id", "repo_name",
    ]

    return spark.createDataFrame(rows, columns)


def test_daily_metrics(spark):
    result = build_daily_metrics(sample_events(spark))

    assert result.agg({"event_count": "sum"}).first()[0] == 3

    push = result.filter("event_type = 'PushEvent'").first()

    assert push.event_count == 2
    assert push.unique_actors == 2
    assert push.unique_repositories == 1


def test_hourly_metrics(spark):
    result = build_hourly_metrics(sample_events(spark))

    assert result.count() == 2

    hour_8 = result.filter("event_hour = 8").first()

    assert hour_8.event_count == 2
    assert hour_8.unique_actors == 2


def test_repository_activity(spark):
    result = build_repo_activity(sample_events(spark))

    assert result.count() == 2

    repo = result.filter("repo_id = 100").first()

    assert repo.event_count == 2
    assert repo.distinct_event_types == 1
    assert repo.unique_contributors == 2


def test_developer_activity_and_bot_classification(spark):
    result = build_developer_activity(sample_events(spark))

    assert result.count() == 2

    bot = result.filter("is_bot = true").first()
    human = result.filter("is_bot = false").first()

    assert bot.actor_login == "dependabot[bot]"
    assert bot.event_count == 1

    assert human.actor_login == "alice"
    assert human.event_count == 2
    assert human.unique_repositories == 2
