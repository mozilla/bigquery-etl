"""Tests for bigquery_etl/experiments/experimenter.py."""

import datetime

import pytest
import pytz

from bigquery_etl.experiments.experimenter import Branch, NimbusExperiment, _status


def make_nimbus_experiment(channel="release", end_date=None):
    return NimbusExperiment(
        slug="test-experiment",
        startDate=pytz.utc.localize(datetime.datetime(2024, 1, 1)),
        endDate=end_date,
        enrollmentEndDate=None,
        proposedEnrollment=7,
        branches=[Branch(slug="control", ratio=1, features=None)],
        referenceBranch="control",
        appName="firefox_desktop",
        appId="firefox-desktop",
        channel=channel,
        channels=[channel] if channel else [],
        targeting="true",
        bucketConfig={
            "namespace": "test-namespace",
            "randomizationUnit": "normandy_id",
            "start": 500,
            "count": 1000,
            "total": 10000,
        },
        featureIds=["test-feature"],
        isRollout=False,
    )


class TestStatus:
    # Shared by to_experiment() and to_metric_config_experiment().

    def test_no_end_date_is_live(self):
        assert _status(None) == "Live"

    def test_future_end_date_is_live(self):
        future = pytz.utc.localize(
            datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)
        )
        assert _status(future) == "Live"

    def test_past_end_date_is_complete(self):
        past = pytz.utc.localize(
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)
        )
        assert _status(past) == "Complete"


class TestToMetricConfigExperiment:
    def test_bucket_config_field_mapping(self):
        ne = make_nimbus_experiment()
        bucket_config = ne.to_metric_config_experiment().bucket_config
        assert bucket_config.randomization_unit == "normandy_id"
        assert bucket_config.namespace == "test-namespace"
        assert bucket_config.start == 500
        assert bucket_config.count == 1000
        assert bucket_config.total == 10000

    @pytest.mark.parametrize("channel", ["nightly", "beta", "release"])
    def test_valid_channel_preserved(self, channel):
        ne = make_nimbus_experiment(channel=channel)
        parser_channel = ne.to_metric_config_experiment().channel
        assert parser_channel is not None
        assert parser_channel.value == channel

    @pytest.mark.parametrize("channel", ["", "esr", "beta-tester"])
    def test_invalid_channel_becomes_none(self, channel):
        ne = make_nimbus_experiment(channel=channel)
        assert ne.to_metric_config_experiment().channel is None
