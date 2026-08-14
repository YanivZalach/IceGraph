import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from snapshot_analyzer import utils
from snapshot_analyzer.snapshot_analyzer import SnapshotAnalyzer


class BuildActionLinkTest(unittest.TestCase):
    def test_builds_spark_history_link(self):
        summary = {"engine-name": "spark", "app-id": "application-123"}

        with patch.dict(os.environ, {"SPARK_HISTORY_SERVER_URL": "http://spark-history:18080"}):
            self.assertEqual(
                utils.build_action_link(summary),
                "http://spark-history:18080/history/application-123/",
            )

    def test_removes_trailing_slash_from_base_url(self):
        summary = {"engine-name": "spark", "app-id": "application-123"}

        with patch.dict(os.environ, {"SPARK_HISTORY_SERVER_URL": "http://spark-history:18080/"}):
            self.assertEqual(
                utils.build_action_link(summary),
                "http://spark-history:18080/history/application-123/",
            )

    def test_returns_none_when_history_server_is_not_configured(self):
        summary = {"engine-name": "spark", "app-id": "application-123"}

        with patch.dict(os.environ, {"SPARK_HISTORY_SERVER_URL": ""}):
            self.assertIsNone(utils.build_action_link(summary))

    def test_returns_none_for_non_spark_snapshot(self):
        summary = {"engine-name": "flink", "app-id": "application-123"}

        with patch.dict(os.environ, {"SPARK_HISTORY_SERVER_URL": "http://spark-history:18080"}):
            self.assertIsNone(utils.build_action_link(summary))

    def test_returns_none_without_application_id(self):
        summary = {"engine-name": "spark"}

        with patch.dict(os.environ, {"SPARK_HISTORY_SERVER_URL": "http://spark-history:18080"}):
            self.assertIsNone(utils.build_action_link(summary))

    def test_analyzer_populates_action_link(self):
        snapshot = SimpleNamespace(
            action_link=None,
            operation="append",
            operation_description="append",
            summary={"engine-name": "spark", "app-id": "application-123"},
        )
        table_data = SimpleNamespace(snapshots=[snapshot])

        with patch.dict(os.environ, {"SPARK_HISTORY_SERVER_URL": "http://spark-history:18080"}):
            analyzed_table_data = SnapshotAnalyzer(table_data).analyze()

        self.assertIs(analyzed_table_data, table_data)
        self.assertEqual(snapshot.action_link, "http://spark-history:18080/history/application-123/")
        self.assertEqual(snapshot.operation_description, "append")


if __name__ == "__main__":
    unittest.main()
