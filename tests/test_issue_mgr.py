import unittest
from microservices_miner.control.issue_mgr import IssueMgr
import os
from microservices_miner.model.issue import Issue


class TestIssueMgr(unittest.TestCase):
    def setUp(self) -> None:
        db_path = os.getenv('DB_PATH')
        self.issue_mgr = IssueMgr(path_to_db=db_path)

    def test_get_issues_by_label(self):
        repository_id = 1
        issues = self.issue_mgr.get_issues_by_label(repository_id=repository_id)
        self.assertGreater(len(issues), 0)
        for issue in issues:
            self.assertIsInstance(issue, Issue)


if __name__ == '__main__':
    unittest.main()
