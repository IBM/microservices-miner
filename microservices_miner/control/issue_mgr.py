# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.control.database_conn import IssueConn, UserConn, RepositoryConn
from microservices_miner.model.repository import Repository
import logging
logging.basicConfig(filename='github_miner.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


class IssueMgr:

    def __init__(self, path_to_db):
        self.db_path = path_to_db
        self.issue_conn = IssueConn(path_to_db)
        self.user_conn = UserConn(path_to_db)
        self.repo_conn = RepositoryConn(path_to_db)

    def insert_issue_into_db(self, repo):
        """

        Parameters
        ----------
        repo: Repository

        Returns
        -------

        """
        for issue in repo.issues:
            updated_at = issue.updated_at
            if updated_at is not None:
                updated_at_str = updated_at.isoformat()
            else:
                updated_at_str = None
            if issue.closed_at is None:
                closed_at_str = None
            else:
                closed_at_str = issue.closed_at.isoformat()
            user_id = issue.user.commit_id
            issue_id = self.issue_conn.insert_issue(title=issue.title, body=issue.body, repository_id=repo.repository_id,
                                                    closed_at=closed_at_str, updated_at=updated_at_str,
                                                    created_at=issue.created_at.isoformat(),
                                                    user_id=user_id, state=issue.state)
            for assignee in issue.assignees:
                assignee_id = self.issue_conn.insert_assignee(assignee)
                self.issue_conn.insert_issue_assignee(assignee_id=assignee_id, issue_id=issue_id)

            for label in issue.labels:
                label_id = self.issue_conn.insert_label(label)
                self.issue_conn.insert_issue_label(issue_id=issue_id, label_id=label_id)

    def get_issues_by_label(self, repository_id: int):
        """

        Parameters
        ----------
        repository_id: int

        Returns
        -------
        List[Issue]
        """
        issues = self.issue_conn.get_issues(repository_id=repository_id)

        return issues

    def get_label(self, name):
        """

        Parameters
        ----------
        name

        Returns
        -------
        Label
        """
        labels = self.issue_conn.get_labels(name=name)
        if len(labels) == 0:
            return None
        else:
            label = labels.pop()
            return label

    def get_assignee(self, login):
        """

        Parameters
        ----------
        login

        Returns
        -------
        Assignee
        """
        assignees = self.issue_conn.get_assignee(login)
        if len(assignees) == 0:
            return None
        else:
            assignee = assignees.pop()
            return assignee

    def insert_assignee(self, assignee):
        """

        Parameters
        ----------
        assignee: Assignee

        Returns
        -------
        int
        """
        rowid = self.issue_conn.insert_assignee(assignee)
        return rowid

    def insert_label(self, label):
        """

        Parameters
        ----------
        label: Label

        Returns
        -------
        int
        """
        row_id = self.issue_conn.insert_label(label)
        return row_id
