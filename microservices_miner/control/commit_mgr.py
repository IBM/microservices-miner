# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.control.database_conn import RepositoryCommitConn, RepositoryConn, UserConn, FileModificationConn, \
    ParentCommitConn, ParentCommitRepoCommitConn
from microservices_miner.model.repository import Repository
from microservices_miner.model.git_commit import Commit
from datetime import datetime
from typing import List
# from control.repository_mgr import RepositoryMgr


class CommitMgr:

    def __init__(self, path_to_db):
        self.path_to_db = path_to_db
        self.repo_commit_conn = RepositoryCommitConn(path_to_db=path_to_db)

    def insert_commit(self, repository, commit, parent_commit_shas):
        """

        Parameters
        ----------
        repository: Repository
        commit: Commit
        parent_commit_shas: list of tuple

        Returns
        -------

        """
        user_conn = UserConn(path_to_db=self.path_to_db)
        file_modification_conn = FileModificationConn(path_to_db=self.path_to_db)
        user = user_conn.get_user(name=commit.user.name, email=commit.user.email)
        assert user is not None, "Error! Unable to find the User"
        commit.commit_id = self.repo_commit_conn.insert_repository_commit(commit=commit,
                                                                          repository_id=repository.repository_id,
                                                                          user_id=user.user_id)
        # insert filemodifications
        for fm in commit.file_modifications:
            file_modification_conn.insert_file_modification(commit_id=commit.commit_id, fm=fm)

        parent_commit_conn = ParentCommitConn(path_to_db=self.path_to_db)
        parent_commit_repo_commit_conn = ParentCommitRepoCommitConn(path_to_db=self.path_to_db)

        # assure that positions are unique
        unique_position = set()
        # insert SHAs and positions into database
        for positions, parent_sha in parent_commit_shas:
            assert positions not in unique_position, "Error! Repeated positions: {}".format(parent_commit_shas)
            unique_position.add(positions)
            parent_commit_id = parent_commit_conn.insert_repository_commit(sha=parent_sha, position=positions)
            parent_commit_repo_commit_conn.insert_parent_commit_repository_commit(repo_commit_id=commit.commit_id,
                                                                                  parent_commit_id=parent_commit_id)

    def find_inconsistent_commits(self, repository_id, extensions):
        """

        Parameters
        ----------
        repository_id: int
        extensions: list of str

        Returns
        -------
        list of Commit
        """
        commit_conn = RepositoryCommitConn(self.path_to_db)
        commits = commit_conn.get_inconsistent_commits(repository_id=repository_id, extensions=extensions)
        return commits

    def get_commit_by_position(self, repository_id, pos):
        """
        sorted by date, 0 is the first and -1 is the last

        Parameters
        ----------
        repository_id: int
        pos: int

        Returns
        -------
        Commit
        """
        repo_conn = RepositoryConn(path_to_db=self.path_to_db)
        repo = repo_conn.get_repository(repo_id=repository_id)
        commits = self.repo_commit_conn.get_commits_by_repo(repository_id=repo.repository_id)
        if len(commits) == 0:
            print('Warning!! {} has no commits'.format(repo))
            return None
        else:
            sorted_commits = sorted(commits, key=lambda k: k.date)
            commit = sorted_commits[pos]
            return commit

    def get_base_commits(self, repo: Repository, start_date, end_date):
        """
        gets a list of Commit objects that represent the base commits, that is, commits that are the base for merge operations
        Parameters
        ----------
        repo: Repository
        start_date: str
            ISO format
        end_date: str
            ISO format

        Returns
        -------
        List
        """
        commits = self.get_commits_by_repo(repository_id=repo.repository_id, start_date=start_date, end_date=end_date)
        commit_list = list()
        if commits is not None and len(commits) > 0:
            commit = commits[-1]

            if start_date is not None:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            else:
                start_dt = datetime(year=1000, month=1, day=1)

            if end_date is not None:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end_dt = datetime(year=9999, month=12, day=1)
            commit_conn = RepositoryCommitConn(self.path_to_db)
            while commit is not None and start_dt <= commit.date < end_dt:
                new_commit = commit_conn.get_commit_and_its_filemodifications_by_sha(sha=commit.sha)
                commit_list.append(new_commit)

                parent_sha = self.get_parent_commit_sha(commit=commit)
                commit = self.get_commit(sha=parent_sha)

        return commit_list

    def get_loc_per_commit(self, commit):
        """

        Parameters
        ----------
        commit: Commit

        Returns
        -------
        int
        """
        self.get_parent_commit_sha(commit=commit)

    def get_commit(self, sha):
        """

        Parameters
        ----------
        sha: str

        Returns
        -------
        Commit
        """
        commit = self.repo_commit_conn.get_commit_and_its_filemodifications_by_sha(sha)

        return commit

    def get_commits_by_repo(self, repository_id: int, start_date: str = None, end_date: str = None) -> List[Commit]:
        """

        Parameters
        ----------
        repository_id: int
        start_date: str
        end_date: str

        Returns
        -------

        """
        commits_aux = self.repo_commit_conn.get_commits_by_repo(repository_id=repository_id,
                                                                start_date=start_date,
                                                                end_date=end_date)
        return commits_aux

    def get_parent_commit_sha(self, commit: Commit):
        """

        Parameters
        ----------
        commit: Commit

        Returns
        -------
        str or None
        """
        parent_commit_conn = ParentCommitConn(path_to_db=self.path_to_db)
        sha = parent_commit_conn.get_parent_commit_sha(child_commit_id=commit.commit_id)

        return sha

    def delete_commits_of_repository(self, repository_id):
        """

        :param repository_id: int
        :return: bool
            True if all commits and file modifications were deleted; False otherwise
        """
        repo_conn = RepositoryConn(path_to_db=self.path_to_db)
        repo = repo_conn.get_repository(repo_id=repository_id)
        commit_counter = 0
        commits_were_deleted = False
        if repo is None:
            commits_were_deleted = True
        else:
            commits_conn = RepositoryCommitConn(path_to_db=self.path_to_db)
            file_modification_conn = FileModificationConn(path_to_db=self.path_to_db)
            parent_commit_conn = ParentCommitConn(path_to_db=self.path_to_db)
            parent_commit_repo_commit_conn = ParentCommitRepoCommitConn(path_to_db=self.path_to_db)
            commits = commits_conn.get_commits_by_repo(repository_id=repo.repository_id)
            for c in commits:
                file_modification_conn.delete_file_modifications(commit_id=c.commit_id)
                parent_commit_id = parent_commit_conn.get_parent_commit_id(sha=c.sha)
                if parent_commit_id is not None:
                    parent_commit_conn.delete_parent_commit(parent_commit_id=parent_commit_id)
                    parent_commit_repo_commit_conn.delete_parent_commit_repocommit(parent_commit_id=parent_commit_id)
                commits_conn.delete_commit(commit_id=c.commit_id)
                commit_counter += 1
                commits_were_deleted = True
        print('{} commits were deleted'.format(commit_counter))
        return commits_were_deleted
