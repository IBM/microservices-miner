# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.control.database_conn import RepositoryConn
from microservices_miner.control.database_conn import ServiceRepositoryConn
from microservices_miner.model.repository import Repository
from microservices_miner.control.commit_mgr import CommitMgr
from microservices_miner.control.filesystem_mgr import FileSystemMgr


class RepositoryMgr:

    def __init__(self, path_to_db):
        self.path_to_db = path_to_db
        self.repo_conn = RepositoryConn(self.path_to_db)

    def insert_repository(self, repository):
        """

        Parameters
        ----------
        repository: Repository

        Returns
        -------
        int
        """
        if repository.repository_id is None:
            repo = self.repo_conn.get_repository_by_url(url=repository.url)
        else:
            repo = self.repo_conn.get_repository(repo_id=repository.repository_id)
        # if repository has not been inserted
        if repo is None:
            row_id = self.repo_conn.insert_repository(repository)
            return row_id
        else:
            return repo.repository_id

    def find_inconsistent_commits(self, repository, extensions):
        """

        :param repository: Repository
        :param extensions: List[str]
        :return: List[Commit]
        """
        commit_mgr = CommitMgr(path_to_db=self.path_to_db)
        commits = list()

        aux = commit_mgr.find_inconsistent_commits(repository_id=repository.repository_id,
                                                   extensions=extensions)
        commits.extend(aux)
        return commits

    def get_repository(self, service_id: int, repository_id: int = None, url: str = None,
                       start_date: str = None, end_date: str = None) -> Repository:
        """

        Parameters
        ----------
        service_id: int
        repository_id: int
        url: str
            repository name
        start_date: str
            date of the first commit
        end_date: str
            date of the last commit

        Returns
        -------
        Repository
        """
        filesystem_mgr = FileSystemMgr(db_path=self.path_to_db)
        if repository_id is not None:
            repo = self.repo_conn.get_repository(repo_id=repository_id)
        else:
            repo = self.repo_conn.get_repository_by_url(url=url)
        if repo is not None:
            commit_mgr = CommitMgr(path_to_db=self.path_to_db)
            repo.commits = commit_mgr.get_base_commits(repo=repo, start_date=start_date, end_date=end_date)
            for c in repo.commits:

                for i in range(len(c.file_modifications)-1, -1, -1):
                    fm = c.file_modifications[i]

                    if not filesystem_mgr.check_filename(filename=fm.filename, service_id=service_id,
                                                         repository_id=repo.repository_id):
                        del c.file_modifications[i]

        return repo

    @staticmethod
    def create_repository(name, url):
        repo = Repository(name=name, url=url)
        return repo

    def list_repositories(self):
        """

        Returns
        -------
        list of Repository
        """
        repos = self.repo_conn.list_repositories()
        return repos

    def get_repository_by_commit(self, sha):
        """

        Parameters
        ----------
        sha: str

        Returns
        -------
        Repository
        """
        repo = self.repo_conn.get_repository_by_sha(sha)
        return repo

    def delete_repository(self, repository_id):
        """
        delete repository and its commits

        :param repository_id: int
        :return: bool
            True if all repository data was deleted, otherwise False
        """
        commit_mgr = CommitMgr(path_to_db=self.path_to_db)
        commit_deleted = commit_mgr.delete_commits_of_repository(repository_id)
        repo_deleted = False
        if commit_deleted:
            self.repo_conn.delete_repository(repository_id=repository_id)
            repo_deleted = True
        self.repo_conn.delete_repository(repository_id=repository_id)
        service_repo_conn = ServiceRepositoryConn(path_to_db=self.path_to_db)
        service_repo_conn.delete_service_repo(repository_id=repository_id)
        return repo_deleted
