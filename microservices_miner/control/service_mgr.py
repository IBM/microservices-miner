# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.control.repository_mgr import RepositoryMgr
from microservices_miner.control.database_conn import ServiceRepositoryConn, ServiceConn, RepositoryCommitConn
from microservices_miner.model.service import Service
from microservices_miner.model.repository import Repository
from datetime import datetime, date
import logging

logging.basicConfig(filename='github_miner.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


class ServiceMgr:

    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_service(self, service_id=None, service_name=None) -> Service:
        """

        Parameters
        ----------
        service_id: int
        service_name: str

        Returns
        -------
        Service
        """
        repo_mgr = RepositoryMgr(path_to_db=self.db_path)
        service_conn = ServiceConn(path_to_db=self.db_path)
        service = service_conn.get_service(service_id=service_id, name=service_name)  # type: Service
        service_repo_conn = ServiceRepositoryConn(path_to_db=self.db_path)
        if service is not None:
            service_repo_list = service_repo_conn.get_service_repository(service_name=service.name)
            for sr in service_repo_list:

                start_date = sr.get('start_date')
                try:
                    st = datetime.strptime(start_date, '%Y-%m-%d').date()
                except (TypeError, ValueError) as e:
                    st = datetime(year=1000, month=1, day=1).date()

                end_date = sr.get('end_date')
                try:
                    ed = datetime.strptime(end_date, '%Y-%m-%d').date()
                except (TypeError, ValueError) as e:
                    ed = datetime(year=9999, month=12, day=30).date()

                repository_id = sr.get('repository_id')
                initial_loc = sr.get('initial_loc')
                repo = repo_mgr.get_repository(repository_id=repository_id, start_date=st.isoformat(),
                                               end_date=ed.isoformat(), service_id=service.service_id)
                assert repo is not None, "Error: Repository is None: repository_id={} service_id={}" \
                    .format(repository_id, service.service_id)
                service.add_repository(repository=repo, start_date=start_date, end_date=end_date,
                                       initial_loc=initial_loc)
        return service

    def update_dates(self, service_id: int, end_date: date, start_date: date = None) -> bool:
        """
        update the field start_date of Service table based on the first commit date of any of the service's
        dependencies

        Parameters
        ----------
        service_id: int
            service ID
        end_date: date
            end date
        start_date: date

        Returns
        -------
        bool
        """
        service_conn = ServiceConn(path_to_db=self.db_path)
        updated = False
        if start_date is None:
            start_date = self._get_first_commit_date_associated_with_service(service_id)

        if start_date is not None:
            updated = service_conn.update_service(service_id=service_id, start_date=start_date,
                                                  end_date=end_date)
        return updated

    def _get_first_commit_date_associated_with_service(self, service_id):
        """
        get the date of the first commit of any repository associated with a given service specified by the service_id
        Parameters
        ----------
        service_id: int

        Returns
        -------
        date
        """
        service_repo_conn = ServiceRepositoryConn(path_to_db=self.db_path)
        commit_conn = RepositoryCommitConn(path_to_db=self.db_path)
        first_commit_date = None
        service_repo_list = service_repo_conn.get_service_repository_by_service_id(service_id=service_id)
        for sr in service_repo_list:
            repository_id = sr.get('repository_id')
            commits = commit_conn.get_commits_by_repo(repository_id=repository_id)
            if len(commits) > 0:
                sorted_commits = sorted(commits, key=lambda c: c.date)
                first_commit = sorted_commits[0]
                if first_commit_date is None or first_commit_date > first_commit.date:
                    first_commit_date = first_commit.date
        return first_commit_date

    def list_all_service_names(self):
        """

        Returns
        -------

        """
        service_conn = ServiceConn(path_to_db=self.db_path)
        names = service_conn.list_all_service_names()
        return names

    def insert_service(self, name: str, start_date_str: str):
        """

        Parameters
        ----------
        name: str
        start_date_str: str

        Returns
        -------
        int
        """
        service_conn = ServiceConn(path_to_db=self.db_path)

        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError as e:
            logging.error(e)
            raise e
        service_id = service_conn.insert_service(name=name, start_date=start_date)
        return service_id

    def insert_service_repository(self, service_name: str, repository_id: int, start_date: str = None,
                                  end_date: str = None, initial_loc: int = None):
        """

        Parameters
        ----------
        service_name: str
        repository_id: int
        start_date: str
            the date that this repo started being used for the specified service
        end_date: str
            the date that this repo stop being used for the specified service
        initial_loc: int

        Returns
        -------

        """
        service_conn = ServiceConn(path_to_db=self.db_path)
        service = service_conn.get_service(name=service_name)
        repo_mgr = RepositoryMgr(path_to_db=self.db_path)
        repo = repo_mgr.get_repository(repository_id=repository_id, service_id=service.service_id)
        service_repo_conn = ServiceRepositoryConn(self.db_path)
        # check if start_date is valid
        try:
            if start_date is not None:
                start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                assert service.start_date <= start_date_dt, \
                    "Error! service={} repo={} {} > {}".format(service.name, repo.name,
                                                               service.start_date, start_date_dt)
        except ValueError as e:
            logging.critical('Error! Invalid start_date: {} msg={}'.format(start_date, e))
            raise e

        # check if end_date is valid
        try:
            if end_date is not None:
                end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
                assert service.end_date >= end_date_dt

        except ValueError as e:
            logging.critical('Error! Invalid end_date: {} msg={}'.format(end_date, e))
            raise e
        service_repo_conn.insert_service_repo(service_id=service.service_id, repository_id=repo.repository_id,
                                              start_date=start_date, end_date=end_date, initial_loc=initial_loc)

    def delete_service(self, service_name):
        """
        delete service and its repositories

        :param service_name: str
        :return: bool
        """
        repo_mgr = RepositoryMgr(path_to_db=self.db_path)
        service = self.get_service(service_name=service_name)
        if service is None:
            return True
        else:
            for repo_data in service.list_repository_data():
                repository = repo_data.get('repository')  # type: Repository
                repo_mgr.delete_repository(repository_id=repository.repository_id)
            service_conn = ServiceConn(path_to_db=self.db_path)
            service_id = service_conn.delete_service(service_id=service.service_id)
            if isinstance(service_id, int):
                return True
            else:
                return False
