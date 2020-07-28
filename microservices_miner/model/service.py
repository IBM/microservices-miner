# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from datetime import datetime, date
from microservices_miner.model.repository import Repository


class Service:

    def __init__(self, name, start_date_str, end_date_str):
        """

        Parameters
        ----------
        name: str
        start_date_str: str
        end_date_str: str
        """
        self._name = name
        self._service_id = None
        try:
            st = datetime.strptime(start_date_str, '%Y-%m-%d')
            self._start_date = st.date()
        except ValueError as e:
            print('Error! Invalid date: {}'.format(start_date_str))
            raise e
        self.repositories = list()
        try:
            assert end_date_str is not None, "Error"
            ed = datetime.strptime(end_date_str, '%Y-%m-%d')
            self._end_date = ed.date()
        except (ValueError, AssertionError):
            self._end_date = None

    @property
    def name(self):
        return self._name

    @property
    def start_date(self):
        """

        Returns
        -------
        date or None
        """

        return self._start_date

    @property
    def end_date(self):
        """

        Returns
        -------
        date or None
        """
        return self._end_date

    @property
    def service_id(self):
        return self._service_id

    @service_id.setter
    def service_id(self, v):
        assert isinstance(v, int), "Error! invalid Id={}".format(v)
        self._service_id = v

    def add_repository(self, repository: Repository, start_date: str, end_date: str, initial_loc: int):
        """

        Parameters
        ----------
        repository: Repository
        start_date: str
        end_date: str
        initial_loc: int

        Returns
        -------

        """
        assert isinstance(repository, Repository), "Error! repository is not a type of Repository"
        if start_date is not None:
            datetime.strptime(start_date, '%Y-%m-%d')
        if end_date is not None:
            try:
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError as e:
                print('Repository: {} start_date={} end_date={}'.format(repository, start_date, end_date))
                print(e)
                raise e

        d = {'repository': repository, 'start_date': start_date, 'end_date': end_date, 'initial_loc': initial_loc}
        self.repositories.append(d)

    def get_repository(self, repository_name):
        """

        Parameters
        ----------
        repository_name

        Returns
        -------
        Repository, str, str
        """
        for d in self.repositories:
            repo = d.get('repository')  # type: Repository
            if repo.name == repository_name:
                start_date = d.get('start_date')
                end_date = d.get('end_date')
                return repo, start_date, end_date
        return None, None, None

    def list_repository_data(self):
        """

        Returns
        -------
        List[dict]
        """
        return self.repositories
