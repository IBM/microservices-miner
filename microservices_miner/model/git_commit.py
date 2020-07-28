# (C) Copyright IBM Corporation 2018
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>

from microservices_miner.model.file_modification import FileModification
from datetime import datetime
from typing import List
from microservices_miner.model.user import User


class Commit:

    def __init__(self, date, sha, user=None, comment=None):
        """

        Parameters
        ----------
        date: datetime
        sha: str
        user: User
        """
        assert date is not None, "Error! date is None"
        assert isinstance(date, datetime), "Error! {} is not a type of datetime".format(type(date))
        self._date = date
        self._comment = comment
        assert isinstance(sha, str), "Error! sha is not a str: {}".format(sha)
        self._sha = sha
        self._file_modifications = list()
        self._user = user
        self._commit_id = None
        self._parents = list()

    @property
    def commit_id(self):
        return self._commit_id

    @commit_id.setter
    def commit_id(self, v):
        assert isinstance(v, int), "Error! commit.id={}".format(v)
        self._commit_id = v

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, v):
        """

        Parameters
        ----------
        v: User

        Returns
        -------

        """
        assert v is not None, "Error! parameter is None"
        assert isinstance(v, User), "Error! invalid type of User"
        self._user = v

    @property
    def sha(self):
        return self._sha

    @property
    def date(self):
        return self._date

    @property
    def file_modifications(self) -> List[FileModification]:
        return self._file_modifications

    @file_modifications.setter
    def file_modifications(self, value):
        assert isinstance(value, list)
        for v in value:
            assert isinstance(v, FileModification)
        self._file_modifications = value

    @property
    def comment(self):
        return self._comment

    def __str__(self):
        s = 'Commit {} {}'.format(self.sha, self.date.isoformat())
        return s

    def __eq__(self, other):
        if self.sha == other.sha:
            return True
        else:
            return False
