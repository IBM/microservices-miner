# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from datetime import datetime
from microservices_miner.model.user import User
import logging

logging.basicConfig(filename='github_miner.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


class Issue:

    def __init__(self, title, body, create_at, state, user, updated_at, closed_at):
        """

        Parameters
        ----------
        title: str
        body: str
        create_at: str
            ISO format
        state: str
        """
        assert user is not None and isinstance(user, User), "Error! Invalid user={}".format(user)
        assert user.user_id is not None, "Error! User ID is None: {}".format(user)
        self._title = title
        self._body = body
        try:
            dt = datetime.strptime(create_at, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            logging.error('Error! created_at={} error={} title={}'.format(create_at, e, title))
            raise e
        self._create_at = dt
        if closed_at is not None:
            try:
                self._closed_at = datetime.strptime(closed_at, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError as e:
                logging.error('Error! closed_at={} error={} title={}'.format(closed_at, e, title))
                raise e
        else:
            self._closed_at = None
        self._assignees = list()
        self._state = state
        self._user = user
        self._labels = list()
        if updated_at is not None:
            self._update_at = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%SZ")
        else:
            self._update_at = None

    @property
    def assignees(self):
        return self._assignees

    @assignees.setter
    def assignees(self, v):
        """

        Parameters
        ----------
        v: list of User

        Returns
        -------

        """
        self._assignees = v

    @property
    def user(self):
        return self._user

    @property
    def title(self):
        return self._title

    @property
    def body(self):
        return self._body

    @property
    def created_at(self):
        """

        Returns
        -------
        datetime
        """
        return self._create_at

    @property
    def state(self):
        return self._state

    @property
    def labels(self):
        return self._labels

    @labels.setter
    def labels(self, v):
        assert isinstance(v, list), "Error! not a list"
        self._labels = v

    @property
    def updated_at(self):
        """

        Returns
        -------
        datetime
        """
        return self._update_at

    @property
    def closed_at(self):
        """

        Returns
        -------
        datetime
        """
        return self._closed_at

    @closed_at.setter
    def closed_at(self, v):
        if v is not None:
            assert isinstance(v, str), "Error! Invalid type for closed_at: type={} value={}".format(type(v), v)
            dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
            self._closed_at = dt

    def __str__(self):
        s = 'Title={} user_id={} labels={}'.format(self.title, self.user.user_id, self.labels)
        return s
