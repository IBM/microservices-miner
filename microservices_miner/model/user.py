# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>


class User:

    def __init__(self, email, name, login):
        """

        Parameters
        ----------
        email: str
        name: str
        login: str
        """
        assert isinstance(email, str), "Error! email is not a str"
        self.email = email
        assert isinstance(name, str), "Error! name is not str"
        self.name = name
        assert isinstance(login, str), "Error! login is not str"
        self.login = login
        self._user_id = None

    @property
    def user_id(self):
        return self._user_id

    @user_id.setter
    def user_id(self, v):
        self._user_id = v

    def __str__(self):
        return 'User name={} login={} email={}'.format(self.name, self.login, self.email)
