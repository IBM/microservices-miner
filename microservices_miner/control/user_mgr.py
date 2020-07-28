# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.control.database_conn import UserConn
from microservices_miner.model.user import User


class UserMgr:

    def __init__(self, path_to_db):
        self.db_conn = UserConn(path_to_db)

    def insert_user(self, user):
        """

        Parameters
        ----------
        user: User

        Returns
        -------
        int
        """
        user_id = self.db_conn.insert_user(user)
        return user_id

    def get_user_from_database(self, login: str, name: str = None, email: str = None) -> User:
        """

        Parameters
        ----------
        login: str
        name: str
        email: str

        Returns
        -------
        User
        """

        user = self.db_conn.get_user(login=login)
        # try to get the user by its name
        if user is None and name is not None:
            user = self.db_conn.get_user(name=name)
        # try to get the user by its email
        if user is None and email is not None:
            user = self.db_conn.get_user(email=email)
        return user

    @staticmethod
    def make_user(email: str, name: str, login: str) -> User:
        """

        Parameters
        ----------
        email: str
        name: str
        login: str

        Returns
        -------
        User
        """
        u = User(name=name, email=email, login=login)
        return u
