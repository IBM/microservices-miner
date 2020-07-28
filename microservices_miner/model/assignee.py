# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>


class Assignee:

    def __init__(self, login, htmlurl):
        self.login = login
        self.htmlurl = htmlurl
        self._assignee_id = None

    @property
    def assignee_id(self):
        return self._assignee_id

    @assignee_id.setter
    def assignee_id(self, v):
        self._assignee_id = v
