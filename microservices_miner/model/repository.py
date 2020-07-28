# (C) Copyright IBM Corporation 2018
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.model.git_commit import Commit
from microservices_miner.model.issue import Issue
from typing import List


class Repository:

    def __init__(self, name, url):
        self._name = name
        self._url = url
        self._commits = list()
        self._repository_id = None
        self._issues = None
        self._owner = None

    @property
    def url(self):
        return self._url

    @property
    def name(self):
        return self._name

    @property
    def commits(self) -> List[Commit]:
        return sorted(self._commits, key=lambda k: k.date)

    @commits.setter
    def commits(self, value):
        assert isinstance(value, list), "Error! commits is not a type of list: {}".format(value)
        if len(value) > 0:
            assert all(isinstance(x, Commit) for x in value), "Error! Invalid item"
        self._commits = value

    @property
    def repository_id(self):
        return self._repository_id

    @repository_id.setter
    def repository_id(self, value):
        self._repository_id = value

    @property
    def issues(self):
        return self._issues

    @issues.setter
    def issues(self, v):
        assert isinstance(v, list) and all(isinstance(i, Issue) for i in v),\
            "Error! Invalid assignment: v={}".format(v)
        self._issues = v

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, v):
        assert isinstance(v, str), "Error! Owner is not a str: {}".format(v)
        self._owner = v

    def __str__(self):
        s = 'Repository {} {}'.format(self.name, self.repository_id)
        return s

    def __eq__(self, other):

        if self.repository_id == other.repository_id:
            return True
        else:
            return False
