# (C) Copyright IBM Corporation 2018
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>


class FileModification:

    def __init__(self, filename, additions, deletions, changes, status):
        """

        Parameters
        ----------
        filename: str
        additions: int
        deletions: int
        """
        self._filename = filename
        assert isinstance(additions, int)
        assert additions >= 0, "Error! additions cannot be negative"
        self._additions = additions
        assert isinstance(deletions, int)
        assert deletions >= 0, "Error! deletions cannot be negative"
        self._deletions = deletions
        self._status = status
        self._changes = changes

    @property
    def filename(self):
        return self._filename

    @property
    def additions(self):
        return self._additions

    @additions.setter
    def additions(self, v):
        assert isinstance(v, int), "Error! additions type is not int"
        assert v >= 0, "Error! additions cannot be negative"
        self._additions = v
        self._changes = self.deletions + self.additions

    @property
    def deletions(self):
        return self._deletions

    @deletions.setter
    def deletions(self, v):
        assert isinstance(v, int), "Error! deletions type is not int"
        assert v >= 0, "Error! deletions cannot be negative"
        self._deletions = v
        self._changes = self.deletions + self.additions

    @property
    def changes(self):
        return self._changes

    @property
    def status(self):
        return self._status

    def __str__(self):
        s = 'FileModification filename={} additions={} deletions={} status={}'\
            .format(self.filename, self.additions, self.deletions, self.status)
        return s