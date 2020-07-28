# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>


class Label:

    def __init__(self, name, description):
        self.name = name
        self.description = description
        self._label_id = None

    @property
    def label_id(self):
        return self._label_id

    @label_id.setter
    def label_id(self, v):
        self._label_id = v