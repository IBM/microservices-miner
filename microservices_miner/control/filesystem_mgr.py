# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.control.database_conn import ExtensionsConn, FilenamePatternConn, ServiceExtensionsConn
from typing import Tuple, List
import logging

logging.basicConfig(filename='github_miner.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


class FileSystemMgr:
    INCLUSION = 'inclusion'
    EXCLUSION = 'exclusion'
    EXCLUDING_PATTERNS = ('_BASE_', '_REMOTE_', '_LOCAL_', '_BACKUP_')

    def __init__(self, db_path):
        self.db_path = db_path

    @staticmethod
    def _get_excluding_pattern_for_all_services() -> Tuple:
        """

        Returns
        -------
        Tuple[str]
        """
        return FileSystemMgr.EXCLUDING_PATTERNS

    def get_extensions(self, service_id: int):
        """

        Parameters
        ----------
        service_id: int

        Returns
        -------
        list of str
        """
        ext_conn = ExtensionsConn(self.db_path)
        extensions = ext_conn.get_extensions_by_service(service_id)
        return extensions

    def get_excluding_patterns(self, service_id: int, repository_id: int):
        """

        Parameters
        ----------
        service_id: int
        repository_id: int

        Returns
        -------
        Tuple
        """
        fpc = FilenamePatternConn(self.db_path)
        excluding_patterns = list()
        fixed_excluding_patterns = self._get_excluding_pattern_for_all_services()
        excluding_patterns.extend(list(fixed_excluding_patterns))
        excluding_patterns.extend(fpc.get_patterns(service_id=service_id, repository_id=repository_id,
                                                   pattern_type=FileSystemMgr.EXCLUSION))
        return tuple(excluding_patterns)

    def get_including_patterns(self, service_id: int, repository_id: int):
        """

        Parameters
        ----------
        service_id: int
        repository_id: int

        Returns
        -------
        Tuple
        """
        fpc = FilenamePatternConn(self.db_path)
        including_patterns = list()
        pats = fpc.get_patterns(service_id=service_id, pattern_type=FileSystemMgr.INCLUSION,
                                repository_id=repository_id)
        including_patterns.extend(pats)
        return tuple(including_patterns)

    def insert_extensions(self, service_id: int, programming_languages: List[str]):
        """

        Parameters
        ----------
        service_id: int
        programming_languages: List[str]

        Returns
        -------

        """
        ext_conn = ExtensionsConn(self.db_path)
        serv_ext_conn = ServiceExtensionsConn(self.db_path)
        extensions = ext_conn.list_extensions()
        for lang in programming_languages:
            j = 0
            found = False
            while j < len(extensions) and not found:
                ext = extensions[j]
                j += 1
                language = ext.get('language').lower()
                ext_id = ext.get('id')
                if lang.lower() == language:
                    serv_ext_conn.insert_service_extension(service_id=service_id, extension_id=ext_id)
                    found = True

            if not found:
                logging.warning('The following language has not been inserted into the database: {}'.format(lang))

    def check_extension(self, service_id, filename):
        """
        checks if the filename extension is allowed

        Parameters
        ----------
        filename: str
        service_id: int
        Returns
        -------
        bool
        """

        index = filename.rfind('.') + 1
        if index < 0:
            # print('Ignoring file: {}'.format(filename))
            return False
        extension = filename[index:]
        allowed_extensions = self.get_extensions(service_id=service_id)
        return extension in allowed_extensions

    def check_filename(self, filename, service_id, repository_id):
        """

        Parameters
        ----------
        filename: str
            name of the file
        service_id: int
            ID of the service
        repository_id: int
            name of the repository

        Returns
        -------
        bool
        """
        including_patterns = self.get_including_patterns(service_id=service_id,
                                                         repository_id=repository_id)
        excluding_patterns = self.get_excluding_patterns(service_id=service_id, repository_id=repository_id)
        is_valid = filename is not None and isinstance(filename, str) and \
                   self.check_extension(service_id=service_id, filename=filename) and \
                   all(excluding_pattern not in filename for excluding_pattern in excluding_patterns) and \
                   (len(including_patterns) == 0 or any(pat in filename for pat in including_patterns))
        return is_valid
