# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>

from unittest import TestCase
import os
from microservices_miner.control.database_conn import FileModificationConn, RepositoryCommitConn
from microservices_miner.control.filesystem_mgr import FileSystemMgr
import pandas as pd


class TestLOCCounter(TestCase):

    ORACLE_PATH = ''

    def setUp(self):
        self.df = pd.read_csv(TestLOCCounter.ORACLE_PATH, sep=',', index_col=0)
        self.db_path = os.getenv('DB_PATH')

    # todo
    def test_loc_counter(self):
        filemodification_conn = FileModificationConn(path_to_db=self.db_path)
        commit_conn = RepositoryCommitConn(path_to_db=self.db_path)
        shas = self.df.SHA.unique()
        for sha in shas:
            print('SHA={}'.format(sha))
            commit = commit_conn.get_commit_and_its_filemodifications_by_sha(sha=sha)
            if commit is not None:
                filemodifications = filemodification_conn. \
                    get_file_modifications_per_commit(commit=commit, including_patterns=(),
                                                      excluding_patterns=())
                subdf = self.df[self.df.SHA == sha]
                for index, row in subdf.iterrows():
                    additions = row[1]
                    deletions = row[2]
                    filename = row[3]
                    found = False
                    for fm in filemodifications:
                        if fm.filename == filename and fm.additions == additions and fm.deletions == deletions:
                            found = True
                            break
                    if not found:
                        print("Error! Not found : SHA={} additions={} deletions={} filename={}" \
                              .format(sha, additions, deletions, filename))
                    self.assertTrue(found), \
                        "Error! Not found : SHA={} additions={} deletions={} filename={}" \
                        .format(sha, additions, deletions, filename)
        commits = commit_conn.get_commits_by_repo(repository_id=)
        filesystem_mgr = FileSystemMgr(self.db_path)
        excluding_patterns = filesystem_mgr.get_excluding_patterns(service_id=,
                                                                   repository_id=)
        including_patterns = filesystem_mgr.get_including_patterns(service_id=,
                                                                   repository_id=)
        for c in commits:
            if c.sha not in shas:
                filemodifications = filemodification_conn. \
                    get_file_modifications_per_commit(commit=c, excluding_patterns=excluding_patterns,
                                                      including_patterns=including_patterns)
                for fm in filemodifications:
                    if fm.filename.endswith('.py') and 'merge' not in c.comment.lower():
                        print('Error! filename = {} SHA = {} date={}'.format(fm, c.sha, c.date.isoformat()))
