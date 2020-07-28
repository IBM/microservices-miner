import unittest
from microservices_miner.control.service_mgr import ServiceMgr
from microservices_miner.control.filesystem_mgr import FileSystemMgr
import os
from microservices_miner.model.service import Service
from microservices_miner.model.repository import Repository
from microservices_miner.control.database_conn import ServiceRepositoryConn, RepositoryCommitConn, FileModificationConn
import sqlite3


class TestServiceMgr(unittest.TestCase):

    def setUp(self) -> None:
        db_path = os.getenv('DB_PATH')
        self.service_mgr = ServiceMgr(db_path=db_path)
        self.conn = sqlite3.connect(db_path)

    def test_get_service_by_name(self):

        cursor = self.conn.cursor()
        service_names = self.service_mgr.list_all_service_names()
        filesystem_mgr = FileSystemMgr(db_path=os.getenv('DB_PATH'))
        for service_name in service_names:
            service = self.service_mgr.get_service(service_name=service_name)

            count_repo_sql = 'select count(*) from {} where service_id = {};'.format(ServiceRepositoryConn.TABLE_NAME,
                                                                                     service.service_id)
            cursor.execute(count_repo_sql)
            row = cursor.fetchone()
            total_number_of_repositories = row[0]

            self.assertIsInstance(service, Service)
            self.assertTrue(len(service.list_repository_data()) == total_number_of_repositories > 0,
                            msg='Error! Invalid number of services')
            for repo_data in service.list_repository_data():
                repo = repo_data.get('repository')
                count_commits_sql = 'select count(*) from {} where repository_id = {};' \
                    .format(RepositoryCommitConn.TABLE_NAME, repo.repository_id)
                cursor.execute(count_commits_sql)
                self.assertIsInstance(repo, Repository)
                for c in repo.commits:

                    filename_sql = 'select filename from {} where commit_id = {};' \
                        .format(FileModificationConn.TABLE_NAME, c.commit_id)
                    cursor.execute(filename_sql)
                    rows = cursor.fetchall()
                    counter = 0
                    for row in rows:
                        filename = row[0]
                        if filesystem_mgr.check_filename(filename=filename, service_id=service.service_id,
                                                         repository_id=repo.repository_id):

                            counter += 1
                    filemodication_counter = len(c.file_modifications)
                    self.assertEqual(counter, filemodication_counter,
                                     msg='Different number of file modifications')

