import unittest
from microservices_miner.control.filesystem_mgr import FileSystemMgr
from microservices_miner.control.service_mgr import ServiceMgr
from microservices_miner.control.database_conn import ServiceRepositoryConn
import os
from microservices_miner.model.repository import Repository
import sqlite3


class TestFileSystemMgr(unittest.TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.fs = FileSystemMgr(db_path=self.db_path)

    def test_check_filename(self):
        service_mgr = ServiceMgr(db_path=self.db_path)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for name in service_mgr.list_all_service_names():
            service = service_mgr.get_service(service_name=name)
            for repo_data in service.list_repository_data():
                repo = repo_data.get('repository')  # type: Repository
                sql = 'select * from {} where repository_id = {};'.format(ServiceRepositoryConn.TABLE_NAME,
                                                                          repo.repository_id)
                cursor.execute(sql)
                rows = cursor.fetchall()
                if len(rows) > 1:
                    sql = 'select pattern, type, repository_id from filename_pattern where service_id = {};'\
                        .format(service.service_id)
                    cursor.execute(sql)
                    patterns = cursor.fetchall()
                    for pat in patterns:
                        pattern = pat[0]
                        type = pat[1]
                        repo_id = pat[2]
                        if repo.repository_id == repo_id:
                            for c in repo.commits:
                                for fm in c.file_modifications:
                                    is_valid = self.fs.check_filename(filename=fm.filename,
                                                                      service_id=service.service_id,
                                                                      repository_id=repo.repository_id)
                                    self.assertIsInstance(is_valid, bool)
                                    endswith_py = fm.filename.endswith('.py')
                                    if pattern in fm.filename.lower() and type == FileSystemMgr.INCLUSION:

                                        self.assertTrue(is_valid,
                                                        msg='service = {} filename={} '.format(service.name, fm.filename))
                                        self.assertTrue(endswith_py,
                                                        msg='service = {} filename={} '.format(service.name, fm.filename))
                                    elif pattern in fm.filename and type == FileSystemMgr.EXCLUSION:
                                        self.assertFalse(is_valid,
                                                             msg='service = {} filename={} '.format(service.name, fm.filename))

                            break


if __name__ == '__main__':
    unittest.main()
