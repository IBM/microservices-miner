# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from unittest import TestCase
from microservices_miner.control.database_conn import ServiceRepositoryConn, ServiceConn, RepositoryCommitConn,\
    ExtensionsConn, UserConn, RepositoryConn, FilenamePatternConn
import os
from microservices_miner.model.repository import Repository
from microservices_miner.model.service import Service
from microservices_miner.model.git_commit import Commit
from microservices_miner.model.user import User
from datetime import datetime, date
import pytest
import random
import sqlite3


class TestServiceRepositoryConn(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.conn = ServiceRepositoryConn(path_to_db=self.db_path)

    def test_get_service_repository(self):
        cursor = self.conn.conn.cursor()
        sql = 'select name, ID from {};'.format(ServiceConn.TABLE_NAME)
        cursor.execute(sql)
        rows = cursor.fetchmany()
        for row in rows:
            name = row[0]
            sid = row[1]
            service_repo_list = self.conn.get_service_repository(service_name=name)
            self.assertIsInstance(service_repo_list, list)
            for sr in service_repo_list:
                self.assertIsInstance(sr, dict)
                repo_id = sr.get('repository_id')
                self.assertIsInstance(repo_id, int)
                service_id = sr.get('service_id')
                self.assertIsInstance(service_id, int)
                self.assertEqual(service_id, sid)

    def test_get_service_repository_invalid_service_name(self):
        name = 'fdsfldskfjlsdjf'
        service_repo_list = self.conn.get_service_repository(service_name=name)
        self.assertIsInstance(service_repo_list, list)
        self.assertEquals(len(service_repo_list), 0)


class TestRepositoryConn(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.conn = RepositoryConn(path_to_db=self.db_path)

    def test_get_repository_by_sha(self):
        commit_conn = RepositoryCommitConn(path_to_db=self.db_path)
        commits = list()
        repo_id = 0
        while len(commits) == 0:
            repo_id += 1
            commits = commit_conn.get_commits_by_repo(repository_id=repo_id)
        for c in random.sample(commits, 10):
            repo = self.conn.get_repository_by_sha(sha=c.sha)
            self.assertEqual(repo.repository_id, repo_id)

    def test_get_repository(self):
        repos = self.conn.list_repositories()
        for r in repos:

            copy_repo = self.conn.get_repository(repo_id=r.repository_id)
            self.assertEqual(r.repository_id, copy_repo.repository_id)
            self.assertEqual(r.name, copy_repo.name)

    def test_get_repository_by_url(self):
        gen_conn = sqlite3.connect(self.db_path)
        cursor = gen_conn.cursor()
        sql = 'select url from repository;'
        cursor.execute(sql)
        rows = cursor.fetchall()
        found = False
        for row in rows:
            url = row[0]
            copy_repo = self.conn.get_repository_by_url(url=url)
            self.assertIsInstance(copy_repo, Repository)
            self.assertEqual(url, copy_repo.url)
            found = True
        self.assertTrue(found)


class TestServiceConn(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.conn = ServiceConn(path_to_db=self.db_path)

    def test_get_service(self):
        cursor = self.conn.conn.cursor()
        sql = 'select name, ID from {};'.format(ServiceConn.TABLE_NAME)
        rows = cursor.execute(sql)
        for row in rows:
            name = row[0]
            sid = row[1]
            s = self.conn.get_service(name=name)
            self.assertIsInstance(s, Service)
            self.assertEqual(s.service_id, sid)
            self.assertEqual(s.name, name)

    def test_get_service_invalid_parameters(self):
        name = 'fdsfdsfd'
        s = self.conn.get_service(name=name)
        self.assertIsNone(s)

    def test_insert(self):
        start_date = date(year=2010, month=1, day=1)
        sid = self.conn.insert_service(name='test_service_name', start_date=start_date)
        self.assertIsInstance(sid, int)
        self.assertGreater(sid, 0)
        self.conn.delete_service(sid)

    def test_insert_existing_service(self):

        service_names = self.conn.list_all_service_names()
        name = random.choice(service_names)
        s = self.conn.get_service(name=name)
        self.assertIsInstance(s, Service)

        sid = self.conn.insert_service(name=s.name, start_date=s.start_date)
        self.assertEqual(s.service_id, sid)

    def test_insert_invalid_parameters(self):
        start_date = date(year=2010, month=1, day=1)
        with pytest.raises(Exception) as e_info:
            self.conn.insert_service(name=None, start_date=start_date)
        self.assertIn('AssertionError', e_info.typename)

        with pytest.raises(Exception) as e_info:
            self.conn.insert_service(name='test', start_date=None)
        self.assertIn('AssertionError', e_info.typename)

    def test_delete(self):
        test_name = 'test_service_name'
        start_date = date(year=2010, month=1, day=1)
        s = self.conn.get_service(test_name)
        self.assertIsNone(s)
        sid = self.conn.insert_service(name=test_name, start_date=start_date)
        self.assertIsInstance(sid, int)
        self.assertGreater(sid, 0)
        s = self.conn.get_service(test_name)
        self.conn.delete_service(s.service_id)
        s = self.conn.get_service(test_name)
        self.assertIsNone(s)

    def test_update_service(self):
        service_names = self.conn.list_all_service_names()
        name = random.choice(service_names)
        s = self.conn.get_service(name=name)
        self.assertIsNotNone(s)
        self.assertIsInstance(s, Service)
        resp = self.conn.update_service(start_date=s.start_date, service_id=s.service_id, end_date=s.end_date)
        self.assertTrue(resp)


class TestRepositoryCommitConn(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.conn = RepositoryCommitConn(path_to_db=self.db_path)

    def test_insert_repository_commit(self):
        u = User(email='tester@ibm.com', name='test-name', login='tester')
        dt = datetime(year=2019, month=1, day=1)
        commit = Commit(date=dt, sha='test', user=u)
        repository_id = 3432432
        user_id = 124397
        commit_id = self.conn.insert_repository_commit(commit=commit, repository_id=repository_id, user_id=user_id)
        self.assertIsInstance(commit_id, int)
        self.conn.delete_commit(commit_id)
        commits = self.conn.get_commits_by_repo(repository_id=repository_id)
        self.assertEquals(len(commits), 0)

    def test_get_commits_by_repo(self):
        repository_id = 1
        commits = self.conn.get_commits_by_repo(repository_id=repository_id)
        self.assertTrue(all(isinstance(c, Commit) for c in commits))

    def test_get_inconsistent_commits(self):
        self.conn.get_inconsistent_commits(repository_id=1, extensions=['py'])


class TestExtensionConn(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.conn = ExtensionsConn(path_to_db=self.db_path)

    def test_get_extensions_by_service(self):
        gen_conn = sqlite3.connect(self.db_path)
        cursor = gen_conn.cursor()
        sql = 'select id from service;'
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            exts = self.conn.get_extensions_by_service(service_id=row[0])
            self.assertIsInstance(exts, list)
            self.assertTrue(all(isinstance(x, str) for x in exts))


class TestFilenamePatternConn(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.conn = FilenamePatternConn(path_to_db=self.db_path)

    def test_get_pattern(self):
        cursor = self.conn.conn.cursor()
        sql = 'select service_id, repository_id, type, pattern from {};'.format(FilenamePatternConn.TABLE_NAME)
        rows = cursor.execute(sql)
        for row in rows:
            service_id = row[0]
            repository_id = row[1]
            pat_type = row[2]
            pattern = row[3]
            patterns = self.conn.get_patterns(service_id=service_id, repository_id=repository_id, pattern_type=pat_type)
            self.assertIsInstance(patterns, list)
            self.assertTrue(all(isinstance(s, str) for s in patterns))
            self.assertGreater(len(patterns), 0)


class TestUserConn(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.conn = UserConn(path_to_db=self.db_path)

    def test_get_user(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        sql = 'select name, id, login, email from user;'
        cursor.execute(sql)
        rows = cursor.fetchall()
        self.assertIsNotNone(rows)
        found_at_least_one = False
        for row in rows:
            found_at_least_one = True
            name = row[0]
            user = self.conn.get_user(name=name)
            self.assertIsInstance(user, User)
            self.assertEqual(user.name, name)

            user_id = row[1]
            user = self.conn.get_user(user_id=user_id)
            self.assertIsInstance(user, User)
            self.assertEqual(user_id, user.user_id)

            login = row[2]
            user = self.conn.get_user(login=login)
            self.assertIsInstance(user, User)
            self.assertEqual(user.login, login)

            email = row[3]
            user = self.conn.get_user(email=email)
            self.assertIsInstance(user, User)
            self.assertEqual(user.email, email)
        self.assertTrue(found_at_least_one)

    def test_get_user_invalid(self):
        user = self.conn.get_user(name='fdsfdsf')
        self.assertIsNone(user)

        user = self.conn.get_user(user_id=999999999)
        self.assertIsNone(user)
