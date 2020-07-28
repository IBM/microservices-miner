# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>

import sqlite3
from microservices_miner.model.user import User
from microservices_miner.model.label import Label
from microservices_miner.model.assignee import Assignee
from microservices_miner.model.git_commit import Commit
from microservices_miner.model.repository import Repository
from microservices_miner.model.file_modification import FileModification
from microservices_miner.model.issue import Issue
from datetime import datetime, date
from microservices_miner.model.service import Service
from typing import List, Tuple
import logging
logging.basicConfig(filename='github_miner.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


class UserConn:

    def __init__(self, path_to_db):
        self.conn = sqlite3.connect(path_to_db)

    def insert_user(self, user):
        """

        Parameters
        ----------
        user: User

        Returns
        -------
        int
        """
        cursor = self.conn.cursor()
        sql = 'select ID from user where name="{}" and email="{}";'.format(user.name, user.email)
        cur = cursor.execute(sql)
        row = cur.fetchone()
        if row is None:
            sql = 'insert into user(name, email, login) values ("{}", "{}", "{}");'.format(user.name, user.email,
                                                                                           user.login)
            cursor.execute(sql)
            last_row_id = cursor.lastrowid
            self.conn.commit()
            logging.info('The following SQL query was executed: {}'.format(sql))
        else:
            last_row_id = row[0]
        cursor.close()
        return last_row_id

    def get_user(self, name=None, email=None, login=None, user_id=None):
        """

        Parameters
        ----------
        name: str
        email: str
        user_id: int
        login: str

        Returns
        -------
        User
        """
        cursor = self.conn.cursor()
        if name is not None and email is not None:
            sql = 'select name, email, ID, login from user where name=="{}" and email=="{}";'.format(name, email)
        elif name is not None:
            sql = 'select name, email, ID, login from user where name=="{}";'.format(name)
        elif user_id is not None:
            sql = 'select name, email, ID, login from user where ID=={};'.format(user_id)
        elif login is not None:
            sql = 'select name, email, ID, login from user where login=="{}";'.format(login)
        else:
            sql = 'select name, email, ID, login from user where email=="{}";'.format(email)
        try:
            cur = cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.critical('Error! {} sql={}'.format(e, sql))
            raise e
        row = cur.fetchone()
        if row is not None:
            user = User(name=row[0], email=row[1], login=row[3])
            user.user_id = row[2]
        else:
            user = None
        return user


class ParentCommitConn:

    TABLE_NAME = 'parentcommit'

    def __init__(self, path_to_db):

        self.conn = sqlite3.connect(path_to_db)
        self.db_path = path_to_db

    def insert_repository_commit(self, sha, position):
        """

        Parameters
        ----------
        sha: str
        position: int

        Returns
        -------
        int

        """
        cursor = self.conn.cursor()
        sql = 'select id from {} where sha == "{}" and position = {};' \
            .format(ParentCommitConn.TABLE_NAME, sha, position)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(sha, position) values ("{}", {});' \
                .format(ParentCommitConn.TABLE_NAME, sha, position)
            cursor.execute(sql)
            last_row_id = cursor.lastrowid
            self.conn.commit()
            logging.info('The following SQL query was executed: {}'.format(sql))
        else:
            last_row_id = row[0]
        cursor.close()
        return last_row_id

    def get_parent_commit_sha(self, child_commit_id):
        """
        gets the SHA of the parent commit given a child commit ID

        Parameters
        ----------
        child_commit_id: int

        Returns
        -------
        str or None
        """
        sql = 'select sha from {0} join {1} on {0}.ID == {1}.parentcommit_id where {1}.repocommit_id == {2} ' \
              'and {0}.position = 0;' \
            .format(ParentCommitConn.TABLE_NAME, ParentCommitRepoCommitConn.TABLE_NAME, child_commit_id)
        cursor = self.conn.cursor()
        # logging.info('The following SQL query was executed: {}'.format(sql))
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is not None:
            sha = row[0]
        else:
            sha = None
        return sha

    def get_parent_commit_id(self, sha):
        """
        gets a list of parent commits SHA that have position equals to zero

        Parameters
        ----------
        sha: str

        Returns
        -------
        int or None
        """
        sql = 'select ID from {0} where sha == "{1}";' \
            .format(ParentCommitConn.TABLE_NAME, ParentCommitRepoCommitConn.TABLE_NAME, sha)
        cursor = self.conn.cursor()
        logging.info('The following SQL query was executed: {}'.format(sql))
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is not None:
            parent_commit_id = row[0]
        else:
            parent_commit_id = None
        return parent_commit_id

    def get_children_commit_shas(self, parent_commit_sha):
        """

        Parameters
        ----------
        parent_commit_sha: str

        Returns
        -------
        set
        """
        sql = 'select ID from {} where sha == "{}"'.format(ParentCommitConn.TABLE_NAME, parent_commit_sha)
        cursor = self.conn.cursor()
        logging.info('The following SQL query was executed: {}'.format(sql))
        cursor.execute(sql)
        row = cursor.fetchone()
        repocommit_ids = set()
        if row is not None:
            parent_id = row[0]
            sql = 'select repocommit_id from {} where parentcommit_id == {}' \
                .format(ParentCommitRepoCommitConn.TABLE_NAME, parent_id)
            cursor = self.conn.cursor()
            logging.info('The following SQL query was executed: {}'.format(sql))
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                repocommit_ids.add(row[0])

        return repocommit_ids

    def delete_parent_commit(self, parent_commit_id):
        """

        Parameters
        ----------
        parent_commit_id: str

        Returns
        -------

        """
        sql = 'delete from {} where ID == {}'.format(ParentCommitConn.TABLE_NAME, parent_commit_id)
        cursor = self.conn.cursor()
        logging.info('The following SQL query was executed: {}'.format(sql))
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            print('sql={} error={}'.format(sql, e))
            logging.error(e)
        self.conn.commit()


class ParentCommitRepoCommitConn:

    TABLE_NAME = 'parentcommit_repocommit'

    def __init__(self, path_to_db):

        self.conn = sqlite3.connect(path_to_db)
        self.db_path = path_to_db

    def insert_parent_commit_repository_commit(self, parent_commit_id, repo_commit_id):
        """

        Parameters
        ----------
        parent_commit_id: int
        repo_commit_id: int

        Returns
        -------
        int

        """
        cursor = self.conn.cursor()
        sql = 'select * from {} where repocommit_id == {} and parentcommit_id == {};' \
            .format(ParentCommitRepoCommitConn.TABLE_NAME, parent_commit_id, repo_commit_id)
        logging.info('The following SQL query was executed: {}'.format(sql))
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(repocommit_id, parentcommit_id) values ({}, {});' \
                .format(ParentCommitRepoCommitConn.TABLE_NAME, repo_commit_id, parent_commit_id)
            cursor.execute(sql)
            last_row_id = cursor.lastrowid
            self.conn.commit()

        else:
            last_row_id = row[0]
        cursor.close()
        return last_row_id

    def delete_parent_commit_repocommit(self, parent_commit_id):
        """

        Parameters
        ----------
        parent_commit_id: str

        Returns
        -------

        """
        sql = 'delete from {} where parentcommit_id == {}'.format(ParentCommitRepoCommitConn.TABLE_NAME,
                                                                  parent_commit_id)
        cursor = self.conn.cursor()
        logging.info('The following SQL query was executed: {}'.format(sql))
        cursor.execute(sql)
        self.conn.commit()


class RepositoryCommitConn:

    TABLE_NAME = 'repocommit'

    def __init__(self, path_to_db):

        self.conn = sqlite3.connect(path_to_db)
        self.db_path = path_to_db

    def insert_repository_commit(self, commit, repository_id, user_id):
        """

        Parameters
        ----------
        commit: Commit
        repository_id: int
        user_id: int

        Returns
        -------
        int

        """
        cursor = self.conn.cursor()
        sql = 'select id from {} where sha == "{}" and repository_id == {};' \
            .format(RepositoryCommitConn.TABLE_NAME, commit.sha, repository_id)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! sql={} msg={}'.format(sql, e))
            raise e
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(repository_id, user_id, date, sha, comment) values ("{}", "{}", "{}", "{}", "{}");' \
                .format(RepositoryCommitConn.TABLE_NAME, repository_id, user_id, commit.date.isoformat(), commit.sha,
                        commit.comment)
            cursor.execute(sql)
            last_row_id = cursor.lastrowid
            self.conn.commit()
            logging.info('The following SQL query was executed: {}'.format(sql))
        else:
            last_row_id = row[0]
        cursor.close()
        return last_row_id

    def get_commits_by_repo(self, repository_id, start_date=None, end_date=None):
        """
        gets a list of Commit objects given a repository specified by repository_id and between start and end dates

        Parameters
        ----------
        repository_id: int
        start_date: str
        end_date: str

        Returns
        -------
        list of Commit
        """
        cursor = self.conn.cursor()
        if start_date is not None or end_date is not None:
            if start_date is None:
                start_date = '1000-01-01'
            if end_date is None:
                end_date = '9999-12-31'

            sql = 'select date, sha, user_id, id, comment from {} where repository_id == {} and ' \
                  'date between date("{}") and date("{}");' \
                .format(RepositoryCommitConn.TABLE_NAME, repository_id, start_date, end_date)
        else:
            sql = 'select date, sha, user_id, id, comment from {} where repository_id == {};' \
                .format(RepositoryCommitConn.TABLE_NAME, repository_id)

        rows = cursor.execute(sql)
        commits = list()
        user_conn = UserConn(path_to_db=self.db_path)
        for row in rows:
            dt = row[0]
            if 'T' not in dt:
                dt = dt.replace(' ', 'T')
            commit_date = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
            c = Commit(date=commit_date, sha=row[1], comment=row[4])
            user = user_conn.get_user(user_id=row[2])
            c.user = user
            c.commit_id = row[3]
            commits.append(c)

        return commits

    def get_commit_and_its_filemodifications_by_sha(self, sha, start_date=None, end_date=None):
        """
        gets a commit object by its SHA; otherwise returns None

        Parameters
        ----------
        sha: str
        start_date: str
        end_date: str

        Returns
        -------
        Commit
        """
        cursor = self.conn.cursor()
        start_sql = 'select date, sha, user_id, {0}.ID, comment, filename, additions, deletions, changes, status from' \
                    ' {0} join {1} on {0}.ID == {1}.commit_id where sha == "{2}"' \
            .format(RepositoryCommitConn.TABLE_NAME, FileModificationConn.TABLE_NAME, sha)
        if start_date is None and end_date is None:
            sql = start_sql
        elif start_date is not None and end_date is not None:
            sql = '{0} and date("{1}") >= date(date) and date(date) < date("{2}");' \
                .format(start_sql, start_date, end_date)
        elif start_date is not None:
            sql = '{} and date("{}") >= date(date);' \
                .format(start_sql, start_date)
        else:
            sql = '{} and date(date) < date("{}");' \
                .format(start_sql, end_date)
        cursor.execute(sql)
        rows = cursor.fetchall()
        user_conn = UserConn(path_to_db=self.db_path)
        commit = None
        filemodifications = list()
        for index, row in enumerate(rows):
            if index == 0:
                dt = row[0]
                if 'T' not in dt:
                    dt = dt.replace(' ', 'T')
                commit_date = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
                commit = Commit(date=commit_date, sha=row[1], comment=row[4])
                user = user_conn.get_user(user_id=row[2])
                commit.user = user
                commit.commit_id = row[3]
            fm = FileModification(filename=row[5], additions=row[6], deletions=row[7], changes=row[8], status=row[9])

            filemodifications.append(fm)

        if commit is not None:
            commit.file_modifications = filemodifications
        else:
            # if this commits has no file modifications
            sql = 'select date, sha, user_id, id, comment from {} where sha == "{}"' \
                .format(RepositoryCommitConn.TABLE_NAME, sha)
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is not None:
                dt = row[0]
                if 'T' not in dt:
                    dt = dt.replace(' ', 'T')
                commit_date = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
                commit = Commit(date=commit_date, sha=row[1], comment=row[4])
                user = user_conn.get_user(user_id=row[2])
                commit.user = user
                commit.commit_id = row[3]

        return commit

    def get_inconsistent_commits(self, repository_id, extensions):
        """

        Parameters
        ----------
        repository_id: int
        extensions: list of str

        Returns
        -------
        list of Commit
        """
        cursor = self.conn.cursor()
        s = '('
        for ext in extensions:
            s += 'filename like "%.{}" or '.format(ext)
        # remove the last and
        index = s.rfind('"') + 1
        s = s[:index]
        s += ')'
        sql = 'select date, sha, user_id, {0}.ID, comment, filename, additions, deletions, changes, status ' \
              ' from {0} join {1} on {0}.ID == {1}.commit_id where repository_id == {2} and ' \
              '( (status == "modified" and changes == 0) or (status == "added" and additions == 0) or ' \
              '(status == "removed" and deletions == 0) ) and {3};' \
            .format(RepositoryCommitConn.TABLE_NAME, FileModificationConn.TABLE_NAME, repository_id, s)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.critical('Error! sql={} e={}'.format(sql, e))
        rows = cursor.fetchall()
        user_conn = UserConn(path_to_db=self.db_path)
        commits = list()
        filemodifications = dict()
        for index, row in enumerate(rows):

            dt = row[0]
            if 'T' not in dt:
                dt = dt.replace(' ', 'T')
            commit_date = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
            sha = row[1]
            commit = Commit(date=commit_date, sha=sha, comment=row[4])
            if commit not in commits:
                user = user_conn.get_user(user_id=row[2])
                commit.user = user
                commit.commit_id = row[3]
                commits.append(commit)
            fm = FileModification(filename=row[5], additions=row[6], deletions=row[7], changes=row[8], status=row[9])
            if sha not in filemodifications.keys():
                filemodifications[sha] = [fm]
            else:
                filemodifications[sha].append(fm)
        for commit in commits:
            commit.file_modifications = filemodifications[commit.sha]
        return commits

    def delete_commit(self, commit_id):
        """

        Returns
        -------
        Commit
        """
        cursor = self.conn.cursor()
        sql = 'delete from {} where id == {};' \
            .format(RepositoryCommitConn.TABLE_NAME, commit_id)
        # logging.info('sql={}'.format(sql))
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()
        print('Deleted commits, query={}'.format(sql))
        return


class RepositoryConn:

    TABLE_NAME = 'repository'

    def __init__(self, path_to_db):
        self.conn = sqlite3.connect(path_to_db)

    def insert_repository(self, repository):
        """

        Parameters
        ----------
        repository: Repository

        Returns
        -------
        int
        """
        cursor = self.conn.cursor()

        sql = 'insert into {}(name, url) values ("{}", "{}");' \
            .format(RepositoryConn.TABLE_NAME, repository.name, repository.url)
        cursor.execute(sql)
        last_row_id = cursor.lastrowid
        self.conn.commit()
        logging.info('The following SQL query was executed: {}'.format(sql))

        cursor.close()
        return last_row_id

    def get_repository(self, repo_id: int = None):
        """

        Parameters
        ----------
        repo_id: int
            repository ID

        Returns
        -------
        Repository
        """
        assert repo_id is not None, "Error! Both parameters cannot be none"
        cursor = self.conn.cursor()
        sql = 'select name, url, ID from {0} where {0}.id = {1}' \
            .format(RepositoryConn.TABLE_NAME, repo_id)

        cursor.execute(sql)
        row = cursor.fetchone()
        if row is not None:
            assert row is not None, "Error! sql={}".format(sql)
            repo = Repository(name=row[0], url=row[1])
            repo.repository_id = row[2]
            assert repo.repository_id is not None, "Error! Invalid Repository object: {}".format(repo)
        else:
            repo = None
        return repo

    def get_repository_by_url(self, url: str):
        """

        Parameters
        ----------
        url: str
            repository URL

        Returns
        -------
        Repository
        """
        assert url is not None, "Error! Both parameters cannot be none"
        cursor = self.conn.cursor()

        sql = 'select name, ID from {0} where {0}.url = "{1}"' \
            .format(RepositoryConn.TABLE_NAME, url)

        cursor.execute(sql)
        row = cursor.fetchone()
        if row is not None:
            assert row is not None, "Error! sql={}".format(sql)
            repo = Repository(name=row[0], url=url)
            repo.repository_id = row[1]
            assert repo.repository_id is not None, "Error! Invalid Repository object: {}".format(repo)
        else:
            repo = None
        return repo

    def get_repository_by_sha(self, sha):
        """

        Parameters
        ----------
        sha: str

        Returns
        -------
        Repository
        """
        cursor = self.conn.cursor()
        sql = 'select name, url, {0}.ID from {0} join {1} on {0}.id = {1}.repository_id where sha == "{2}";' \
            .format(RepositoryConn.TABLE_NAME, RepositoryCommitConn.TABLE_NAME, sha)

        cursor.execute(sql)
        row = cursor.fetchone()
        if row is not None:
            assert row is not None, "Error! sql={}".format(sql)
            repo = Repository(name=row[0], url=row[1])
            repo.repository_id = row[2]
            assert repo.repository_id is not None, "Error! Invalid Repository object: {}".format(repo)
        else:
            repo = None
        return repo

    def list_repositories(self):
        """

        Returns
        -------
        list of Repository
        """
        cursor = self.conn.cursor()
        sql = 'select name, url, ID from {};'.format(RepositoryConn.TABLE_NAME)
        cursor.execute(sql)
        rows = cursor.fetchall()
        repos = list()
        for row in rows:
            assert row is not None, "Error! sql={}".format(sql)
            repo = Repository(name=row[0], url=row[1])
            repo.repository_id = row[2]
            repos.append(repo)
        return repos

    def delete_repository(self, repository_id=None, repository_name=None):
        """

        :param repository_id: int
        :param repository_name: str
        :return:
        """
        assert (repository_name is not None) or (repository_id is not None), \
            "Error! Both repository_id and repository_name are None"

        cursor = self.conn.cursor()
        if repository_id is not None:
            sql = 'delete from {} where ID == {};'.format(RepositoryConn.TABLE_NAME, repository_id)
        else:
            sql = 'delete from {} where name == "{}";'.format(RepositoryConn.TABLE_NAME, repository_name)
        cursor.execute(sql)
        self.conn.commit()


class FileModificationConn:

    TABLE_NAME = 'filemodification'

    def __init__(self, path_to_db):
        self.conn = sqlite3.connect(path_to_db)

    def insert_file_modification(self, commit_id, fm):
        """

        Parameters
        ----------
        fm: FileModification
        commit_id: int

        Returns
        -------

        """
        cursor = self.conn.cursor()
        sql = 'select * from {} where commit_id == {} AND additions == {} AND changes == {} AND deletions == {} AND ' \
              'filename == "{}";'.format(FileModificationConn.TABLE_NAME, commit_id, fm.additions, fm.changes,
                                         fm.deletions, fm.filename)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(filename, changes, additions, deletions, status, commit_id) values ("{}", {}, {}, {}, "{}",' \
                  ' {});' \
                .format(FileModificationConn.TABLE_NAME, fm.filename, fm.changes, fm.additions, fm.deletions, fm.status,
                        commit_id)
            logging.info(sql)
            cursor.execute(sql)
            self.conn.commit()
        cursor.close()

    def delete_file_modifications(self, commit_id):
        """

        Parameters
        ----------
        commit_id

        Returns
        -------

        """
        cursor = self.conn.cursor()
        sql = 'delete from {} where commit_id == {};' \
            .format(FileModificationConn.TABLE_NAME, commit_id)
        # logging.info('sql={}'.format(sql))
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()
        print('Deleted file modifications, query={}'.format(sql))
        return

    def get_file_modification_by_name_and_date(self, filename, until_date, repository_id):
        """

        Parameters
        ----------
        filename: str
            pattern of the filename
        until_date: str
            date in isoformat
        repository_id: int
        Returns
        -------

        """
        file_modifications = list()
        cursor = self.conn.cursor()
        sql = 'select * from {0} join {1} on {1}.id == {0}.commit_id ' \
              'where date({1}.date) < date("{2}") and {1}.repository_id == {3} ' \
              'and {0}.filename == "{4}";' \
            .format(FileModificationConn.TABLE_NAME, RepositoryCommitConn.TABLE_NAME, until_date, repository_id,
                    filename)
        # logging.info('sql={}'.format(sql))
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            fm = FileModification(filename=row[0], changes=row[1], additions=row[2], deletions=row[3],
                                  status=row[4])
            file_modifications.append(fm)
        return file_modifications

    def update_filemodification(self, filemodification, commit_id):
        """

        Parameters
        ----------
        filemodification: FileModification
        commit_id: int

        Returns
        -------

        """
        cursor = self.conn.cursor()
        sql = 'update {} set changes={}, deletions={}, additions={} where filename="{}" and commit_id={}' \
            .format(FileModificationConn.TABLE_NAME, filemodification.changes, filemodification.deletions,
                    filemodification.additions, filemodification.filename, commit_id)
        logging.info('updating: {}'.format(sql))
        cursor.execute(sql)
        self.conn.commit()
        return

    def get_file_modifications_per_commit(self, commit: Commit, excluding_patterns: Tuple = (),
                                          including_patterns: Tuple = ()) -> List[FileModification]:
        """

        Parameters
        ----------
        commit: Commit
        excluding_patterns: tuple of str
            if the pattern is anywhere in the filename, the FileModification object is ignored
        including_patterns: tuple of str
        Returns
        -------
        list of FileModification
        """
        file_modifications = list()
        cursor = self.conn.cursor()
        sql = 'select filename, changes, additions, deletions, status from {} where commit_id == {};' \
            .format(FileModificationConn.TABLE_NAME, commit.commit_id)

        cursor.execute(sql)
        rows = cursor.fetchall()

        for row in rows:
            fm = FileModification(filename=row[0], changes=row[1], additions=row[2], deletions=row[3],
                                  status=row[4])
            if all(exc_pat not in fm.filename for exc_pat in excluding_patterns) or \
                any(inc_pat in fm.filename for inc_pat in including_patterns) or \
                len(including_patterns) == len(excluding_patterns) == 0:
                file_modifications.append(fm)

        return file_modifications

    def get_file_modification(self, filename, commit_id):
        """

        Parameters
        ----------
        filename: str
        commit_id: int

        Returns
        -------
        FileModification
        """
        cursor = self.conn.cursor()
        sql = 'select filename, changes, additions, deletions, status from {} where commit_id == {} and filename == "{}";' \
            .format(FileModificationConn.TABLE_NAME, commit_id, filename)
        print(sql)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is not None:
            fm = FileModification(filename=row[0], changes=row[1], additions=row[2], deletions=row[3],
                                  status=row[4])
        else:
            fm = None
        return fm


class IssueConn:

    ISSUE_TABLE_NAME = 'issue'
    ISSUELABEL_TABLE_NAME = 'issuelabel'
    LABEL_TABLE_NAME = 'label'
    ASSIGNEE_TABLE_NAME = 'assignee'
    ISSUEASSIGNEE_TABLE_NAME = 'issueassignee'

    def __init__(self, path_to_db):
        self.db_path = path_to_db
        self.conn = sqlite3.connect(path_to_db)

    def insert_issue(self, title, body, created_at, closed_at, updated_at, repository_id, user_id, state):
        """

        Parameters
        ----------
        title: str
        body: str
        created_at: str
            ISO format
        closed_at: str
            ISO format
        updated_at: str
            ISO format
        repository_id: int
        user_id: int
        state: str

        Returns
        -------
        int
        """
        cursor = self.conn.cursor()
        sql = 'select ID from {} where created_at == "{}" AND repository_id == {} AND user_id == {} AND title == "{}";' \
            .format(IssueConn.ISSUE_TABLE_NAME, created_at, repository_id, user_id, title)
        # logging.info('sql={}'.format(sql))
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(title, body, created_at, closed_at, updated_at, repository_id, user_id, state)' \
                  ' values (?, ?, ?, ?, ?, ?, ?, ?);'.format(IssueConn.ISSUE_TABLE_NAME)
            values = (title, body, created_at, closed_at, updated_at, repository_id, user_id, state)
            # logging.info('Inserting issue: {}'.format(sql))
            cursor.execute(sql, values)
            last_row_id = cursor.lastrowid
            logging.info('The following sql has been executed: {}'.format(sql))
            self.conn.commit()
        else:
            last_row_id = row[0]
        cursor.close()
        return last_row_id

    def get_issues(self, repository_id):
        """

        Parameters
        ----------
        repository_id: int

        Returns
        -------
        List[Issue]
        """
        sql = 'select issue.ID, issue.title, issue.body, issue.created_at, issue.closed_at, issue.updated_at,' \
              ' issue.repository_id, issue.state, issue.user_id from {} join' \
              ' issuelabel on issuelabel.issue_id == issue.ID and issue.repository_id == {}' \
              ' join label on issuelabel.label_id == label.ID and label.name == "bug";' \
            .format(IssueConn.ISSUE_TABLE_NAME, repository_id)
        cursor = self.conn.cursor()
        user_conn = UserConn(path_to_db=self.db_path)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        rows = cursor.fetchall()
        issues = list()
        for row in rows:
            user_id = row[8]
            user = user_conn.get_user(user_id=user_id)
            created_at = row[3]  # type: str
            if created_at is not None and not created_at.endswith('Z'):
                created_at += 'Z'
            closed_at = row[4]  # type: str
            if closed_at is not None and not closed_at.endswith('Z'):
                closed_at += 'Z'
            updated_at = row[5]  # type: str
            if updated_at is not None and not updated_at.endswith('Z'):
                updated_at += 'Z'
            issue = Issue(title=row[1], body=row[2], create_at=created_at, closed_at=closed_at, updated_at=updated_at,
                          state=row[7], user=user)
            issues.append(issue)
        return issues

    def insert_label(self, label):
        """

        Parameters
        ----------
        label: Label

        Returns
        -------
        int
        """
        cursor = self.conn.cursor()
        sql = 'select ID from {} where name == "{}" AND description == "{}";' \
            .format(IssueConn.LABEL_TABLE_NAME, label.name, label.description)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(name, description)' \
                  ' values ("{}", "{}");' \
                .format(IssueConn.LABEL_TABLE_NAME, label.name, label.description)
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError as e:
                logging.error('Error! Invalid operation: {}'.format(sql))
                raise e
            last_row_id = cursor.lastrowid
            logging.info('The following sql has been executed: {}'.format(sql))
            self.conn.commit()
        else:
            last_row_id = row[0]
        cursor.close()
        return last_row_id

    def get_labels(self, name):
        """

        Parameters
        ----------
        name: str

        Returns
        -------
        list of Label
        """
        cursor = self.conn.cursor()
        sql = 'select id, name, description from {} where name == "{}";' \
            .format(IssueConn.LABEL_TABLE_NAME, name)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        rows = cursor.fetchall()
        labels = list()
        for row in rows:
            l = Label(name=row[1], description=row[2])
            l.label_id = row[0]
            labels.append(l)
        return labels

    def insert_assignee(self, assignee):
        """

        Parameters
        ----------
        assignee: Assignee

        Returns
        -------
        int
        """
        cursor = self.conn.cursor()
        sql = 'select ID from {} where login == "{}";' \
            .format(IssueConn.ASSIGNEE_TABLE_NAME, assignee.login)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(login, htmlurl)' \
                  ' values ("{}", "{}");' \
                .format(IssueConn.ASSIGNEE_TABLE_NAME, assignee.login, assignee.htmlurl)
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError as e:
                logging.error('Error! Invalid operation: {}'.format(sql))
                raise e
            last_row_id = cursor.lastrowid
            logging.info('The following sql has been executed: {}'.format(sql))
            self.conn.commit()
        else:
            last_row_id = row[0]
        cursor.close()
        return last_row_id

    def get_assignee(self, login):
        """

        Parameters
        ----------
        login: str

        Returns
        -------
        list of Assignee
        """
        cursor = self.conn.cursor()
        sql = 'select ID, login, htmlurl from {} where login == "{}";' \
            .format(IssueConn.ASSIGNEE_TABLE_NAME, login)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        rows = cursor.fetchall()
        assignees = list()
        for row in rows:
            l = Assignee(login=row[1], htmlurl=row[2])
            l.assignee_id = row[0]
            assignees.append(l)
        return assignees

    def insert_issue_label(self, issue_id, label_id):
        """

        Parameters
        ----------
        issue_id: int
        label_id: int

        Returns
        -------
        None
        """

        cursor = self.conn.cursor()
        sql = 'select issue_id from {} where issue_id == {} and label_id == {}' \
            .format(IssueConn.ISSUELABEL_TABLE_NAME, issue_id, label_id)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(issue_id, label_id) values ({}, {});' \
                .format(IssueConn.ISSUELABEL_TABLE_NAME, issue_id, label_id)
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError as e:
                logging.error('Error! Invalid operation: {}'.format(sql))
                raise e
            logging.info('The following sql has been executed: {}'.format(sql))
            self.conn.commit()
        cursor.close()

    def insert_issue_assignee(self, issue_id, assignee_id):
        """

        Parameters
        ----------
        issue_id: int
        assignee_id: int

        Returns
        -------
        None
        """
        cursor = self.conn.cursor()
        sql = 'select issue_id from {} where issue_id == {} and assignee_id == {};' \
            .format(IssueConn.ISSUEASSIGNEE_TABLE_NAME, issue_id, assignee_id)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(issue_id, assignee_id) values ({}, {});' \
                .format(IssueConn.ISSUEASSIGNEE_TABLE_NAME, issue_id, assignee_id)
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError as e:
                logging.error('Error! Invalid operation: {}'.format(sql))
                raise e
            logging.info('The following sql has been executed: {}'.format(sql))
            self.conn.commit()
        cursor.close()


class ServiceConn:

    TABLE_NAME = 'service'

    def __init__(self, path_to_db):
        self.conn = sqlite3.connect(path_to_db)

    def update_service(self, start_date: date, end_date: date, service_id: int):
        """
        update service based on the first commit date of any of its dependencies

        Parameters
        ----------
        start_date: date
        end_date: date
        service_id: int

        Returns
        -------
        bool
            True if service's info was updated, otherwise False
        """
        try:
            assert isinstance(start_date, date), "Error! start_date is not a date: {}".format(type(start_date))
            assert type(start_date) is date, "Error! start_date is not a type of date: {}".format(type(start_date))
        except AssertionError as e:
            logging.critical('Error! update_service: start_date is not a date: {}'.format(e))
            raise e

        cursor = self.conn.cursor()
        if end_date is not None:
            sql = 'update {} set start_date = "{}" , end_date = "{}" where id = {}' \
                .format(ServiceConn.TABLE_NAME, start_date.isoformat(), end_date.isoformat(), service_id)
        else:
            sql = 'update {} set start_date = "{}" where id = {}' \
                .format(ServiceConn.TABLE_NAME, start_date.isoformat(), service_id)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logging.critical('Error! update_service sql={} msg={}'.format(sql, e))
            raise e
        cursor.close()
        self.conn.commit()
        return True

    def get_services(self, since_date=None):
        """
        gets a list of Service objects since the date specified by the since_date parameter

        Parameters:
        -----------
        since_date: date

        Returns
        -------
        list of Service
        """

        cursor = self.conn.cursor()
        if since_date is None:
            sql = 'select id, name, start_date, end_date from {};' \
                .format(ServiceConn.TABLE_NAME)
        else:
            assert isinstance(since_date, date), "Error! since_date is not a type of date"
            sql = 'select id, name, start_date, end_date from {} where date(start_date) <= date("{}");' \
                .format(ServiceConn.TABLE_NAME, since_date.isoformat(), since_date.isoformat())
        cursor.execute(sql)
        rows = cursor.fetchall()
        services = list()
        for row in rows:
            s = Service(name=row[1], start_date_str=row[2], end_date_str=row[3])
            if s.end_date is not None and since_date is not None and \
                s.end_date >= since_date:
                s.service_id = row[0]
                services.append(s)
            else:
                s.service_id = row[0]
                services.append(s)
        cursor.close()
        return services

    def list_all_service_names(self):
        """

        Parameters:
        -----------
        service_name: str
        repository_name: str

        Returns
        -------
        list of dict
        """
        cursor = self.conn.cursor()
        sql = 'select name from {0};'.format(ServiceConn.TABLE_NAME)
        cursor.execute(sql)
        rows = cursor.fetchall()
        services = list()
        for row in rows:
            name = row[0]

            services.append(name)
        cursor.close()
        return services

    def get_service(self, name=None, service_id=None):
        """

        Parameters
        ----------
        name: str
        service_id: int

        Returns
        -------
        Service
        """
        assert name is not None or service_id is not None, "Error! both parameters are none"
        cursor = self.conn.cursor()
        if name is not None:
            sql = 'select id, name, start_date, end_date from {} where name == "{}";' \
                .format(ServiceConn.TABLE_NAME, name)
        else:
            sql = 'select id, name, start_date, end_date from {} where ID == {};' \
                .format(ServiceConn.TABLE_NAME, service_id)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            s = Service(name=row[1], start_date_str=row[2], end_date_str=row[3])
            s.service_id = row[0]

            cursor.close()
            return s

    def insert_service(self, name: str, start_date: date) -> int:
        """

        Parameters
        ----------
        name: str
        start_date: date

        Returns
        -------
        int
            new service ID
        """
        s = self.get_service(name)
        if s is None:
            assert name is not None, "Error! database schema does not allow name to be None"
            assert start_date is not None, "Error! database schema does not allow start_date to be None"
            assert isinstance(start_date, date), "Error! start_date is not a date: {}".format(type(start_date))
            sql = 'insert into service(name, start_date) values ("{}", "{}")'.format(name, start_date.isoformat())
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql)

            except sqlite3.OperationalError as e:
                logging.error('Error! Invalid operation: {}'.format(sql))
                raise e
            service_id = cursor.lastrowid
            logging.info('The following sql has been executed: {}'.format(sql))
            self.conn.commit()
        else:
            service_id = s.service_id
        return service_id

    def delete_service(self, service_id):
        """

        Parameters
        ----------
        service_id: int

        Returns
        -------
        int
            new service ID
        """
        sql = 'delete from service where ID={}'.format(service_id)
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)

        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        self.conn.commit()


class ServiceRepositoryConn:

    TABLE_NAME = 'servicerepository'

    def __init__(self, path_to_db):
        self.conn = sqlite3.connect(path_to_db)

    def get_service_repository(self, service_name=None, repository_name=None):
        """

        Parameters:
        -----------
        service_name: str
        repository_name: str

        Returns
        -------
        list of dict
        """
        cursor = self.conn.cursor()
        if service_name is not None:
            sql = 'select {0}.repository_id, {0}.service_id, {0}.start_date, ' \
                  '{0}.end_date, {0}.initial_loc from {0} join {1} ' \
                  'on {1}.ID == {0}.service_id where service.name == "{2}";' \
                .format(ServiceRepositoryConn.TABLE_NAME, ServiceConn.TABLE_NAME, service_name)
        else:
            sql = 'select servicerepository.repository_id, servicerepository.service_id, servicerepository.start_date, ' \
                  'servicerepository.end_date from {0} join {1} ' \
                  'on service.ID == servicerepository.service_id where {1}.name == "{2}";' \
                .format(ServiceRepositoryConn.TABLE_NAME, RepositoryConn.TABLE_NAME, repository_name)
        cursor.execute(sql)
        rows = cursor.fetchall()
        services = list()
        if rows is not None:
            for row in rows:
                d = dict()
                d['repository_id'] = row[0]
                d['service_id'] = row[1]
                d['start_date'] = row[2]
                d['end_date'] = row[3]
                d['initial_loc'] = row[4]
                services.append(d)
            cursor.close()
        return services

    def get_service_repository_by_service_id(self, service_id: int) -> List[dict]:
        """

        :param service_id:
        :type service_id: int
        :return:
        :rtype: List[dict]
        """
        cursor = self.conn.cursor()
        sql = 'select servicerepository.repository_id, servicerepository.service_id, servicerepository.start_date, ' \
              'servicerepository.end_date from {0} where service_id == "{1}";' \
            .format(ServiceRepositoryConn.TABLE_NAME, service_id)
        cursor.execute(sql)
        rows = cursor.fetchall()
        service_repos = list()
        if rows is not None:
            for row in rows:
                d = dict()
                d['repository_id'] = row[0]
                d['service_id'] = row[1]
                d['start_date'] = row[2]
                d['end_date'] = row[3]
                d['initial_loc'] = row[4]
                service_repos.append(d)
            cursor.close()
        return service_repos

    def insert_service_repo(self, service_id: int, repository_id: int, start_date: str, end_date: str,
                            initial_loc: int) -> None:
        """
        insert the relationship between service and repository if it is new

        Parameters
        ----------
        service_id: int
        repository_id: int
        start_date: str
        end_date: str
        initial_loc: int

        Returns
        -------
        None
        """
        if initial_loc is not None:
            assert isinstance(initial_loc, int), "Error! Invalid type of initial_loc"
            assert initial_loc >= 0, "Error! Invalid initial_loc = {}".format(initial_loc)
        cursor = self.conn.cursor()
        sql = 'select start_date, end_date, initial_loc from servicerepository where service_id = {} and' \
              ' repository_id = {}'.format(service_id, repository_id)
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            raise e
        row = cursor.fetchone()
        # if this relationship has not been inserted, then insert it
        if row is None:
            if end_date is not None:
                sql = 'insert into servicerepository(service_id, repository_id, start_date, end_date, initial_loc)' \
                      ' values ({}, {}, "{}", "{}", {})'.format(service_id, repository_id, start_date, end_date,
                                                                initial_loc)
            else:
                sql = 'insert into servicerepository(service_id, repository_id, start_date, initial_loc)' \
                      ' values ({}, {}, "{}", {})'.format(service_id, repository_id, start_date, initial_loc)

            try:
                cursor.execute(sql)

            except sqlite3.OperationalError as e:
                logging.error('Error! Invalid operation: {}'.format(sql))
                raise e
            logging.info('The following sql has been executed: {}'.format(sql))
            self.conn.commit()
        else:
            logging.info('The relationship between service_id = {} and repository_id = {} has already been inserted'
                         .format(service_id, repository_id))

    def delete_service_repo(self, service_id=None, repository_id=None):
        """

        :param service_id: int
        :param repository_id: int
        :return:
        """
        assert (service_id is not None) or (repository_id is not None)
        if service_id is not None:
            sql = 'delete from servicerepository where service_id={}'.format(service_id)
        else:
            sql = 'delete from servicerepository where repository_id={}'.format(repository_id)
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)

        except sqlite3.OperationalError as e:
            logging.error('Error! Invalid operation: {}'.format(sql))
            raise e
        self.conn.commit()


class ServiceExtensionsConn:

    TABLE_NAME = 'service_extensions'

    def __init__(self, path_to_db):

        self.conn = sqlite3.connect(path_to_db)
        self.db_path = path_to_db

    def insert_service_extension(self, service_id: int, extension_id: int):
        """

        Parameters
        ----------
        service_id: int
        extension_id: int

        Returns
        -------
        bool
        """
        cursor = self.conn.cursor()

        sql = 'select * from {} where service_id = {} and extension_id = {};' \
            .format(ServiceExtensionsConn.TABLE_NAME, service_id, extension_id)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            sql = 'insert into {}(service_id, extension_id) values({},{});' \
                .format(ServiceExtensionsConn.TABLE_NAME, service_id, extension_id)
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError as e:
                logging.error('Error! insert_service_extension: sql={} msg={}'.format(sql, e))
                raise e
            self.conn.commit()
        return True


class ExtensionsConn:

    TABLE_NAME = 'extensions'
    SERVICE_EXTENSIONS = 'service_extensions'

    def __init__(self, path_to_db):

        self.conn = sqlite3.connect(path_to_db)
        self.db_path = path_to_db

    def list_extensions(self) -> list:
        """

        Returns
        -------
        List[]
        """
        cursor = self.conn.cursor()

        sql = 'select id, value, language from {};' \
            .format(ExtensionsConn.TABLE_NAME)
        cursor.execute(sql)
        rows = cursor.fetchall()
        exts = list()
        if rows is None or len(rows) == 0:
            pass
        else:
            for row in rows:
                ext_id = row[0]
                value = row[1]
                lang = row[2]
                assert value is not None, "Error! value cannot be None"
                assert lang is not None, "Error! lang cannot be None"
                exts.append({'id': ext_id, 'value': value, 'language': lang})
        return exts

    def get_extensions_by_service(self, service_id):
        """

        Parameters
        ----------
        service_id: int

        Returns
        -------
        List[dict]
        """
        assert service_id is not None, "Error! service_id is None"
        assert isinstance(service_id, int), "Error! service_id is not a int"
        cursor = self.conn.cursor()

        sql = 'select value from {0} join {1} on {0}.id == {1}.extension_id where {1}.service_id == {2};' \
            .format(ExtensionsConn.TABLE_NAME, ExtensionsConn.SERVICE_EXTENSIONS, service_id)
        cursor.execute(sql)
        rows = cursor.fetchall()
        exts = list()
        if rows is None or len(rows) == 0:
            pass
        else:
            for row in rows:
                value = row[0]
                exts.append(value)
        return exts


class FilenamePatternConn:

    TABLE_NAME = 'filename_pattern'

    def __init__(self, path_to_db):

        self.conn = sqlite3.connect(path_to_db)
        self.db_path = path_to_db

    def get_patterns(self, service_id, repository_id, pattern_type):
        """

        Parameters
        ----------
        service_id: int
        repository_id: int
        pattern_type: str

        Returns
        -------
        List[str]
        """
        assert pattern_type == 'inclusion' or pattern_type == 'exclusion', \
            "Error! Invalid pattern type: {}".format(pattern_type)
        cursor = self.conn.cursor()

        sql = 'select pattern from {0} where service_id == {1} and repository_id = {2} and type = "{3}";' \
            .format(FilenamePatternConn.TABLE_NAME, service_id, repository_id, pattern_type)
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows is None or len(rows) == 0:
            return list()
        else:
            patterns = list()
            for row in rows:
                value = row[0]
                patterns.append(value)
            return patterns
