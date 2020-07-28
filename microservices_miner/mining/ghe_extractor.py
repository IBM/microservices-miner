# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
import os
import requests
from microservices_miner.model.git_commit import Commit
from microservices_miner.model.user import User
from microservices_miner.control.database_conn import FileModificationConn
from microservices_miner.model.assignee import Assignee
from microservices_miner.control.issue_mgr import IssueMgr
from microservices_miner.model.repository import Repository
from microservices_miner.model.label import Label
from datetime import datetime, date
from microservices_miner.control.user_mgr import UserMgr
from microservices_miner.control.repository_mgr import RepositoryMgr
from microservices_miner.control.commit_mgr import CommitMgr
from microservices_miner.control.service_mgr import ServiceMgr
from microservices_miner.model.file_modification import FileModification
from microservices_miner.control.filesystem_mgr import FileSystemMgr
from microservices_miner.model.issue import Issue
import re
import base64
import difflib
from pathlib import Path
from typing import List, Dict
import json
import logging
import argparse

logging.basicConfig(filename='github_miner.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


class GHEExtractor:

    DIFF_DIR = 'diff_dir'
    API_V3 = 'api/v3'
    COM_PATTERN = '.com'
    PUBLIC_GITHUB_API = 'https://api.github.com'
    PUBLIC_GITHUB_REPO_URL = 'https://github.com/'

    def __init__(self, db_path):
        """

        Parameters
        ----------
        db_path: str
            path to the database file
        """

        self.db_path = db_path
        self.commit_mgr = CommitMgr(self.db_path)

    @staticmethod
    def _extract_users_from_ghe(base_url, username, api_token=None):
        """

        Parameters
        ----------
        username: str
        base_url: str
        api_token: str

        Returns
        -------
        User
        """
        username = username.replace('.', '-')
        url = '{}/users/{}'.format(base_url, username)

        resp = GHEExtractor._make_get_request(url=url, api_token=api_token)
        user_resp = resp.json()
        user = UserMgr.make_user(email=user_resp.get('email'), login=user_resp.get('login'), name=user_resp.get('name'))

        return user

    @staticmethod
    def _make_get_request(url, params=None, api_token=None, allow_redirects=False):
        """

        Parameters
        ----------
        url: str
        params: dict
        api_token: str
        allow_redirects: bool

        Returns
        -------
        requests.Response
        """
        headers = None
        # if not url.startswith(GHEExtractor.PUBLIC_GITHUB_API):
        headers = dict()
        headers['Authorization'] = 'token %s' % api_token
        resp = requests.get(url=url, params=params, headers=headers, allow_redirects=allow_redirects)
        return resp

    def extract_commits_from_ghe(self, base_url, owner, repo, api_token, since=None):
        """
        gets commit data from GHE API

        Parameters
        ----------
        base_url: str
        owner: str
            owner name
        repo: Repository
            repository name
        since: datetime
        api_token: str

        Returns
        -------
        list of dict
            a sorted list of commit data
        """
        logging.info('Extracting commits from repo: {} since={}'.format(repo.name, since))
        user_mgr = UserMgr(path_to_db=self.db_path)
        url = '{}/repos/{}/{}/commits'.format(base_url, owner, repo.name)
        commit_list = list()
        params = {'sha': 'master'}
        if since is not None:
            params['since'] = since.isoformat(sep='T')
        resp = GHEExtractor._make_get_request(url=url, params=params, api_token=api_token)
        assert resp.status_code == 200, \
            "Error! status_code={} msg={} url={}".format(resp.status_code, resp.content, resp.url)
        resp_data = resp.json()
        assert resp_data is not None, 'Error! response is None'
        assert isinstance(resp_data, list), "Error! response is not list"
        commit_list.extend(resp_data)
        while 'next' in resp.links.keys():
            url = resp.links['next']['url']
            resp = GHEExtractor._make_get_request(url=url, params=params, api_token=api_token)
            logging.info('Requesting to GHE: URL={} params={}'.format(url, params))
            resp_data = resp.json()
            assert resp_data is not None, 'Error! response is None'
            assert isinstance(resp_data, list), "Error! response is not list"
            commit_list.extend(resp_data)

        commit_list_aux = list()
        for c in commit_list:
            commit_item = c.get('commit')
            sha = c.get('sha')
            if commit_item is not None:
                dt = commit_item.get('author').get('date')
                if dt is not None:
                    d = dict()
                    author_data = commit_item.get('author')
                    name = author_data.get('name')

                    email = author_data.get('email')
                    if email is not None and '@' in email:
                        fields = email.split('@')
                        login = fields[0]
                    else:
                        login = None
                    # get message and remove special characters
                    message_aux = commit_item.get('message')
                    message = ''
                    for k in message_aux.split("\n"):
                        message += re.sub(r"[^a-zA-Z0-9]+", ' ', k)
                    user = user_mgr.get_user_from_database(login=login, email=email, name=name)
                    if user is None:
                        try:
                            user = GHEExtractor._extract_users_from_ghe(username=login, base_url=base_url)
                        except AssertionError:
                            name = author_data.get('name')
                            user = UserMgr.make_user(email=email, name=name, login=login)
                        user.user_id = user_mgr.insert_user(user)

                    author_dt_str = author_data.get('date')
                    dt = datetime.strptime(author_dt_str, "%Y-%m-%dT%H:%M:%SZ")
                    assert sha is not None, "Error! c = {}".format(c)
                    d['date'] = dt
                    d['user'] = user
                    d['email'] = email
                    d['sha'] = sha
                    d['message'] = message
                    parents = c.get('parents')
                    if parents is not None:
                        d['parents_sha'] = list()
                        for index, p in enumerate(parents):
                            d['parents_sha'].append((index, p.get('sha')))

                    else:
                        d['parents_sha'] = list()
                    commit_list_aux.append(d)
        sorted_commit_list = sorted(commit_list_aux, key=lambda k: k.get('date'))

        return sorted_commit_list

    def _insert_commit_data_into_database(self, base_url, commit_list, repo, owner, api_token):
        """

        Parameters
        ----------
        commit_list: List
        repo: Repository
        owner: str
        base_url: str
        api_token: str

        Returns
        -------

        """
        commit_dates = list()
        num_commits = len(commit_list)

        for i, d in enumerate(commit_list):

            sha = d.get('sha')
            commit = Commit(date=d.get('date'), sha=sha, user=d.get('user'), comment=d.get('message'))
            logging.info('Extracting commit from repo {}: SHA={} {} out of {}'.format(repo.name, sha, i+1, num_commits))

            existing_commit = self.commit_mgr.get_commit(sha=sha)
            # if commit has not already been inserted into the database or its file modifications have not been
            # collected
            if existing_commit is None or len(existing_commit.file_modifications) == 0:
                # get the file modifications from a single commit
                commit.file_modifications, num_add, num_del = \
                    self._extract_file_modifications_from_ghe(owner=owner, repo=repo, sha=sha, base_url=base_url,
                                                              api_token=api_token)
                commit_dates.append(commit.date.isoformat())
                self.commit_mgr.insert_commit(repository=repo, commit=commit, parent_commit_shas=d.get('parents_sha'))

            else:
                logging.info('Yay! {} has already been inserted into the database'.format(repo.name))

    def _find_and_repair_inconsistencies(self, repository, owner, extensions, base_url):
        """
        find and repair inconsistencies on data made provided by GHE (e.g., a file that has status modified but the
        number of modifications is zero)

        Parameters
        ----------
        repository: Repository
        owner: str
        extensions: List[str]
        base_url: str

        Returns
        -------

        """
        repository_mgr = RepositoryMgr(self.db_path)

        commits = repository_mgr.find_inconsistent_commits(repository=repository, extensions=extensions)
        sorted_commits = sorted(commits, key=lambda c: c.date)
        repo_mgr = RepositoryMgr(path_to_db=self.db_path)

        for commit in sorted_commits[1:]:

            parent_commit_sha = self.commit_mgr.get_parent_commit_sha(commit=commit)
            assert parent_commit_sha is not None, 'Error! Unable to find parent of commit: {}'.format(commit.sha)
            repository = repo_mgr.get_repository_by_commit(sha=commit.sha)
            self._compare_two_commits(older_commit_sha=parent_commit_sha, newer_commit=commit,
                                      owner=owner, repo=repository, base_url=base_url)

    @staticmethod
    def _extract_file_modifications_from_ghe(base_url, owner, repo, sha, api_token):
        """
        extracts file modifications from GHE, given an owner, a repository and the SHA of the commit

        Parameters
        ----------
        sha: str
        repo: Repository
        owner: str
        base_url: str

        Returns
        -------
        list of FileModification
        """
        url = '{}/repos/{}/{}/commits/{}'.format(base_url, owner, repo.name, sha)
        resp = GHEExtractor._make_get_request(url=url, api_token=api_token)
        assert resp.status_code == 200, "Error! status={} msg={} url={}".format(resp.status_code, resp.text, resp.url)
        single_commit = resp.json()
        stats = single_commit.get('stats')
        additions = stats.get('additions')
        deletions = stats.get('deletions')
        files = single_commit.get('files')
        file_modifications = list()
        for f in files:
            filename = f.get('filename')
            status = f.get('status')
            logging.info('filename={} status={}'.format(filename, status))
            fm = FileModification(filename=filename, status=f.get('status'), additions=f.get('additions'),
                                  deletions=f.get('deletions'), changes=f.get('changes'))
            file_modifications.append(fm)

        return file_modifications, additions, deletions

    def _compare_two_commits(self, base_url, older_commit_sha, newer_commit, repo, owner):
        """
        compare two commits and update FileModification table when an inconsistency is detected (e.g., a file is
        modified but the number of modifications is zero

        Parameters
        ----------
        older_commit_sha: str
        newer_commit: Commit
        repo: Repository
        owner: str

        Returns
        -------

        """
        url = '{}/repos/{}/{}/compare/{}...{}'.format(base_url, owner, repo.name, older_commit_sha,
                                                      newer_commit.sha)
        resp = GHEExtractor._make_get_request(url=url)
        # logging.info('GET {}'.format(url))
        assert resp.status_code == 200, "Error! status={} msg={} url={}".format(resp.status_code, resp.text, resp.url)
        compare_resp = resp.json()
        assert isinstance(compare_resp, dict), "Error! Unexpected response: {}".format(compare_resp)
        filemodification_conn = FileModificationConn(path_to_db=self.db_path)
        files = compare_resp.get('files')
        assert isinstance(files, list), "Error! files is not a list: {}".format(files)
        for file in files:
            assert isinstance(file, dict), "Error! item of files is not a dict: {}".format(file)
            assert 'status'
            f_name = file.get('filename')
            for fm in newer_commit.file_modifications:
                if f_name in fm.filename and not f_name.endswith('__init__.py'):
                    try:
                        status = file.get('status')
                        additions = file.get('additions')
                        deletions = file.get('deletions')
                        changes = file.get('changes')
                    except AttributeError as e:
                        logging.critical('f={} e={}'.format(file, e))
                        raise e
                    fm = FileModification(filename=f_name, status=status, additions=additions,
                                          deletions=deletions, changes=changes)
                    contents_url = file.get('contents_url')  # type: str
                    equal_index = contents_url.rfind('=') + 1
                    contents_url = contents_url[:equal_index]
                    # update data when file is modified but number of changes is zero
                    if fm.status == 'modified' and fm.changes == 0:

                        downloaded_files = list()
                        for sha in [older_commit_sha, newer_commit.sha]:

                            downloaded_file_path = self._download_file(url=contents_url, commit_sha=sha,
                                                                       filename=fm.filename)
                            downloaded_files.append(downloaded_file_path)
                        num_additions, num_deletions = GHEExtractor._diff_files(older_filename=downloaded_files[0],
                                                                                newer_filename=downloaded_files[1])
                        fm.additions = num_additions
                        fm.deletions = num_deletions
                    # update data when file is removed but number of deletions is zero
                    elif fm.status == 'removed' and fm.deletions == 0:
                        downloaded_file_path = self._download_file(url=contents_url, commit_sha=older_commit_sha,
                                                                   filename=fm.filename)

                        num_deletions = GHEExtractor._count_loc(downloaded_file_path)
                        fm.deletions = num_deletions
                    # update data when file is added but number of additions is zero
                    elif fm.status == 'added' and fm.additions == 0:
                        downloaded_file_path = self._download_file(url=contents_url, commit_sha=newer_commit.sha,
                                                                   filename=fm.filename)

                        num_additions = GHEExtractor._count_loc(downloaded_file_path)
                        fm.additions = num_additions
                    logging.info('filemodification={}'.format(fm))
                    filemodification_conn.update_filemodification(filemodification=fm, commit_id=newer_commit.commit_id)

    @staticmethod
    def _count_loc(file):
        """

        Parameters
        ----------
        file: str

        Returns
        -------
        int
        """
        with open(file, 'r') as f:
            lines = f.readlines()
            loc = len(lines)
        return loc

    def _download_file(self, url, commit_sha, filename):
        """

        Parameters
        ----------
        url:str
        commit_sha: str
        filename: str

        Returns
        -------
        str
        """
        p = Path(os.getcwd())
        posix_path = p.parent / GHEExtractor.DIFF_DIR / commit_sha / filename
        path = str(posix_path)
        if not os.path.isfile(path):
            url += commit_sha
            resp = GHEExtractor._make_get_request(url, allow_redirects=True)
            data = resp.json()
            relative_path = data.get('path')
            posix_path = p.parent / GHEExtractor.DIFF_DIR / commit_sha / relative_path
            path = str(posix_path)
            index = path.rfind('/')
            dirs_path = path[:index]
            os.makedirs(dirs_path, exist_ok=True)
            with open(path, 'wb') as f:
                content = data.get('content')
                try:
                    body = base64.b64decode(content)
                except TypeError as e:
                    logging.critical('path={} url={} exception={}'.format(path, url, e))
                    raise e
                f.write(body)

        assert os.path.isfile(path) is True, "Error! {} file not found".format(path)
        logging.info('saved file {}'.format(path))
        return path

    @staticmethod
    def _diff_files(older_filename, newer_filename):
        """

        Parameters
        ----------
        older_filename: str
        newer_filename: str

        Returns
        -------
        (int, int)
        """
        additions = deletions = 0
        with open(older_filename) as text1:
            with open(newer_filename) as text2:
                d = difflib.Differ()
                diff = list(d.compare(text1.readlines(), text2.readlines()))
                for d in diff:
                    if d.startswith('+'):
                        additions += 1
                    elif d.startswith('-'):
                        deletions += 1
        return additions, deletions

    def _get_filemodification(self, filename, until_date, repository_id):
        """

        Parameters
        ----------
        filename
        until_date: date

        Returns
        -------

        """
        file_modification_conn = FileModificationConn(path_to_db=self.db_path)
        file_modifications = file_modification_conn. \
            get_file_modification_by_name_and_date(filename=filename, until_date=until_date.isoformat(),
                                                   repository_id=repository_id)
        return len(file_modifications) > 0

    def _get_label(self, label_data):
        """

        Parameters
        ----------
        label_data: dict

        Returns
        -------
        Label
        """
        name = label_data.get('name')
        issue_mgr = IssueMgr(path_to_db=self.db_path)
        label = issue_mgr.get_label(name)
        if label is None:
            description = label_data.get('description')
            label = Label(name=name, description=description)
            label_id = issue_mgr.insert_label(label)
            label.label_id = label_id
        return label

    def _create_assignee(self, assignee_data):
        """

        Parameters
        ----------
        assignee_data: dict

        Returns
        -------
        Assignee
        """
        login = assignee_data.get('login')
        issue_mgr = IssueMgr(path_to_db=self.db_path)
        assignee = issue_mgr.get_assignee(login)
        if assignee is None:
            html_url = assignee_data.get('html_url')
            assignee = Assignee(login=login, htmlurl=html_url)
            assignee_id = issue_mgr.insert_assignee(assignee)
            assignee.assignee_id = assignee_id
        return assignee

    def _get_issues_from_ghe(self, base_url, owner, repo_name):
        """

        Parameters
        ----------
        owner: str
        repo_name: str
        base_url: str

        Returns
        -------

        """
        issues = list()
        url = '{}/repos/{}/{}/issues'.format(base_url, owner, repo_name)
        # for la in ['bug', 'enhancement']:
        params = {'state': 'all', 'filter': 'all'}
        resp = GHEExtractor._make_get_request(url=url, params=params)
        # assert resp.status_code == 200, "Error! status={} msg={}".format(resp.status_code, resp.text)
        status = resp.status_code
        has_next = True
        while status == 200 and has_next:
            ghe_issues_resp = resp.json()
            user_mgr = UserMgr(path_to_db=self.db_path)
            for i in ghe_issues_resp:
                if 'pull_request' not in i.keys():

                    title_aux = i.get('title')
                    title = ''
                    for k in title_aux.split("\n"):
                        title += re.sub(r"[^a-zA-Z0-9]+", ' ', k)
                    body_aux = i.get('body')
                    body = ''
                    if body_aux is not None and isinstance(body_aux, str):
                        for k in body_aux.split("\n"):
                            body += re.sub(r"[^a-zA-Z0-9]+", ' ', k)
                    created_at = i.get('created_at')
                    closed_at = i.get('closed_at')
                    updated_at = i.get('updated_at')
                    state = i.get('state')
                    labels = list()
                    label_resp = i.get('labels')
                    if label_resp is not None:
                        for l in label_resp:
                            labels.append(self._get_label(l))

                    login = i.get('user').get('login')
                    user = user_mgr.get_user_from_database(login=login)
                    if user is None:
                        user = GHEExtractor._extract_users_from_ghe(base_url=base_url, username=login)
                        user.user_id = user_mgr.insert_user(user)
                    assert user is not None, "Error! User is None: login={}".format(login)
                    assignees_data = i.get('assignees')
                    assignees_list = list()
                    for a in assignees_data:
                        assignees_list.append(self._create_assignee(assignee_data=a))

                    issue = Issue(title=title, body=body, create_at=created_at, state=state, user=user,
                                  closed_at=closed_at, updated_at=updated_at)
                    issue.assignees = assignees_list
                    issues.append(issue)
                    issue.labels = labels
            if 'next' in resp.links.keys():
                url = resp.links['next']['url']
                resp = GHEExtractor._make_get_request(url=url, params=params)
                assert resp.status_code == 200, "Error! status={} msg={}".format(resp.status_code, resp.text)
            else:
                has_next = False
        return issues

    @staticmethod
    def _get_base_url(repo_url: str) -> str:
        """

        Parameters
        ----------
        repo_url: str

        Returns
        -------
        str
        """
        assert repo_url.startswith('https://github'), 'Error! Invalid URL: {}'.format(repo_url)
        if repo_url.startswith(GHEExtractor.PUBLIC_GITHUB_REPO_URL):
            return GHEExtractor.PUBLIC_GITHUB_API
        else:
            com_index = repo_url.find(GHEExtractor.COM_PATTERN)
            base_url = repo_url[:com_index + len(GHEExtractor.COM_PATTERN)]
            s = base_url + '/' + GHEExtractor.API_V3
            return s

    @staticmethod
    def extract_new_data_from_ghe(service_list: List[Dict], db_path: str, api_token: str) -> None:
        """

        Parameters
        ----------
        service_list: List[Dict]
        db_path: str
        api_token: str

        Returns
        -------
        None
        """
        extractor = GHEExtractor(db_path=db_path)
        commit_mgr = CommitMgr(path_to_db=db_path)
        repo_mgr = RepositoryMgr(path_to_db=db_path)
        service_mgr = ServiceMgr(db_path=db_path)
        filesystem_mgr = FileSystemMgr(db_path)

        for s in service_list:
            service_name = s.get('name')
            end_date_str = s.get('end_date')
            start_date_str = s.get('start_date')
            programming_languages = s.get('programming_languages')
            logging.info('Extracting data from: {}'.format(service_name))
            service = service_mgr.get_service(service_name=service_name)
            if service is None:
                sid = service_mgr.insert_service(name=service_name, start_date_str=start_date_str)
                service = service_mgr.get_service(service_id=sid)
                assert isinstance(sid, int)
                filesystem_mgr.insert_extensions(service_id=sid, programming_languages=programming_languages)
            else:
                sid = service.service_id

            # get list of extensions of this service
            extensions = filesystem_mgr.get_extensions(service_id=sid)
            for rep in s.get('repositories'):
                url = rep.get('url')
                base_url = GHEExtractor._get_base_url(repo_url=url)
                print('Getting data from: {}'.format(url))
                repo_name = rep.get('name')
                owner = rep.get('owner')
                repo = repo_mgr.get_repository(url=url, service_id=sid)
                if repo is None:
                    repo_url = '{}/{}/{}'.format(base_url, owner, repo_name)
                    repo = repo_mgr.create_repository(name=repo_name, url=repo_url)
                    repo.repository_id = repo_mgr.insert_repository(repository=repo)
                    repo.owner = owner

                start_date_repo = rep.get('start_date')
                end_date_repo = rep.get('end_date')
                initial_loc = rep.get('initial_loc')
                service_mgr.insert_service_repository(service_name=service_name, repository_id=repo.repository_id,
                                                      start_date=start_date_repo, end_date=end_date_repo,
                                                      initial_loc=initial_loc)

                # get most recent commits
                last_commit = commit_mgr.get_commit_by_position(repository_id=repo.repository_id, pos=-1)
                if last_commit is not None:
                    since = last_commit.date
                else:
                    since = None
                assert repo is not None, "Error! Invalid repo"
                assert isinstance(repo, Repository), "Error! repo is not a type of Repository"
                assert repo.repository_id is not None, "Error! repo id is none: {}".format(repo)
                commits = extractor.extract_commits_from_ghe(base_url=base_url, owner=owner, repo=repo, since=since,
                                                             api_token=api_token)
                extractor._insert_commit_data_into_database(commit_list=commits, repo=repo, owner=owner,
                                                            base_url=base_url, api_token=api_token)

                extractor._find_and_repair_inconsistencies(owner=owner, repository=repo, extensions=extensions,
                                                           base_url=base_url)
            try:
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            except (ValueError, TypeError) as e:
                end_date = None

            try:
                start_date_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
                start_date = start_date_dt.date()
            except (ValueError, TypeError) as e:
                start_date = None

            service_mgr.update_dates(service_id=service.service_id, end_date=end_date, start_date=start_date)

    @staticmethod
    def get_target_services_description(path: str) -> List[Dict]:
        """

        Parameters
        ----------
        path: str

        Returns
        -------
        List[Dict]
        """
        with open(path, 'r') as json_file:
            try:
                data = json.load(json_file)
            except Exception as e:
                print('Error! Unable to read file: {} msg: {}'.format(json_file, e))
                raise e
            assert isinstance(data, list), "Error! Input data is not a list"
            for d in data:
                assert isinstance(d, dict), "Error! Items are not a type of dict"
                assert all(k in d.keys() for k in ['name', 'start_date', 'end_date', 'programming_languages',
                                                   'repositories']), "Error! Missing fields"
        return data


def main():
    """

    """
    logging.info('Starting GHE extractor...')
    parser = argparse.ArgumentParser(description='path to input data')
    parser.add_argument('--path', type=str, nargs='?', help='path to input data')
    args = parser.parse_args()
    path = args.path
    assert path is not None
    assert os.path.isfile(path), "Error! {} file does not exist".format(path)
    target_services_description = GHEExtractor.get_target_services_description(path)

    mining_token = os.getenv('MINING_GHE_PERSONAL_ACCESS_TOKEN')
    db_path = os.getenv('DB_PATH')
    assert os.path.isfile(db_path), "Error! There is no {} database file".format(db_path)

    GHEExtractor.extract_new_data_from_ghe(service_list=target_services_description, api_token=mining_token,
                                           db_path=db_path)
    logging.info('Data mining is completed: {}'.format(target_services_description))


if __name__ == '__main__':

    main()
