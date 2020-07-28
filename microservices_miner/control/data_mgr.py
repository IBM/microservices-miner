# (C) Copyright IBM Corporation 2018
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from microservices_miner.model.file_modification import FileModification
import pandas as pd
import numpy as np
from microservices_miner.control.issue_mgr import IssueMgr
from microservices_miner.model.service import Service
from typing import Tuple
from microservices_miner.control.service_mgr import ServiceMgr
from microservices_miner.model.repository import Repository
from microservices_miner.control.database_conn import RepositoryConn, RepositoryCommitConn, \
    ServiceRepositoryConn
from typing import List
import os
from microservices_miner.control.plot_mgr import PlotMgr
from datetime import datetime


class DataMgr:

    def __init__(self, db_path):
        self.db_path = db_path

    def get_time_bins(self, service_names: List[str], step_size_aprox: int, until: datetime) -> Tuple:
        """

        get the date of the first commit given a list of services

        Parameters
        ----------
        service_names: list of str
        step_size_aprox: int
        until: datetime

        Returns
        -------
        tuple of datetime
        """
        time_bins = list()
        repository_conn = RepositoryConn(path_to_db=self.db_path)
        commits_conn = RepositoryCommitConn(path_to_db=self.db_path)
        service_repo_conn = ServiceRepositoryConn(path_to_db=self.db_path)
        first_commit_dt = None
        for service_name in service_names:
            service_repos = service_repo_conn.get_service_repository(service_name=service_name)
            for service_repo in service_repos:

                repo = repository_conn.get_repository(repo_id=service_repo.get('repository_id'))
                assert repo is not None, "Error! Unable to find repo of {} service".format(service_name)
                commits = commits_conn.get_commits_by_repo(repository_id=repo.repository_id)
                assert len(commits) > 0, "Error! empty commits = {}".format(repo)
                sorted_commits = sorted(commits, key=lambda c: c.date)
                if first_commit_dt is None or \
                        (first_commit_dt is not None and first_commit_dt > sorted_commits[0].date):
                    first_commit_dt = sorted_commits[0].date

        t = first_commit_dt
        time_bins.append(first_commit_dt)

        td = until - first_commit_dt
        step_time = td / step_size_aprox
        if step_time.seconds > 60 * 60 * 12:
            num_steps = step_time.days + 1
        else:
            num_steps = step_time.days
        step_size = td / num_steps
        while t < until:
            t += step_size
            time_bins.append(t)
        time_bins = sorted(time_bins)
        assert len(time_bins) > 0, "Error! empty time bins"
        i = 1
        while i < len(time_bins):
            assert time_bins[i] > time_bins[i-1], "Error! i={} i-1={}".format(time_bins[i], time_bins[i-1])
            i += 1
        return tuple(time_bins)

    @staticmethod
    def compute_changes_per_loc(service, time_bins):
        """
        compute LOC growth over time

        Parameters
        ----------
        service: Service
        time_bins: Tuple[datetime]

        Returns
        -------
        dict, list
        """
        print('Computing changes per loc of {} service'.format(service.name))
        loc = 0
        changes_per_loc_list = list()
        for i in range(1, len(time_bins)):
            total_changes = 0
            prev_time = time_bins[i - 1]
            cur_time = time_bins[i]
            for repo_data in service.list_repository_data():
                repo = repo_data.get('repository')
                start_date = repo_data.get('start_date')
                if start_date is not None:
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                else:
                    start_dt = None
                initial_loc = repo_data.get('initial_loc')
                # print('Total number of commits = {}'.format(len(repo.commits)))
                sorted_commits = sorted(repo.commits, key=lambda k: k.date)
                if start_dt is not None and prev_time < start_dt < cur_time:
                    prev_time = start_dt
                    if initial_loc is not None:
                        loc = initial_loc
                for c in sorted_commits:
                    # if there is no modification in current and previous time bins
                    if prev_time <= c.date < cur_time:
                        num_additions, num_deletions, num_changes = \
                            DataMgr.compute_modifications(file_modifications=c.file_modifications)
                        try:
                            loc += num_additions - num_deletions
                        except TypeError as e:
                            print('Error! msg={} loc={} num_additions={} num_deletions={} sha={}'
                                  .format(e, loc, num_additions, num_deletions, c.sha))
                            raise e
                        total_changes += num_changes
            if loc == 0:
                changes_per_loc = np.nan
            else:
                changes_per_loc = total_changes/loc
            changes_per_loc_list.append(changes_per_loc)
        return changes_per_loc_list

    @staticmethod
    def compute_changes(service: Service, time_bins: Tuple):
        """
        compute LOC growth over time

        Parameters
        ----------
        service: Service
        time_bins: Tuple[datetime]

        Returns
        -------
        dict, list
        """
        print('Computing changes of {} service'.format(service.name))
        loc = 0
        changes = list()

        for i in range(1, len(time_bins)):
            total_changes = 0
            prev_time = time_bins[i - 1]
            cur_time = time_bins[i]
            for repo_data in service.list_repository_data():
                repo = repo_data.get('repository')
                initial_loc = repo_data.get('initial_loc')
                sorted_commits = sorted(repo.commits, key=lambda k: k.date)
                i = 0
                c = sorted_commits[i]
                commit_date = c.date
                while i < len(sorted_commits) and commit_date < cur_time:
                    # if there is no modification in current and previous time bins
                    c = sorted_commits[i]
                    commit_date = c.date

                    if prev_time <= c.date:
                        num_additions, num_deletions, num_changes = \
                            DataMgr.compute_modifications(file_modifications=c.file_modifications)
                        loc += num_additions - num_deletions
                        total_changes += num_changes
                        if i == len(sorted_commits) - 1 and initial_loc is not None:
                            num_changes = np.abs(loc - initial_loc)
                            total_changes += num_changes
                    i += 1
            changes.append(total_changes)
        return changes

    @staticmethod
    def compute_loc(service: Service, time_bins: Tuple[datetime]):
        """
        compute the number of LOC given the specified Service object for the specified time bins. That is, be tn a date,
         this method computes the number of LOC in tn. t0 is ignored

        Parameters
        ----------
        service: Service
        time_bins: Tuple[datetime]

        Returns
        -------
        List[int]
        """
        print('Computing LOC of {} service between time bins {}'.format(service.name, time_bins))
        loc = 0
        loc_list = list()
        for i in range(1, len(time_bins)):
            prev_time = time_bins[i-1]
            cur_time = time_bins[i]
            for repo_data in service.list_repository_data():
                repo = repo_data.get('repository')
                start_date = repo_data.get('start_date')
                if start_date is not None:
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                else:
                    start_dt = None
                # print('Total number of commits = {}'.format(len(repo.commits)))
                sorted_commits = sorted(repo.commits, key=lambda k: k.date)
                if start_dt is not None and prev_time <= start_dt < cur_time:
                    prev_time = start_dt
                    initial_loc = repo_data.get('initial_loc')
                    if initial_loc is not None:
                        loc = initial_loc
                for c in sorted_commits:
                    # if there is no modification in current and previous time bins
                    if prev_time <= c.date < cur_time:
                        if c.sha == '632ed9a8ede4912168e9b40382882a97836391e5':
                            print(c)
                        num_additions, num_deletions, _ = \
                            DataMgr.compute_modifications(file_modifications=c.file_modifications)
                        loc_aux = num_additions - num_deletions
                        loc += loc_aux
                        # print('date={} loc={}'.format(c.date, loc))
            loc_list.append(loc)
        return loc_list

    @staticmethod
    def compute_loc_per_repository(repository: Repository) -> (List[int], List[int], List[str]):
        """

        Parameters
        ----------
        repository: Repository

        Returns
        -------
        Tuple
        """
        loc = 0
        loc_list = list()
        sha_list = list()
        date_list = list()

        print('Total number of commits = {}'.format(len(repository.commits)))
        sorted_commits = sorted(repository.commits, key=lambda k: k.date)

        for i in range(0, len(sorted_commits)):
            c = sorted_commits[i]

            num_additions, num_deletions, _ = \
                DataMgr.compute_modifications(file_modifications=c.file_modifications)
            loc += num_additions - num_deletions
            if loc < 0:
                loc = 0
            # assert loc >= 0, \
            #     "Error! LOC cannot be negative: Repository={} SHA={}\n file modifications = {}"\
            #         .format(repository.name, c.sha, [(fm.additions, fm.deletions) for fm in c.file_modifications])
            loc_list.append(loc)
            sha_list.append(c.sha)
            date_list.append(c.date.isoformat())
        return loc_list, sha_list, date_list

    @staticmethod
    def compute_modifications(file_modifications: List[FileModification]):
        """

        Parameters
        ----------
        file_modifications: list of FileModification

        Returns
        -------
        int, int, int
        """
        changes = 0
        additions = 0
        deletions = 0
        for fm in sorted(file_modifications, key=lambda k: k.filename):
            changes += fm.changes
            additions += fm.additions
            deletions += fm.deletions
        assert (deletions + additions) == changes, \
            "Error! deletions + additions != changes: {} + {} != {}".format(deletions, additions, changes)
        return additions, deletions, changes

    @staticmethod
    def _compute_percentual_modification(df, group_by):
        """

        Parameters
        ----------
        df: pd.DataFrame

        Returns
        -------
        (list, list, list)
        """
        df_aux = df[['date', 'additions', 'deletions', 'changes']]
        df_aux.index = df['date']
        df_aux = df_aux.drop('date', axis=1)

        df_group = df_aux.resample(group_by).sum()
        df_group = df_group.sort_index()
        date_list = list()
        percentage_list = list()
        loc_list = list()
        for i, row in enumerate(df_group.iterrows()):
            timestamp = row[0]
            date_list.append(timestamp)
            s = row[1]
            additions = s[0]
            deletions = s[1]
            changes = s[2]
            if i == 0:
                loc = additions - deletions
            else:
                loc = loc_list[i-1] + additions - deletions
            # print(loc)
            assert loc >= 0, "Error! LOC cannot be less than zero s={}".format(s)
            loc_list.append(loc)
            if loc == 0:
                perc = 0
            else:
                perc = changes / loc
            assert perc >= 0, "Error! Invalid percentage - s={} loc={}".format(s, loc)
            percentage_list.append(perc)
        return date_list, loc_list, percentage_list

    @staticmethod
    def compare_repair_time(df):
        """

        Parameters
        ----------
        df: pd.DataFrame

        Returns
        -------

        """

        data = dict()
        for index, row in df.iterrows():
            year = row['closed_at'].year
            repo = row['repository']
            k = (repo, year)
            repair_time = row['time_to_repair']

            if k in data.keys():
                data[k].append(repair_time)
            else:
                data[k] = [repair_time]
        repositories = list()
        repair_time_list = list()
        year_list = list()
        for k,v in data.items():
            repo, year = k
            median_repair = np.median(v)
            repositories.append(repo)
            year_list.append(year)
            repair_time_list.append(median_repair)
        newdf = pd.DataFrame(data={'median_repair_time': repair_time_list, 'repository': repositories,
                                   'year': year_list})
        newdf.sort_values(by=['year'])
        filename = os.path.join(os.getenv('BASE_DIR'), 'data', 'metrics', 'repair_time_per_year_repository.csv')
        newdf.to_csv(filename)
        print('Saved {}'.format(filename))

    def compute_repair_time(self, service_names):
        """
        compute the time to repair a bug, i.e., the time between the bug report and the bug fix.
        This time is computed in days

        Parameters
        ----------
        service_names: list of str

        Returns
        -------
        pd.DataFrame
        """
        repository_list = list()
        service_mgr = ServiceMgr(db_path=self.db_path)
        issue_mgr = IssueMgr(path_to_db=self.db_path)
        for service_name in service_names:
            print('Getting data from {}'.format(service_name))
            service = service_mgr.get_service(service_name=service_name)

            for repo_data in service.list_repository_data():
                repo = repo_data.get('repository')
                if repo not in repository_list:
                    repository_list.append(repo)

        time_to_repair = list()
        closed_at_list = list()
        repositories = list()
        for repo in repository_list:
            issues_aux = issue_mgr.get_issues_by_label(repository_id=repo.repository_id)
            for issue in issues_aux:
                if issue.state == 'closed':
                    td = issue.closed_at - issue.created_at
                    ttr = td.days * 24 * 60 * 60 + td.seconds
                    ttrhr = ttr / (24 * 60 * 60)
                    if ttrhr >= 500:
                        print(issue)
                    time_to_repair.append(ttrhr)
                    closed_at_list.append(issue.closed_at)
                    repositories.append(repo.name)

        df = pd.DataFrame(data={'closed_at': closed_at_list, 'time_to_repair': time_to_repair,
                                'repository': repositories})
        filename = os.path.join(os.getenv('BASE_DIR'), 'data', 'metrics', 'repair_time.csv')
        df.to_csv(filename)
        return df

    @staticmethod
    def group_repair_time(df, interval):
        """

        Parameters
        ----------
        df: pd.DataFrame
        interval: str

        Returns
        -------
        pd.DataFrame
        """
        df = df.set_index(df['closed_at'])
        df = df.drop('closed_at', axis=1)
        df_group = df.resample(interval).median()
        closed_at_date_grps = df_group.index
        date_list = list()
        time_to_repair_list = list()
        for closed_at_date, rows_2 in df.iterrows():
            time_to_repair = rows_2[0]
            for i in range(1, len(closed_at_date_grps)):
                previous_timestamp = closed_at_date_grps[i - 1]
                cur_timestamp = closed_at_date_grps[i]
                if previous_timestamp <= closed_at_date < cur_timestamp:
                    # dt = pd.to_datetime(previous_timestamp)
                    date_list.append(cur_timestamp)
                    time_to_repair_list.append(time_to_repair)

        data = {'date': date_list, 'time_to_repair': time_to_repair_list}
        df_aux = pd.DataFrame(data=data)
        for t in closed_at_date_grps:
            subdf = df_aux[df_aux.date == t]
            print('t={} size={}'.format(t, len(subdf)))
            for q in [0.25, 0.5, 0.75]:
                print(subdf.quantile(q, axis='index'))
        return df_aux

    @staticmethod
    def group_repair_time_by_year(df):
        """

        Parameters
        ----------
        df: pd.DataFrame

        Returns
        -------
        pd.DataFrame
        """
        df['year'] = df['closed_at'].apply(lambda ts: ts.year)
        filename = os.path.join(os.getenv('BASE_DIR'), 'data', 'metrics', 'repair_time_per_year.csv')
        df.to_csv(filename)

        return df

    @staticmethod
    def closed_using_keywords(line):
        """

        Parameters
        ----------
        line

        Returns
        -------

        """
        line = line.lower()
        keywords = [
            'closes',
            'closed',
            'close',
            'fixes',
            'fixed',
            'fix',
            'resolves',
            'resolved',
            'resolve',
        ]
        found = False
        j = 0
        while j < len(keywords) and not found:
            k = keywords[j]
            j += 1
            fields = line.split(' ')
            i = 0
            while i < len(fields):
                field = fields[i]
                if k == field and i + 1 < len(fields):
                    next_field = fields[i + 1]
                    try:
                        int(next_field)
                        found = True
                    except ValueError:
                        i += 1
                        continue
                i += 1

        return found

    @staticmethod
    def analyze_bugs_and_locs(filepath):
        """

        Parameters
        ----------
        filepath

        Returns
        -------

        """
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filepath, sep=',', parse_dates=['date'], date_parser=dateparse)

        df['year'] = df['date'].apply(lambda t: t.year)
        df_group = df.groupby(by=['year']).median()

    @staticmethod
    def get_defect_density_df(file_path):
        """

        Returns
        -------

        """
        df = pd.read_csv(file_path, sep=',')
        return df

    @staticmethod
    def compare_defect_density_and_changes(changes_df, defect_df):
        """

        Returns
        -------

        """
        # changes_df['year'] = changes_df['date'].apply(lambda t: t.year)
        x = list()
        y = list()
        for year in range(2016, 2020):
            defect_df_year = defect_df[(defect_df.year == year) & (defect_df.loc != 0)]
            dd = defect_df_year['defect_density'].values[0]
            y.append(dd)
            changes_df_year = changes_df[changes_df.year == year]
            c = changes_df_year['num_changes'].values[0]
            x.append(c)
        newdf = pd.DataFrame(data={'num_changes': x, 'defect_density': y})
        f = os.path.join(os.getenv('BASE_DIR'), 'data', 'metrics', 'changes_defects.csv')
        newdf.to_csv(f)
        PlotMgr.reg_plot(df=newdf, x='num_changes', y='defect_density', xlabel='num_changes', ylabel='defect_density',
                         filename='scatter_plot_changes_defect_density.pdf')

    @staticmethod
    def _is_bug_fix(comment):
        """

        Parameters
        ----------
        comment: str

        Returns
        -------

        """
        words = comment.split()
        if any(w in words for w in ['fix', 'bug', 'error', 'erro', 'falha', 'fail', 'bug-fix', 'correction']):
            return True
        else:
            return False

    def compute_bug_per_loc_ratio(self, service_names, time_bins):
        """

        Parameters
        ----------
        service_names: List[str]
        time_bins: Tuple[datetime]

        Returns
        -------
        pd.DataFrame
        """
        print('Getting bugs and changes from services: {}'.format(service_names))
        # connecting to the database
        issue_mgr = IssueMgr(path_to_db=self.db_path)
        service_mgr = ServiceMgr(db_path=self.db_path)

        # initializing variables
        bugs = list()
        loc_list = list()
        date_list = list()
        service_list = list()

        # for each service
        for service_name in service_names:
            print('Analysing the defect density of {} service'.format(service_name))
            service = service_mgr.get_service(service_name=service_name)
            loc_aux = self.compute_loc(service, time_bins)
            loc_list.extend(loc_aux)
            for i in range(1, len(time_bins)):

                prev_t = time_bins[i-1]
                cur_t = time_bins[i]
                bugs_counter = 0

                for repo_data in service.list_repository_data():
                    repo = repo_data.get('repository')  # type: Repository
                    # print('Getting issues from {}'.format(repo.name))
                    issues = issue_mgr.get_issues_by_label(repository_id=repo.repository_id)

                    for issue in issues:
                        if issue.closed_at is not None and prev_t <= issue.closed_at < cur_t:
                            bugs_counter += 1

                    for c in repo.commits:
                        if prev_t <= c.date < cur_t:
                            if DataMgr._is_bug_fix(comment=c.comment) \
                                    and not DataMgr.closed_using_keywords(line=c.comment):
                                print('{}: {}'.format(service.name, c.comment))
                                bugs_counter += 1
                bugs.append(bugs_counter)
                service_list.append(service_name)
                date_list.append(cur_t)
            assert len(loc_list) == len(bugs) == len(date_list) == len(service_list)
        temp_df = pd.DataFrame(data={'service': service_list, 'date': date_list, 'bugs': bugs, 'loc': loc_list})
        temp_filename = os.path.join(os.getenv('BASE_DIR'), 'data', 'metrics', 'defect_density.csv')
        temp_df.to_csv(temp_filename, sep=',')
        temp_df = temp_df[temp_df['loc'] != 0]
        temp_df.loc[:, 'year'] = temp_df['date'].apply(lambda t: t.year)
        df_group = temp_df.groupby(by=['year'], as_index=False).sum()
        df_group['defect_density'] = df_group['bugs'] / (df_group['loc'] / 1000)

        df_group.to_csv(os.path.join(os.getenv('BASE_DIR'), 'data', 'metrics', PlotMgr.DEFECT_DENSITY_CSV_FILE))
        return df_group

    def compute_bugs(self, service: Service, time_bins: Tuple[datetime]):
        """

        Parameters
        ----------
        service:Service
        time_bins: List[datetime]

        Returns
        -------
        pd.DataFrame
        """
        print('Getting bugs from {}'.format(service.name))
        # connecting to the database
        issue_mgr = IssueMgr(path_to_db=self.db_path)

        # initializing variables
        bugs = list()
        # sorted_time_bins = sorted(time_bins)
        # for each service
        for i in range(1, len(time_bins)):
            prev_t = time_bins[i-1]
            cur_t = time_bins[i]
            bugs_counter = 0
            for repo_data in service.list_repository_data():
                repo = repo_data.get('repository')
                # print('Getting issues from {}'.format(repo.name))
                issues = issue_mgr.get_issues_by_label(repository_id=repo.name)

                for issue in issues:
                    if issue.closed_at is not None and prev_t <= issue.closed_at < cur_t:
                        bugs_counter += 1

                for c in repo.commits:
                    if prev_t <= c.date < cur_t:
                        if any(w in c.comment for w in ['fix', 'bug', 'error', 'erro', 'falha', 'fail', 'correction']) \
                                and not DataMgr.closed_using_keywords(line=c.comment):
                            bugs_counter += 1
            bugs.append(bugs_counter)

        return bugs

