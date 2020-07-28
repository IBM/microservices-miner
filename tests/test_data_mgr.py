# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from unittest import TestCase
from microservices_miner.control.data_mgr import DataMgr
from microservices_miner.control.service_mgr import ServiceMgr
from microservices_miner.control.repository_mgr import RepositoryMgr
from microservices_miner.model.service import Service
from microservices_miner.model.repository import Repository
import os
import pandas as pd
import numpy as np
from datetime import datetime


class TestDataMgr(TestCase):

    def setUp(self) -> None:
        self.db_path = os.getenv('DB_PATH')
        self.data_mgr = DataMgr(db_path=self.db_path)

    def test_compute_loc_per_commit(self):
        service_mgr = ServiceMgr(self.db_path)
        repository_mgr = RepositoryMgr(self.db_path)
        max_absolute_error = 1000
        max_relative_error = 0.1
        service_names = service_mgr.list_all_service_names()
        for service_name in sorted(service_names):

            print('**********\n{}'.format(service_name))
            service = service_mgr.get_service(service_name=service_name)  # type: Service
            for repo_data in service.list_repository_data():
                temp_repo = repo_data.get('repository')  # type: Repository

                repo_sha_set = set()
                oracle_sha_set = set()
                repository = repository_mgr.get_repository(service_id=service.service_id,
                                                           repository_id=temp_repo.repository_id)
                test_oracle = os.path.join(os.getenv('BASE_DIR'), 'data', 'oracle',
                                           'service_id_{}_{}__oracle_cloc.csv'.format(service.service_id,
                                                                                      temp_repo.name))
                self.assertTrue(os.path.isfile(test_oracle), msg='Error! file does not exist: {}'.format(test_oracle))
                loc_list, sha_list, date_list = self.data_mgr.compute_loc_per_repository(repository=repository)
                df = pd.read_csv(test_oracle, index_col=0, sep=';')
                self.assertGreater(df.shape[0], 0)
                for i in df.index:
                    oracle_sha_set.add(i)
                self.assertGreater(len(loc_list), 0)
                for loc, sha, dt in zip(loc_list, sha_list, date_list):

                    if sha in df.index:
                        s = df.loc[sha, ['CLOC']]
                        l = s.tolist()
                        assert len(l) == 1, "Error! {}".format(l)
                        cloc = l.pop()
                        if loc == 0:
                            if cloc == 0:
                                ratio = 1.0
                            else:
                                continue
                        else:
                            ratio = cloc/loc
                        rel_diff = np.abs(ratio - 1.0)
                        print('SHA={} LOC={} CLOC={}'.format(sha, loc, cloc))
                        self.assertIsInstance(rel_diff, float,
                                              msg='Error! delta is {} type={}'.format(rel_diff, type(rel_diff)))
                        abs_diff = np.abs(loc - cloc)
                        print('{} relative diff = {} absolute diff = {}'.format(dt, rel_diff, abs_diff))
                        if rel_diff > max_relative_error:

                            self.assertGreater(max_absolute_error, abs_diff, msg='Error! sha={}'.format(sha))

                print('service={} len oracle = {} len GHE = {}'.format(service_name, len(oracle_sha_set),
                                                                       len(repo_sha_set)))
                print('service={} oracle - GHE = {}'.format(service_name, oracle_sha_set - repo_sha_set))
                print('service={} GHE - oracle = {}'.format(service_name, repo_sha_set - oracle_sha_set))
                print('service={} intersection {}'.format(service_name, repo_sha_set.intersection(oracle_sha_set)))

    def test_compute_loc_using_different_time_bins(self):
        service_mgr = ServiceMgr(self.db_path)
        service_names = service_mgr.list_all_service_names()
        until = datetime(year=2019, month=12, day=31)
        step_size = 182
        for service_name in service_names:
            service = service_mgr.get_service(service_name=service_name)
            loc_dict = dict()
            for i in [1, 2]:
                s = step_size * i
                time_bins = self.data_mgr.get_time_bins(service_names=service_names, step_size_aprox=s, until=until)
                print(time_bins)
                locs = self.data_mgr.compute_loc(service=service, time_bins=time_bins)
                for t, l in zip(time_bins[1:], locs):
                    t_str = t.isoformat()
                    if t_str in loc_dict.keys():
                        loc_dict[t_str].append(l)
                    else:
                        loc_dict[t_str] = [l]

            for k, v in loc_dict.items():
                if len(v) == 2:
                    l1 = v[0]
                    l2 = v[1]
                    self.assertEqual(l1, l2)

    def test_compute_bug_per_loc_ratio(self):
        """

        Returns
        -------

        """
        until = datetime(year=2019, month=12, day=31)
        service_mgr = ServiceMgr(db_path=self.db_path)
        service_names = service_mgr.list_all_service_names()
        time_bins = self.data_mgr.get_time_bins(service_names=service_names, step_size_aprox=365, until=until)
        df = self.data_mgr.compute_bug_per_loc_ratio(service_names=service_names, time_bins=time_bins)
        self.assertGreater(df.shape[0], 0)
        self.assertEqual(df.shape[1], 4)
        self.assertTrue(all(c in df.columns for c in ['year', 'defect_density', 'bugs', 'loc']))

    def test_compute_time_to_repair(self):
        service_mgr = ServiceMgr(self.db_path)
        service_names = service_mgr.list_all_service_names()
        df = self.data_mgr.compute_repair_time(service_names=service_names)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape[1], 3)
        self.assertIn('closed_at', df.columns)
        self.assertIn('time_to_repair', df.columns)
        self.assertIn('repository', df.columns)
        self.assertGreaterEqual(df.shape[0], 1)
