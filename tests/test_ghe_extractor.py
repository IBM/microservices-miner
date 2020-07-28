# (C) Copyright IBM Corporation 2017, 2018, 2019
# U.S. Government Users Restricted Rights:  Use, duplication or disclosure restricted
# by GSA ADP Schedule Contract with IBM Corp.
#
# Author: Leonardo P. Tizzei <ltizzei@br.ibm.com>
from unittest import TestCase
import os
from microservices_miner.mining.ghe_extractor import GHEExtractor
import json
from microservices_miner.control.service_mgr import ServiceMgr
from datetime import datetime


class TestGHEExtractor(TestCase):

    def setUp(self):
        self.db_path = os.getenv('DB_PATH')
        self.ghe_extractor = GHEExtractor(db_path=self.db_path)

    def test_extract_new_data_from_ghe(self):
        f = os.path.join(os.getcwd(), 'microservices_miner', 'example.json')
        api_token = os.getenv('MINING_GHE_PERSONAL_ACCESS_TOKEN')
        with open(f) as file:
            input_data = json.load(file)
            self.ghe_extractor.extract_new_data_from_ghe(service_list=input_data,
                                                         db_path=self.db_path, api_token=api_token)
        service_mgr = ServiceMgr(db_path=self.db_path)
        for s in input_data:
            name = s.get('name')
            service_names = service_mgr.list_all_service_names()
            self.assertIn(name, service_names)
            service = service_mgr.get_service(service_name=name)
            start_dt = datetime.strptime(s.get('start_date'), '%Y-%m-%d').date()
            self.assertEqual(service.start_date, start_dt)
            input_repos = s.get('repositories')
            for input_repo in input_repos:
                found = False
                for repo_data in service.list_repository_data():
                    repo = repo_data.get('repository')
                    if repo.name == input_repo.get('name'):
                        repo_start_date = input_repo.get('start_date')
                        start_date = repo_data.get('start_date')
                        self.assertEqual(repo_start_date, start_date)

                        repo_end_date = input_repo.get('end_date')
                        end_date = repo_data.get('end_date')
                        self.assertEqual(repo_end_date, end_date)
                        found = True
                self.assertTrue(found)
