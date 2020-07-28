
microservices-miner
===================

A mining tool for collecting microservices data from Github repositories


## Installation
This tool has been implemented using python 3.7. Create your virtual environment and install the dependencies using pip.

```shell script
pip install -r requirements.txt
```

## Extracting data from GHE
1. Set `MINING_GHE_PERSONAL_ACCESS_TOKEN` environment variable
    - Go to Github
    - Click on `Settings > Developer settings`
    - Click on `Personal access tokens`
    - Click on `Generate new token`
2. Set  `DB_PATH` environment variable, which is the path to the database file (sqlite)
3. Set `BASE_DIR`, which is the directory that stores plots and CSVs files generated during analysis
4. Create the input file (see [example](microservices_miner/example.json))
5. Go to microservices-miner home dir
5. Run `python microservices_miner/mining/ghe_extractor.py --path <full-path-to-input-data>` and check the log file `github_miner.log`


## License

[MIT license](LICENSE)
