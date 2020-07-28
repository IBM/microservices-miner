#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'matplotlib==3.3.0',
    'numpy==1.19.1',
    'pandas==1.0.5',
    'requests==2.24.0',
    'scikit-learn==0.23.1',
    'scipy==1.5.2',
    'seaborn==0.10.1',
    'pytz==2020.1'
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Leonardo Pondian Tizzei",
    author_email='ltizzei@br.ibm.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="A mining tool for collecting microservices data from Github repositories",
    entry_points={
        'console_scripts': [
            'microservices_miner=microservices_miner.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='microservices_miner',
    name='microservices_miner',
    packages=find_packages(include=['microservices_miner', 'microservices_miner.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/ltizzei/microservices_miner',
    version='0.1.0',
    zip_safe=False,
)
