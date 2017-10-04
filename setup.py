#!/usr/bin/env python
import re
import sys
import os

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

__license__ = """
Copyright 2016 Parsely, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


# Get version without importing, which avoids dependency issues
def get_version():
    with open('parsely_raw_data/__init__.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')

lint_requires = [
    'pep8',
    'pyflakes'
]


def read_lines(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.readlines()

tests_require = [
    x.strip() for x in read_lines('test-requirements.txt') if not x.startswith('-')
]

install_requires = [
    x.strip() for x in read_lines('requirements.txt') if not x.startswith('-')
]

dependency_links = []
setup_requires = []


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')


def run_setup():
    setup(
        name='parsely_raw_data',
        version=get_version(),
        author='Emmett Butler',
        author_email='support@parsely.com',
        url='https://github.com/Parsely/parsely_raw_data',
        description='Utilities for accessing raw Parse.ly data',
        keywords='parsely s3 kinesis redshift firehose bigquery',
        license='Apache License 2.0',
        packages=find_packages(),
        entry_points={
            'console_scripts': [
                'parsely_bigquery = parsely_raw_data.bigquery:main',
                'parsely_redshift = parsely_raw_data.redshift:main',
                'parsely_s3 = parsely_raw_data.s3:main',
                'parsely_stream = parsely_raw_data.stream:main',
                'parsely_schema = parsely_raw_data.docgen:main'
            ]
        },
        install_requires=install_requires,
        tests_require=tests_require,
        setup_requires=setup_requires,
        extras_require={
            'test': tests_require,
            'all': install_requires + tests_require,
            'docs': tests_require,
            'lint': lint_requires
        },
        cmdclass={'test': PyTest},
        ext_modules=[],
        dependency_links=dependency_links,
        test_suite='nose.collector',
        include_package_data=True,
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.4",
            "Programming Language :: Python :: 3.5",
            "Topic :: Software Development :: Libraries :: Python Modules"
        ],
    )

run_setup()
