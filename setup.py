from setuptools import setup, find_packages


def readme():
    with open("README.md", 'r') as f:
        return f.read()


setup(
    name="qremis_spiderlib",
    description="Library for crawling a qremis database and performing actions",
    version="0.0.1",
    long_description=readme(),
    author="Brian Balsamo",
    author_email="brian@brianbalsamo.com",
    packages=find_packages(
        exclude=[
        ]
    ),
    include_package_data=True,
    url='https://github.com/bnbalsamo/qremis_spiderlib',
    dependency_links=[
        'https://github.com/bnbalsamo/pyqremis' +
        '/tarball/master#egg=pyqremis'
    ],
    install_requires=[
        'redlock',
        'requests',
        'pyqremis'
    ],
    tests_require=[
        'pytest'
    ],
    test_suite='tests'
)
