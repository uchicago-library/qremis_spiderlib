from setuptools import setup, find_packages

def readme():
    with open("README.md", 'r') as f:
        return f.read()

setup(
    name = "qremis_spiderlib",
    description = "A crawler library for the qremis_api",
    long_description = readme(),
    packages = find_packages(
        exclude = [
        ]
    ),
    dependency_links = [
        'https://github.com/bnbalsamo/pyqremis' +
        '/tarball/master#egg=pyqremis'
    ],
    install_requires = [
        'redlock',
        'requests',
        'pyqremis'
    ],
)
