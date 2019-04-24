from setuptools import setup, find_packages

setup(
    name="com",
    version="0.1",
    packages=find_packages(), install_requires=['lxml', 'simplejson']
)