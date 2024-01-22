from setuptools import setup, find_packages

setup(
    name='NewDay',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pyspark==3.1.2',
        'pytest==7.3.0',
    ],
)