#!/usr/bin/env python
from setuptools import setup

setup(
    name='django-s3',
    version='0.1',
    description='Simple S3 storage backend',
    author='Jochem Oosterveen',
    author_email='jochem@oosterveen.net',
    url='https://github.com/jochem/django-s3',
    keywords='django amazon s3',
    packages=['django_s3'],
    install_requires=['Django', 'boto3'])
