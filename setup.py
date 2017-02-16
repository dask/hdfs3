#!/usr/bin/env python

import os
from setuptools import setup

setup(name='hdfs3',
      version='0.1.3',
      description='Python wrappers for libhdfs3, a native HDFS client',
      url='http://github.com/dask/hdfs3/',
      maintainer='Martin Durant',
      maintainer_email='mdurant@continuum.io',
      license='BSD',
      keywords='hdfs',
      packages=['hdfs3'],
      install_requires=[],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False)
