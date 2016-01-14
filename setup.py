#!/usr/bin/env python

import os
from setuptools import setup

setup(name='hdfs3',
      version='0.0.1',
      description='Python wrappers for libhdfs3, a native HDFS client',
      url='http://github.com/blaze/hdfs3/',
      maintainer='Martin Durant',
      maintainer_email='mdurant@continuum.io',
      license='MIT',
      keywords='hdfs',
      packages=['hdfs3'],
      install_requires=[],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      scripts=[os.path.join('bin', 'hdfs3')],
      zip_safe=False)
