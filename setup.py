#!/usr/bin/env python

import os
from setuptools import setup

setup(name='libhdfs3',
      version='0.0.1',
      description='Python wrappers for libhdfs3, a native HDFS client',
      url='http://github.com/mdurant/libhdfs3-ctypes/',
      maintainer='Martin Durant',
      maintainer_email='mdurant@continuum.io',
      license='MIT',
      keywords='hdfs',
      packages=['libhdfs3'],
      install_requires=[],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      scripts=[os.path.join('bin', 'cli.py')],
      zip_safe=False)
