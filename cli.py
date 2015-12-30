#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 13:58:24 2015

@author: mdurant
"""

import argparse
import hdfs
import sys
import inspect

parser = argparse.ArgumentParser(description='HDFS commands')
parser.add_argument("command", help="filesystem command to run")
parser.add_argument("par1", help="", nargs="?", default=None)
parser.add_argument("par2", help="", nargs="?", default=None)
parser.add_argument("par3", help="", nargs="?", default=None)
parser.add_argument('--port', type=int, 
                   help='Name node port')
parser.add_argument('--host', type=str,
                   help='Name node address')
parser.add_argument('--verbose', type=int, default=0,
                   help='Verbosity')

args = parser.parse_args()
par1, par2, par3 = args.par1, args.par2, args.par3
if args.verbose > 0:
    print(args)

commands = ['ls', 'cat', 'info', 'mkdir', 'rmdir', 'rm', 'mv', 'exists',
            'chmod', 'chmown', 'set_replication', 'get_block_locations',
            'get', 'getmerge', 'put', 'du', 'tail', 'df']

if __name__ == "__main__":
    if args.command not in commands:
        print("Available commands:", list(sorted(commands)))
        sys.exit(1)
    kwargs = {}
    if args.host:
        kwargs['host'] = args.host
    if args.port:
        kwargs['port'] = args.port
    fs = hdfs.HDFileSystem(**kwargs)
    cmd = getattr(fs, args.command)
    nargs = len(inspect.getargspec(cmd).args) - 1
    args = (par1, par2, par3)[:nargs]
    out = cmd(*args)
    if isinstance(out, list):
        for l in out:
            print(l)
    elif hasattr(out, 'decode'):
        print(out.decode())
    elif out is not None:
        print(out)
