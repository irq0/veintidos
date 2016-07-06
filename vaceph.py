#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

from StringIO import StringIO

import argparse

from rados import Rados, ObjectNotFound, ObjectExists

from vaceph.cas import CAS as CAS_
from vaceph.chunk import Chunker

RADOS = None
POOL_NAME = "vaceph"
CAS = None
CHUNKER = None
IOCTX_CAS = None
IOCTX_INDEX = None

def setup_rados():
    global RADOS
    RADOS = Rados(conffile='')
    RADOS.connect()

    global POOL_NAME
    try:
        RADOS.create_pool(POOL_NAME)
    except ObjectExists:
        pass

    global IOCTX_CAS
    global IOCTX_INDEX
    IOCTX_CAS = RADOS.open_ioctx(POOL_NAME)
    IOCTX_INDEX = RADOS.open_ioctx(POOL_NAME)

    global CAS
    global CHUNKER
    CAS = CAS_(IOCTX_CAS)
    CHUNKER = Chunker(CAS, IOCTX_INDEX)


def cmd_ls(args):
    print "CAS Objects:"
    for obj, refcount in CAS.list():
        print obj, "#", refcount

    print
    print "Index Objects:"
    objs = [o.key for o in IOCTX_INDEX.list_objects()
            if o.nspace == "INDEX"]

    for obj in objs:
        print obj, "→", ", ".join(CHUNKER.versions(obj))


def cmd_put(args):
    version = CHUNKER.write_full(args.name, args.file)
    print "Wrote {} version {}".format(args.name, version)


def cmd_rm(args):
    if args.version == "ALL":
        CHUNKER.remove_all_versions(args.name)
    else:
        CHUNKER.remove_version(args.name, args.version)

def cmd_get(args):
    CHUNKER.read_full(args.name, args.file, args.version)


def main():
    parser = argparse.ArgumentParser(prog="vaceph client")

    subparsers = parser.add_subparsers()

    ls_parser = subparsers.add_parser("ls")
    ls_parser.set_defaults(func=cmd_ls)

    put_parser = subparsers.add_parser("put")
    put_parser.add_argument('name', type=str)
    put_parser.add_argument('file', type=argparse.FileType('r'))
    put_parser.set_defaults(func=cmd_put)

    rm_parser = subparsers.add_parser("rm")
    rm_parser.add_argument('name', type=str)
    rm_parser.add_argument('--version', type=str, default="ALL")
    rm_parser.set_defaults(func=cmd_rm)

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument('name', type=str)
    get_parser.add_argument('file', type=argparse.FileType('w'))
    get_parser.add_argument('--version', type=str, default="HEAD")
    get_parser.set_defaults(func=cmd_get)


    args = parser.parse_args()

    setup_rados()

    args.func(args)

if __name__ == "__main__":
    main()
