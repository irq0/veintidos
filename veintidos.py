#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

import logging
import argparse

from rados import Rados, ObjectExists

import veintidos.cas as cas
from veintidos.chunk import Chunker

RADOS = None
CAS = None
CHUNKER = None
IOCTX_CAS = None
IOCTX_INDEX = None


def setup_rados(args):
    global RADOS
    RADOS = Rados(conffile='')
    RADOS.connect()

    try:
        RADOS.create_pool(args.pool)
    except ObjectExists:
        pass

    global IOCTX_CAS
    global IOCTX_INDEX
    IOCTX_CAS = RADOS.open_ioctx(args.pool)
    IOCTX_INDEX = RADOS.open_ioctx(args.pool)

    global CAS
    global CHUNKER

    if args.no_compression:
        CAS = cas.CAS(IOCTX_CAS)
    else:
        CAS = cas.CompressedCAS(IOCTX_CAS)

    if "chunk_size" in args:
        CHUNKER = Chunker(CAS, IOCTX_INDEX, chunk_size=args.chunk_size)
    else:
        CHUNKER = Chunker(CAS, IOCTX_INDEX)


def cmd_ls(args):

    if args.cas:
        print "CAS Objects:"
        for obj, refcount in CAS.list():
            print obj, "#", refcount

    print

    if args.index:
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


def parse_cmdline():
    parser = argparse.ArgumentParser(prog="veintidos client")
    parser.add_argument("--pool", type=str, default="veintidos", help="Ceph pool")
    parser.add_argument("--no-compression", action="store_true", help="Don't use compressed CAS writer/reader")
    parser.add_argument("--debug", action="store_const", dest="loglevel",
                        const=logging.DEBUG)
    parser.add_argument("--verbose", action="store_const", dest="loglevel",
                        const=logging.INFO)

    subparsers = parser.add_subparsers()

    ls_parser = subparsers.add_parser(
        "ls", help="List CAS and INDEX objects")
    ls_parser.add_argument('--no-index', action="store_false", dest="index")
    ls_parser.add_argument('--no-cas', action="store_false", dest="cas")
    ls_parser.set_defaults(func=cmd_ls)

    put_parser = subparsers.add_parser(
        "put", help="Store file to CAS pool and write INDEX")
    put_parser.add_argument("--chunk-size", type=int, default=(4 * 1024**2), help="Size of chunks in byte")
    put_parser.add_argument('name', type=str)
    put_parser.add_argument('file', type=argparse.FileType('rb'))
    put_parser.set_defaults(func=cmd_put)

    rm_parser = subparsers.add_parser(
        "rm", help="Remove file")
    rm_parser.add_argument('name', type=str)
    rm_parser.add_argument('--version', type=str, default="ALL")
    rm_parser.set_defaults(func=cmd_rm)

    get_parser = subparsers.add_parser(
        "get", help="Retrieve file from CAS pool")
    get_parser.add_argument('name', type=str)
    get_parser.add_argument('file', type=argparse.FileType('w+b'))
    get_parser.add_argument('--version', type=str, default="HEAD")
    get_parser.set_defaults(func=cmd_get)

    return parser.parse_args()


def setup_logging(args):
    logging.basicConfig(level=args.loglevel,
                        format=""
                        "%(asctime)-15s %(levelname)-9s"
                        "%(threadName)-12s %(name)-15s %(filename)s:%(lineno)s -> %(funcName)s():"
                        "\n\t"
                        "%(message)s")

def main():
    args = parse_cmdline()
    setup_logging(args)
    setup_rados(args)
    args.func(args)

if __name__ == "__main__":
    main()
