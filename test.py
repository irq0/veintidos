#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

#LD_LIBRARY_PATH="/srv/ceph-devel/src/src/.libs:/srv/ceph-devel/src/src/build/lib.linux-x86_64-2.7" ipython

from StringIO import StringIO
import sys
import os
import logging
import uuid
import binascii

import vaceph
from cas import CAS
from chunk import Chunker

logging.basicConfig(level=logging.DEBUG)

def main():
    cluster = vaceph.setup_ceph_local_dev()

    ioctx_cas = cluster.open_ioctx("rbd")
    ioctx_index = cluster.open_ioctx("rbd")

    print "io ctx", ioctx_cas, ioctx_index

    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)


    foo = cas.put("foo")
    print "put(foo) ->", foo
    bar = cas.get(foo)
    print "get(..) ->", bar

    print "write_full:", chunker.write_full("test", open(sys.argv[1], "r"))
    print "versions:", chunker.versions("test")
    print "head_version", chunker.head_version("test")
    print "read_full:", chunker.read_full("test", open(sys.argv[2], "w"))

    print "TESTS"
    # TODO build unit tests

    test_str = os.urandom(3*1024**2)
    obj_name = str(uuid.uuid4())

    version = chunker.write_full(obj_name, StringIO(test_str))
    out = StringIO()
    chunker.read_full(obj_name, out, version)
    print "->", test_str == out.getvalue()

    print "->", len(chunker.versions(obj_name)) == 1
    print "->", chunker.head_version(obj_name) == version

    out = StringIO()
    chunker.read_full(obj_name, out, version="HEAD")
    print "->", test_str == out.getvalue()


if __name__ == '__main__':
    main()
