#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

from StringIO import StringIO

from nose.tools import eq_ as eq

from rados import Rados

from vaceph.cas import CAS
from vaceph.chunk import Chunker

from util import random_id, random_bytes

rados = None
pool_name = None
ioctx_cas = None
ioctx_index = None


def setup_module():
    global rados
    rados = Rados(conffile='')
    rados.connect()

    global pool_name
    pool_name = random_id()
    rados.create_pool(pool_name)

    global ioctx_cas
    global ioctx_index
    ioctx_cas = rados.open_ioctx(pool_name)
    ioctx_index = rados.open_ioctx(pool_name)


def test_cas_put_get():
    cas = CAS(ioctx_cas)

    data_in = random_bytes(99)

    obj_name = cas.put(data_in)
    eq(data_in, cas.get(obj_name))


def test_chunker_put_get_single():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(42))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq(data_in.getvalue(), data_out.getvalue())


def test_chunker_put_get_multiple():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(chunker.chunk_size*4))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq(data_in.getvalue(), data_out.getvalue())


def test_chunker_versions():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(10*1024**1))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    eq(len(chunker.versions(obj_name)), 1)

    eq(version, chunker.head_version(obj_name))
    eq(version, chunker.versions(obj_name)[0])
