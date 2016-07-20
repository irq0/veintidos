#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

from StringIO import StringIO

from nose.tools import eq_ as eq, assert_raises

from rados import Rados, ObjectNotFound

from vaceph.cas import CAS, CompressedCAS
from vaceph.chunk import Chunker

from util import random_id, random_bytes, eq_buffer

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


def teardown_module():
    global pool_name
    rados.delete_pool(pool_name)


def test_chunker_no_litter():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(chunker.chunk_size*4))
    obj_name = random_id()

    chunker.write_full(obj_name, data_in)
    chunker.remove_all_versions(obj_name)

    cas_objs = [x.key for x in ioctx_cas.list_objects()]
    index_objs = [x.key for x in ioctx_index.list_objects()]

    print "CAS objects left:", cas_objs
    print "Index objects left:", index_objs

    eq(len(cas_objs), 0)
    eq(len(index_objs), 0)


def test_cas_put_get():
    cas = CAS(ioctx_cas)

    data_in = random_bytes(99)

    obj_name = cas.put(data_in)
    eq_buffer(data_in, cas.get(obj_name))


def test_cas_put_deduplicatable_content():
    cas = CAS(ioctx_cas)

    data_in = "\x00" * (4 * 1024**2)

    obj_name_1 = cas.put(data_in)
    obj_name_2 = cas.put(data_in)

    eq(obj_name_1, obj_name_2)


def test_compressed_cas_put_get():
    ccas = CompressedCAS(ioctx_cas)

    data_in = random_bytes(8*1024**2)
    obj_name = ccas.put(data_in)
    eq_buffer(data_in, ccas.get(obj_name, size=8*1024**2))

    data_in = "\xFF" * 11*1024**2
    obj_name = ccas.put(data_in)
    eq_buffer(data_in, ccas.get(obj_name, size=11*1024**2))

    data_in = "\x00" * 42
    obj_name = ccas.put(data_in)
    eq_buffer(data_in, ccas.get(obj_name))


def test_chunker_put_get_single():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(42))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq_buffer(data_in.getvalue(), data_out.getvalue())


def test_chunker_put_get_multiple():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(chunker.chunk_size*4))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq_buffer(data_in.getvalue(), data_out.getvalue())


def test_chunker_put_get_multiple_fraction():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(int(chunker.chunk_size*1.5)))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq_buffer(data_in.getvalue(), data_out.getvalue())


def test_chunker_versions():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(10*1024**1))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    eq(len(chunker.versions(obj_name)), 1)

    eq(version, chunker.head_version(obj_name))
    eq(version, chunker.versions(obj_name)[0])


def test_chunker_multiple_versions():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = random_bytes(42)
    obj_name = random_id()

    versions = (
        chunker.write_full(obj_name, StringIO(data_in)),
        chunker.write_full(obj_name, StringIO(data_in)),
        chunker.write_full(obj_name, StringIO(data_in)),
        chunker.write_full(obj_name, StringIO(data_in)),
        chunker.write_full(obj_name, StringIO(data_in)),
    )

    eq(len(versions), len(chunker.versions(obj_name)))

    eq(versions[-1], chunker.head_version(obj_name))
    eq(versions[0], chunker.versions(obj_name)[0])


def test_chunker_remove():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = random_bytes(42)
    obj_name = random_id()

    version = chunker.write_full(obj_name, StringIO(data_in))
    chunker.remove_version(obj_name, version)

    # object gone
    eq(chunker.head_version(obj_name), None)

    chunker.write_full(obj_name, StringIO(data_in))
    chunker.write_full(obj_name, StringIO(data_in))
    chunker.remove_all_versions(obj_name)

    # object gone
    assert_raises(ObjectNotFound, chunker.head_version, obj_name)


def test_chunker_partial_read():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO("\x00" * chunker.chunk_size +
                       "\xFF" * chunker.chunk_size)
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    middle = chunker.chunk_size / 2

    buf = chunker.read(obj_name, chunker.chunk_size, middle, version)

    eq(len(buf), chunker.chunk_size)
    eq_buffer("\x00" * (chunker.chunk_size/2) +
              "\xFF" * (chunker.chunk_size/2), buf)


def test_chunker_partial_read_past_size():
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO("\x00" * chunker.chunk_size)
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    buf = chunker.read(obj_name, chunker.chunk_size, chunker.chunk_size, version)

    eq_buffer(buf, "")
