#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓

"""
CAS / Chunker Unit Tests


Classes under test:

- CAS
- Chunker

"""

from StringIO import StringIO

from nose.tools import eq_ as eq, assert_raises

from rados import Rados, ObjectNotFound

from veintidos.cas import CAS, Compressor
from veintidos.chunk import Chunker

from util import random_id, random_bytes, eq_buffer

# = Setup / Teardown =

# Use single RADOS connection for the module. Create new pool for tests
# and delete it afterwards

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


# = Tests =

# == CAS Class ==

def test_chunker_no_litter():
    """
    Test: Write and immediate remove should not leave any object behind
    """

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
    """
    Test: `get(put(x)) == x` with random buffer content
    """
    cas = CAS(ioctx_cas)

    data_in = random_bytes(99)

    obj_name = cas.put(data_in)
    eq_buffer(data_in, cas.get(obj_name))


def test_cas_put_deduplicatable_content():
    """
    Test: `get(put(x)) == x` with deduplicatable content (zeros)
    """
    cas = CAS(ioctx_cas)

    data_in = "\x00" * (4 * 1024**2)

    obj_name_1 = cas.put(data_in)
    obj_name_2 = cas.put(data_in)

    eq(obj_name_1, obj_name_2)


def compressed_cas_put_get(cas):
    """
    Test Utility: `get(put(x)) == x` for given `cas` and x in `{random, 1s, 0s}`
    """
    data_in = random_bytes(8*1024**2)
    obj_name = cas.put(data_in)
    eq_buffer(data_in, cas.get(obj_name, size=8*1024**2))
    cas.down(obj_name)

    data_in = "\xFF" * 11*1024**2
    obj_name = cas.put(data_in)
    eq_buffer(data_in, cas.get(obj_name, size=11*1024**2))
    cas.down(obj_name)

    data_in = "\x00" * 42
    obj_name = cas.put(data_in)
    eq_buffer(data_in, cas.get(obj_name))
    cas.down(obj_name)


def test_compressed_cas_put_get():
    """
    Test: `get(put(x)) == x` for all available compressors
    """
    for compression in Compressor.supported():
        print compression
        cas = CAS(ioctx_cas, compression=compression)
        compressed_cas_put_get(cas)


def test_mixed_compression():
    """
    Test: Uncompressed put and compressed put afterwards
    """

    # put something uncompressed and then put new objs with compression

    cas = CAS(ioctx_cas, compression="no")
    data_in = "\xFF" * 11*1024**2
    obj_name = cas.put(data_in)
    eq_buffer(data_in, cas.get(obj_name, size=11*1024**2))

    for compression in Compressor.supported():
        ccas = CAS(ioctx_cas, compression=compression)
        tmp_obj_name = ccas.put(data_in)
        eq_buffer(data_in, ccas.get(tmp_obj_name, size=11*1024**2))

    eq_buffer(data_in, cas.get(obj_name, size=11*1024**2))


# == Chunker Class ==

def test_chunker_put_get_single():
    """
    Test: read(write(x)) = x for x filling only a single chunk
    """
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(42))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq_buffer(data_in.getvalue(), data_out.getvalue())


def test_chunker_put_get_multiple():
    """
    Test: read(write(x)) = x for x spread over multiple chunks. Every chunk filled
    """
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(chunker.chunk_size*4))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq_buffer(data_in.getvalue(), data_out.getvalue())


def test_chunker_put_get_multiple_fraction():
    """
    Test: read(write(x)) = x for x spread over multiple chunks. With partially filled chunks
    """
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(int(chunker.chunk_size*1.5)))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    data_out = StringIO()
    chunker.read_full(obj_name, data_out, version)

    eq_buffer(data_in.getvalue(), data_out.getvalue())


def test_chunker_versions():
    """
    Test: versions / head_version returns version of last write_full. Single write_full
    """
    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO(random_bytes(10*1024**1))
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    eq(len(chunker.versions(obj_name)), 1)

    eq(version, chunker.head_version(obj_name))
    eq(version, chunker.versions(obj_name)[0])


def test_chunker_multiple_versions():
    """
    Test: versions / head_version return version of last write_full. Multiple write_full
    """
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
    """
    Test: remove actually removes

    - `remove_version(write_full)`: No versions, but index object
    - `write_full, write_full, remove_all_versions`: Index object gone
    """

    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = random_bytes(42)
    obj_name = random_id()

    version = chunker.write_full(obj_name, StringIO(data_in))
    chunker.remove_version(obj_name, version)

    eq(chunker.head_version(obj_name), None)

    chunker.write_full(obj_name, StringIO(data_in))
    chunker.write_full(obj_name, StringIO(data_in))
    chunker.remove_all_versions(obj_name)

    assert_raises(ObjectNotFound, chunker.head_version, obj_name)


def test_chunker_partial_read():
    """
    Test: partial reads using chunker.read with different input and weird extents
    """
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
    """
    Test: partial reads past *file* size
    """

    cas = CAS(ioctx_cas)
    chunker = Chunker(cas, ioctx_index)

    data_in = StringIO("\x00" * chunker.chunk_size)
    obj_name = random_id()

    version = chunker.write_full(obj_name, data_in)

    buf = chunker.read(obj_name, chunker.chunk_size, chunker.chunk_size, version)

    eq_buffer(buf, "")
