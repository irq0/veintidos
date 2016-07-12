#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

import base64
import binascii
import json
from nose.tools import eq_ as eq, assert_raises

from rados import Error, Rados

from util import random_fp, random_bytes, random_id

rados = None
pool_name = None
ioctx = None


def setup_module():
    global rados
    rados = Rados(conffile='')
    rados.connect()

    global pool_name
    pool_name = random_id()
    rados.create_pool(pool_name)

    global ioctx
    ioctx = rados.open_ioctx(pool_name)


def teardown_module():
    global pool_name
    rados.delete_pool(pool_name)


def test_put_correct():
    fp = random_fp()
    data = random_bytes(100)

    meta = {
        "fp_algo": "test",
        "lib": "vaceph_unittests",
        "compression": "no",
    }

    args = {
        "data": base64.b64encode(data),
        "meta": [{"key": k, "val": v} for k, v in meta.iteritems()],
    }

    jargs = json.dumps(args)

    ret, _ = ioctx.execute(fp, "cas", "put", jargs)

    eq(ret, 0)


def test_put_broken():
    fp = random_fp()
    data = random_bytes(100)

    meta = {
        "fp_algo": "test",
        "lib": "vaceph_unittests",
        "compression": "no",
    }

    flattened_meta = [{"key": k, "val": v} for k, v in meta.iteritems()]

    assert_raises(Error, ioctx.execute,
                  fp, "cas", "put", json.dumps({"meta": flattened_meta}))
    assert_raises(Error, ioctx.execute,
                  fp, "cas", "put",
                  json.dumps({"data": base64.b64encode(data)}))
    assert_raises(Error, ioctx.execute, fp, "cas", "put",
                  json.dumps({"data": binascii.b2a_hex(data)}))
    assert_raises(Error, ioctx.execute, fp, "cas", "put", "")
    assert_raises(Error, ioctx.execute, fp, "cas", "put", "{}")
    assert_raises(Error, ioctx.execute, fp, "cas", "put", "[]")


def test_up_down():
    fp = random_fp()
    data = random_bytes(100)

    meta = {
        "fp_algo": "test",
        "lib": "vaceph_unittests",
        "compression": "no",
    }

    args = {
        "data": base64.b64encode(data),
        "meta": [{"key": k, "val": v} for k, v in meta.iteritems()],
    }

    jargs = json.dumps(args)

    # refcount = 1
    ret, _ = ioctx.execute(fp, "cas", "put", jargs)
    eq(ret, 0)

    # refcount = 2
    ret, foo = ioctx.execute(fp, "cas", "up", "")
    eq(ret, 0)

    # refcount = 3
    ret, _ = ioctx.execute(fp, "cas", "up", "")
    eq(ret, 0)

    # refcount = 2
    ret, _ = ioctx.execute(fp, "cas", "down", "")
    eq(ret, 0)

    # refcount = 1
    ret, _ = ioctx.execute(fp, "cas", "down", "")
    eq(ret, 0)

    # refcount = 0 => obj gone
    ret, _ = ioctx.execute(fp, "cas", "down", "")
    eq(ret, 0)

    assert_raises(Error, ioctx.execute, fp, "cas", "down", "")
