#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓
import uuid
import os

from vaceph.cas import fingerprint


def random_bytes(size=4*1024**2):
    return os.urandom(size)


def random_id():
    return str(uuid.uuid4())


def random_fp():
    return fingerprint(random_id())[1]


def eq_buffer(x, y):
    if x != y:
        assert False, "x != y ([:16]..[-16:]): %r..%r != %r..%r  " % \
            (x[:16], x[-16:], y[:16], y[-16:])
