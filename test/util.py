#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓
import uuid
import os
import filecmp

from vaceph.cas import fingerprint


def random_bytes(size=4*1024**2):
    return os.urandom(size)


def zeros(size=4*1024**2):
    return "\x00" * size


def random_id():
    return str(uuid.uuid4())


def random_fp():
    return fingerprint(random_id())[1]


def eq_buffer(x, y):
    if x != y:
        assert False, "x != y ([:16]..[-16:]): %r..%r != %r..%r  " % \
            (x[:16], x[-16:], y[:16], y[-16:])


def eq_file(x, y):
    if not filecmp.cmp(x, y, shallow=False):
        assert False, "Files %s and %s differ" % (x, y)
