#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓

"""
= Utility Functions for Unit Tests =
"""

import uuid
import os
import filecmp

from veintidos.cas import fingerprint

# === Generate content for test objects ===

def random_bytes(size=4*1024**2):
    return os.urandom(size)


def zeros(size=4*1024**2):
    return "\x00" * size

# === Generate ids for test objects ===

def random_id():
    return str(uuid.uuid4())


def random_fp():
    return fingerprint(random_id())[1]

# === Special equality assertions ===

def eq_buffer(x, y):
    if x != y:
        assert False, "x != y ([:16]..[-16:]): %r..%r != %r..%r  " % \
            (x[:16], x[-16:], y[:16], y[-16:])


def eq_file(x, y):
    if not filecmp.cmp(x, y, shallow=False):
        assert False, "Files %s and %s differ" % (x, y)


# === Generate Recipes ===
def make_test_fps(n=42):
    chunk_size = 4*1024**2
    return [(i*chunk_size, chunk_size, random_fp())
            for i in range(n)]
