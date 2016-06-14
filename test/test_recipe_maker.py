#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

from nose.tools import eq_ as eq
from util import random_fp

from vaceph.recipe import SimpleRecipeMaker


def make_test_fps(n=42):
    chunk_size = 4*1024**2
    return [(i*chunk_size, chunk_size, random_fp())
            for i in range(n)]


def test_SimpleRecipeMaker():
    r = SimpleRecipeMaker

    fps_in = make_test_fps()

    data = r.pack(fps_in)
    fps_out = r.unpack(data)

    eq(fps_in, fps_out)
