#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓

"""
Recipe Tests

Classes under test:

- SimpleRecipe

"""

from nose.tools import eq_ as eq
from util import make_test_fps

from veintidos.recipe import SimpleRecipe


# = Tests =

def test_SimpleRecipeMaker():
    """
    Test: SimpleRecipeMaker: pack/unpack of recipes of differnet size
    """
    fps_in = make_test_fps()

    r = SimpleRecipe(fps_in)
    data = r.pack()
    fps_out = r.unpack(data)

    eq(fps_in, list(fps_out))

    fps_in = make_test_fps(1000000)

    r = SimpleRecipe(fps_in)
    data = r.pack()
    fps_out = r.unpack(data)

    eq(fps_in, list(fps_out))
