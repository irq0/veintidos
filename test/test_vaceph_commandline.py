#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓

"""
Test `veintidos.py` CLI
"""


import os
import tempfile
from subprocess import call
import threading

from rados import Rados
from nose.tools import eq_ as eq

from util import random_bytes, random_id, eq_file, zeros


# = Setup / Tear Down =

# Use RADOS connection to create a temporary pool. Tests create their own
# RADOS connections later


POOL_NAME = None


def setup_module():
    global rados
    rados = Rados(conffile='')
    rados.connect()

    global POOL_NAME
    POOL_NAME = random_id()
    rados.create_pool(POOL_NAME)


def teardown_module():
    global POOL_NAME
    rados.delete_pool(POOL_NAME)


# = Utility Functions =

def prepare_input_file(size, content_func=random_bytes):
    """
    Create temporary file with content generated by `content_func`
    """
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(content_func(size))
    f.close()

    print "Input file:", f.name
    return f.name


def prepare_input_fifo(size, content_func=random_bytes):
    """
    Create temporary FIFO with content generated by `content_func`
    """

    # Write data first to a file and then from there to FIFO
    # (.. so that we can compare contents with the output file later)

    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(content_func(size))
    f.close()
    f_fn = f.name

    fifo_fn = os.path.join(tempfile.gettempdir(), random_id())
    os.mkfifo(fifo_fn)

    def writer():
        with open(f_fn, "r") as f_fd:
            with open(fifo_fn, "w") as fifo_fd:
                for line in f_fd:
                    fifo_fd.write(line)

    threading.Thread(target=writer).start()

    print "Input file: fifo=", fifo_fn, "file=", f_fn
    return f_fn, fifo_fn


def prepare_output_file():
    """
    Create empty temporary file
    """
    f = tempfile.NamedTemporaryFile(delete=False)
    f.close()

    print "Output file:", f.name
    return f.name


# = Test Utility Functions =

def put_and_compare_file(size, content_func):
    """
    Create file with `size` and content generated by `content_func`.
    Use CLI to PUT and GET that file. Compare afterwards
    """

    obj = random_id()
    in_file = prepare_input_file(size, content_func)
    out_file = prepare_output_file()

    ret = call(["./veintidos.py",
                "--pool", POOL_NAME,
                "put", obj,
                in_file])
    eq(0, ret)

    ret = call(["./veintidos.py",
                "--pool", POOL_NAME,
                "get", obj,
                out_file])
    eq(0, ret)

    eq_file(in_file, out_file)

    os.unlink(in_file)
    os.unlink(out_file)


def put_and_compare_fifo(size, content_func):
    """
    Create FIFO with `size` and content generated by `content_func`.
    Use CLI to PUT and GET that file. Compare afterwards.
    """
    obj = random_id()
    in_file, in_fifo = prepare_input_fifo(size, content_func)
    out_file = prepare_output_file()

    ret = call(["./veintidos.py",
                "--pool", POOL_NAME,
                "put", obj,
                in_fifo])
    eq(0, ret)

    ret = call(["./veintidos.py",
                "--pool", POOL_NAME,
                "get", obj,
                out_file])
    eq(0, ret)

    eq_file(in_file, out_file)

    os.unlink(in_file)
    os.unlink(in_fifo)
    os.unlink(out_file)


# = Tests =

# Parameterize above Test Utility Functions
def test_put_from_fifo_with_small_input_gets_the_same_content_back():
    """
    Test: CLI put FIFO small input
    """
    put_and_compare_fifo(42, random_bytes)
    put_and_compare_fifo(42, zeros)


def test_put_from_file_with_small_input_gets_the_same_content_back():
    """
    Test: CLI put regular file small input
    """
    put_and_compare_file(42, random_bytes)
    put_and_compare_file(42, zeros)


def test_put_from_fifo_8M_input_gets_the_same_content_back():
    """
    Test: CLI put FIFO 8M input
    """
    put_and_compare_fifo(8*1024**2, random_bytes)
    put_and_compare_fifo(8*1024**2, zeros)


def test_put_from_file_8M_input_gets_the_same_content_back():
    """
    Test: CLI put regular file 8M input
    """
    put_and_compare_file(8*1024**2, random_bytes)
    put_and_compare_file(8*1024**2, zeros)


def test_put_from_file_100M_zeros():
    """
    Test: CLI put file 100M only zeros
    """
    put_and_compare_file(100*1024**2, zeros)
