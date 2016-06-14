#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

import struct
import logging

logging.basicConfig(level=logging.DEBUG)


def pack_entry(extent):
    return struct.pack("<QQ64s", *extent)


def unpack_entry(data):
    return struct.unpack("<QQ64s", data)


class RecipeMaker(object):
    @staticmethod
    def pack(fps):
        return fps

    @staticmethod
    def unpack(data):
        return data


class SimpleRecipeMaker(RecipeMaker):
    log = logging.getLogger("Chunker")

    @staticmethod
    def pack(fps):
        payload = "\x1E".join((pack_entry(fp) for fp in fps))
        return payload

    @staticmethod
    def unpack(data):
        fps = [unpack_entry(record)for record in data.split("\x1E")]
        return fps
