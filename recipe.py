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
    __version__ = "vaceph-recipe-maker-0.1"

    log = logging.getLogger("Chunker")

    def __init__(self, cas):

        self.cas = cas

    def put(self, fps):
        """
        Write recipe for fingerprints as CAS object
        fps: [(offset, size, fingerprint), ...]
        returns name of recipe object
        """

        payload = "\x1E".join((pack_entry(fp) for fp in fps))
        self.log.debug("Recipe payload: %r", payload)

        name = self.cas.put(payload)

        self.log.info("Saved recipe as %r", name)
        return name

    def get(self, name):
        payload = self.cas.get(name)

        self.log.info("Read recipe %r: %r", name, payload)

        fps = [unpack_entry(record)for record in payload.split("\x1E")]

        return fps
