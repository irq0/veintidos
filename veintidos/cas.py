#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓


"""
CAS Class and related exceptions

The CAS Class is a thin wrapper over RADOS and the
CAS Object Class extension in Ceph.
"""

# = Imports =

import base64
import json
import struct
import logging

import rados

# Compressor and fingerprinting utilities used
# by the CAS class:

from compressor import Compressor
from fingerprint import fingerprint


# = Exceptions =

class CASError(Exception):
    pass

# = CAS Class =

class CAS(object):
    """
    Access methods for Ceph CAS Pools.
    Wraps CAS Object Class methods `put`, `up`, and `down`.

    Adds fingerprinting (See: [[fingerprint.py]]), compression (See: [[compressor.py]]),
    and convinience methods like `info` and `list`.
    """

    __version__ = "veintidos-cas-0.1"

    log = logging.getLogger("CAS")

    # == Init ==

    def __init__(self, ioctx, compression="no"):
        """
        Initialize CAS object. Caller needs to provide a connected and initialized
        RADOS I/O context.

        CAS objects need their own, exclusive RADOS I/O Context, since they operate on
        objects in an extra namespace.

        On initialization you can also specify the compression algorithm for
        **new** objects. CAS never overwrites objects that already exists, but rather
        increments their reference count. Existing objects have metadata to
        select the right decompressor on `get`.
        """

        self.ioctx = ioctx
        self.ioctx.set_namespace("CAS")
        self.compressor = Compressor.select(compression)

    # == Put ==

    def put(self, data):
        """
        Put object in CAS pool

        - If exists, increase refcount.
        - If not exists, create with refcount=1 and compression set on CAS object init

        Return fingerprint (= object name) of `data` in any case
        """

        algo, fp = fingerprint(data)
        self.log.debug("PUT [%s:%r]: %d bytes", algo, fp, len(data))

        meta = {
            "fp_algo": algo,
            "lib": self.__version__,
        }

        algo, fp = fingerprint(data)

        compression_meta, compressed_data = self.compressor.compress(data)
        meta.update(compression_meta)

        self.log.debug("PUT [%s:%r]: %d bytes, %d compressed with \"%s\"", algo, fp, len(data), len(compressed_data),
                       compression_meta["compression"])

        jargs = self._make_cas_put_arg(compressed_data, meta)

        ret, _ = self.ioctx.execute(fp, "cas", "put", jargs)

        if ret == 0:
            return fp
        else:
            raise CASError("PUT failed")

    # == Get ==

    def get(self, fp, off=0, size=-1):
        """
        Get object by `fingerprint`

        - Throws `ObjectNotFound` RADOS exception, if no object with that fingerprint exists
        - Return decompressed object
        - Return the whole object by default
        - If you specify `off` and `size` it is applied only after fetching the whole object
        """
        self.log.debug("GET [%r]: %s:%s", fp, off, size)

        obj_size, _ = self.ioctx.stat(fp)

        compression_id = CAS._convert_meta(self.ioctx.get_xattr(fp, "cas.meta.compression"))
        decompressor = Compressor.select(compression_id)

        self.log.debug("GET [%r]: size %d compressed with %r", fp, obj_size, compression_id)

        compressed_data = self.ioctx.read(fp, obj_size, 0)
        data = decompressor.decompress(compressed_data)

        if size < 0:
            size = len(data)

        return data[off:off+size]

    # == Up ==

    def up(self, fp):
        """
        Increment refcount for object with fingerprint `fp`.
        """
        self.log.debug("UP [%r]", fp)
        try:
            ret, _ = self.ioctx.execute(fp, "cas", "up", "")

            if ret == 0:
                return True
        except rados.Error:
            return False

    # == Down ==

    def down(self, fp):
        """
        Decrement refcount for object with fingerprint `fp`.
        """
        self.log.debug("DOWN [%r]", fp)
        try:
            ret, _ = self.ioctx.execute(fp, "cas", "down", "")

            if ret == 0:
                return True
        except rados.Error:
            return False

    # == Info ==

    def info(self, fp):
        """
        Return dict with object's metadata attributes
        """
        keys = (
            ("cas.meta.compression", CAS._convert_meta),
            ("cas.meta.lib", CAS._convert_meta),
            ("cas.meta.fp_algo", CAS._convert_meta),
            ("cas.meta.orig_size", CAS._convert_meta),
            ("cas.refcount", CAS._convert_refcount),
        )

        return {key: conv(self.ioctx.get_xattr(fp, key))
                for key, conv in keys}

    # == List ==

    def list(self):
        """
        Return list of CAS objects in pool and their reference count
        """
        return [(o.key, CAS._convert_refcount(o.get_xattr("cas.refcount")))
                for o in self.ioctx.list_objects()
                if o.nspace == "CAS"]

    # == Utility Functions ==

    @staticmethod
    def _convert_refcount(r):
        """Convert refcount from xattr to python"""
        return struct.unpack("<Q", r)[0]

    @staticmethod
    def _convert_meta(m):
        """Convert metadata xattr to python"""
        # Decode Pascal style string with 4 bytes length field
        l = struct.unpack("<I", m[:4])[0]
        return m[4:4+l]

    @staticmethod
    def _make_cas_put_arg(data, meta):
        """
        Helper method to convert `data` and `meta` into
        JSON understood by the CAS Object Class
        """

        args = {
            # Encode `data` as Base64 so that it won't interfere with the JSON
            "data": base64.b64encode(data),
            # Encode `meta` data into a list of {"key":..,"val":..} elements,
            # that Ceph's JSON decoder can decode as a C++ map
            "meta": [{"key": k, "val": v} for k, v in meta.iteritems()],
        }

        jargs = json.dumps(args)
        return jargs
