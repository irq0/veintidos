#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓
# LD_LIBRARY_PATH="/srv/ceph-devel/src/src/.libs:/srv/ceph-devel/src/src/build/lib.linux-x86_64-2.7" ipython


import base64
import hashlib
import json
import struct
import logging

import bz2
import snappy
import rados


def fingerprint(data):
    h = hashlib.sha256()
    h.update(data)
from compressor import Compressor

    return "SHA-256", h.hexdigest()


class CASError(Exception):
    pass


    """


class CAS(object):
    __version__ = "veintidos-cas-0.1"

    log = logging.getLogger("CAS")

    def __init__(self, ioctx, compression="no"):

        self.ioctx = ioctx
        self.ioctx.set_namespace("CAS")
        self.compressor = Compressor.select(compression)

    def _put(self, fp, data, meta):
        args = {
            "data": base64.b64encode(data),
            "meta": [{"key": k, "val": v} for k, v in meta.iteritems()],
        }

        jargs = json.dumps(args)

        ret, _ = self.ioctx.execute(fp, "cas", "put", jargs)

        if ret == 0:
            return fp
        else:
            raise CASError("PUT failed")

    def put(self, data):
        """
        Put object in CAS store.
        If exists, increase refcount.
        If not exists, create with refcount 1
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

        return self._put(fp, compressed_data, meta)

    def get(self, fp, off=0, size=8192):
        """
        Get object by fingerprint
        Throws ObjectNotFound if no object by that fingerprint exists
        """
        self.log.debug("GET [%r]: %s:%s", fp, off, size)

        obj_size, _ = self.ioctx.stat(fp)

        compression_id = CAS._convert_meta(self.ioctx.get_xattr(fp, "cas.meta.compression"))
        decompressor = Compressor.select(compression_id)

        self.log.debug("GET [%r]: size %d compressed with %r", fp, obj_size, compression_id)

        compressed_data = self.ioctx.read(fp, obj_size, 0)
        data = decompressor.decompress(compressed_data)

        return data[off:off+size]

    def up(self, fp):
        """
        Increment refcount for fingerprint.
        Pin object if refcount hits its maximum
        """
        self.log.debug("UP [%r]", fp)
        try:
            ret, _ = self.ioctx.execute(fp, "cas", "up", "")

            if ret == 0:
                return True
        except rados.Error:
            return False

    def down(self, fp):
        """
        Decrement refcount for fingerprint.
        Destroy object if fingerprint reaches 0 and if it is not pinned
        """
        self.log.debug("DOWN [%r]", fp)
        try:
            ret, _ = self.ioctx.execute(fp, "cas", "down", "")

            if ret == 0:
                return True
        except rados.Error:
            return False

    def info(self, fp):
        keys = (
            ("cas.meta.compression", CAS._convert_meta),
            ("cas.meta.lib", CAS._convert_meta),
            ("cas.meta.fp_algo", CAS._convert_meta),
            ("cas.meta.orig_size", CAS._convert_meta),
            ("cas.refcount", CAS._convert_refcount),
        )

        return {key: conv(self.ioctx.get_xattr(fp, key))
                for key, conv in keys}

    @staticmethod
    def _convert_refcount(r):
        return struct.unpack("<Q", r)[0]

    @staticmethod
    def _convert_meta(m):
        l = struct.unpack("<I", m[:4])[0]
        return m[4:4+l]

    def list(self):
        return [(o.key, CAS._convert_refcount(o.get_xattr("cas.refcount")))
                for o in self.ioctx.list_objects()
                if o.nspace == "CAS"]
