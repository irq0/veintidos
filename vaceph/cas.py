#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓
# LD_LIBRARY_PATH="/srv/ceph-devel/src/src/.libs:/srv/ceph-devel/src/src/build/lib.linux-x86_64-2.7" ipython


import base64
import hashlib
import json
import struct
import logging

import snappy
import rados

def fingerprint(data):
    h = hashlib.sha256()
    h.update(data)

    return "SHA-256", h.hexdigest()


class CASError(Exception):
    pass


class CAS(object):
    __version__ = "vaceph-cas-0.1"

    log = logging.getLogger("CAS")

    def __init__(self, ioctx):

        self.ioctx = ioctx
        self.ioctx.set_namespace("CAS")

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
            "compression": "no",
        }

        return self._put(fp, data, meta)

    def get(self, fp, off=0, size=8192):
        """
        Get object by fingerprint
        Throws ObjectNotFound if no object by that fingerprint exists
        """
        self.log.debug("GET [%r]: %s:%s", fp, off, size)

        return self.ioctx.read(fp, size, off)

    #        ret, out = self.ioctx.execute(fp, "cas", "get", "")

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

    def list(self):
        def convert_refcount(r):
            return struct.unpack("<Q", r)[0]

        return [(o.key, convert_refcount(o.get_xattr("cas.refcount")))
                for o in self.ioctx.list_objects()
                if o.nspace == "CAS"]


class CompressedCAS(CAS):
    log = logging.getLogger("CompressedCAS")

    def __init__(self, ioctx):
        super(CompressedCAS, self).__init__(ioctx)

    def put(self, data):
        """
        Put object in CAS store.
        If exists, increase refcount.
        If not exists, create with refcount 1
        """

        algo, fp = fingerprint(data)

        compressed_data = snappy.compress(data)

        self.log.debug("PUT [%s:%r]: %d bytes, %d compressed", algo, fp, len(data), len(compressed_data))

        meta = {
            "fp_algo": algo,
            "lib": self.__version__,
            "compression": "snappy",
            "orig_size": len(data)
        }

        return self._put(fp, compressed_data, meta)

    def get(self, fp, off=0, size=8192):
        """
        Get object by fingerprint
        Throws ObjectNotFound if no object by that fingerprint exists
        """
        self.log.debug("GET [%r]: %s:%s", fp, off, size)

        # TODO improve me. don't get whole object and return (off, size) chunk

        obj_size, _ = self.ioctx.stat(fp)

        compressed_data = super(CompressedCAS, self).get(fp, 0, obj_size)
        data = snappy.uncompress(compressed_data)

        return data[off:off+size]
