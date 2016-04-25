
import base64
import hashlib
import json
import rados

def fingerprint(data):
    h = hashlib.sha256()
    h.update(data)

    return "SHA-256", h.hexdigest()

class CAS():
    __version__ = "vaceph-cas-0.1"

    def __init__(self, ioctx):

        self.ioctx = ioctx

        self.ioctx.set_namespace("CAS")


    def put(self, data):
        """
        Put object in CAS store.
        If exists, increase refcount.
        If not exists, create with refcount 1
        """
        algo, fp = fingerprint(data)

        meta = {
            "fp_algo" : algo,
            "lib" : self.__version__,
            "compression" : "no",
        }

        args = {
            "data" : base64.b64encode(data),
            "meta" : [{"key" : k, "val" : v} for k, v in meta.iteritems()],
        }

        jargs = json.dumps(args)

        print "PUT:", jargs

        ret, _ = self.ioctx.execute(fp, "cas", "put", jargs)

        if ret == 0:
            return fp

    def get(self, fp):
        """
        Get object by fingerprint
        Throws ObjectNotFound if no object by that fingerprint exists
        """
        ret, out = self.ioctx.execute(fp, "cas", "get", "")

        if ret == 0:
            return out

    def up(self, fp):
        """
        Increment refcount for fingerprint.
        Pin object if refcount hits its maximum
        """
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
        try:
            ret, _ = self.ioctx.execute(fp, "cas", "down", "")

            if ret == 0:
                return True
        except rados.Error:
            return False
