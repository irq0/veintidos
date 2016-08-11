#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓

import bz2

import snappy


# = Exceptions =

class UnknownCompressor(Exception):
    pass


# = Compressor Classes =

# Compressor classes adapt compression APIs from python libraries
# to what the CAS class [[cas.py]] needs.

# == Base Class / No Compression ==

class Compressor(object):
    """
    Compression / Decompression wrapper CAS <-> python
    """

    # Each class has an identifier attribute by which CAS selects the
    # compressor
    identifier = "no"

    # === Compress ===

    @staticmethod
    def compress(data):
        """
        Compress `data` and return it with additional metadata for [[CAS.py#put]]
        """
        meta = {
            "compression": "no",
            "orig_size": len(data)
        }
        return meta, data

    # === Decompress ===

    @staticmethod
    def decompress(data):
        """
        Decompress data
        """
        return data

    # === Select ===

    @staticmethod
    def select(identifier):
        """
        Select compressor class by identifier. Return class object
        """
        for cls in Compressor.__subclasses__():
            if cls.identifier == identifier:
                return cls

        if Compressor.identifier == identifier:
            return Compressor

        raise UnknownCompressor("Compressor unknown: " + identifier)

    # === Supported ===

    @staticmethod
    def supported():
        """
        Return list of supported compressors
        """
        return [cls.identifier for cls in Compressor.__subclasses__()] + [Compressor.identifier]


# == Snappy Compressor ==

class SnappyCompressor(Compressor):
    identifier = "snappy"

    @staticmethod
    def compress(data):
        meta = {
            "compression": "snappy",
            "orig_size": len(data)
        }

        compressed_data = snappy.compress(data)
        return meta, compressed_data

    @staticmethod
    def decompress(compressed_data):
        data = snappy.uncompress(compressed_data)
        return data


# == BZ2 Compressor ==

class BZ2Compressor(Compressor):
    identifier = "bz2"

    @staticmethod
    def compress(data):
        meta = {
            "compression": "bz2",
            "orig_size": len(data),
            "compresslevel": 9
        }

        compressed_data = bz2.compress(data, 9)
        return meta, compressed_data

    @staticmethod
    def decompress(compressed_data):
        data = bz2.decompress(compressed_data)
        return data
