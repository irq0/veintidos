#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓

import logging
import msgpack

# = Utility Functions =

def get_extents_in_range(recipe, length, offset):
    """
    Really stupid way to find overlapping extents.
    But, since we only have a couple of hundred per file..
    """
    # TODO use interval tree to get better runtime

    result = []

    a = offset
    b = offset + length

    result = [(e_offset, e_length, fp)
              for e_offset, e_length, fp in recipe
              if (a <= e_offset <= b or
                  a <= (e_offset + e_length) <= b)]

    return result


# = Recipe Classes =

# == Recipe Interface Class ==

class Recipe(object):
    # === Pack / Unpack ===

    def __init__(self, fps):
        """
        Initialize recipe from fingerprint extent list.

        Expected format: `(offset, length, fingerprint)`
        """
        pass

    def pack(self):
        """
        Return packed recipe data
        """
        raise NotImplementedError()

    @staticmethod
    def unpack(data):
        """
        Unpack recipe data into Recipe object
        """
        raise NotImplementedError()

    # === Iterator Methods ===

    def __iter__(self):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()


# == Simple Recipe ==

class SimpleRecipe(Recipe):
    """
    Simple msgpack-based recipe

    Packs fingerprints + small header
    """

    version = 0
    fps = []
    log = logging.getLogger("SimpleRecipe")

    # === Init ===

    def __init__(self, fps):
        self.fps = fps

    # === Utility Methods ===

    def get_size(self):
        off, length, _ = self.fps[-1]
        return off + length

    def make_header(self):
        """
        Make header for msgpack recipe

        Currently only add a version number
        """
        return (self.version,)

    def __iter__(self):
        return self.fps.__iter__()

    def __len__(self):
        return len(self.fps)

    def extents_in_range(self, length, offset):
        """Class version of `get_extents_in_range`"""
        return get_extents_in_range(self.fps, length, offset)

    # === Pack ===

    def pack(self):
        return msgpack.packb((self.make_header(), self.fps))

    # === Unpack ===

    @staticmethod
    def unpack(data):

        try:
            header, fps = msgpack.unpackb(data, use_list=False)

        except msgpack.ExtraData, e:
            # If msgpack finds more data whan it expects, almost
            # certainly means that the object is damaged
            log = logging.getLogger("SimpleRecipe")
            log.exception("Recipe unpack error")
            log.debug("Extra Data: %r", e.extra)
            log.debug("Unpacked: %r", e.unpacked)

            return None

        r = SimpleRecipe(fps)

        if header[0] != r.version:
            raise RuntimeError("Unsupported version")

        return r
