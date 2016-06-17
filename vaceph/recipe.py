#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

import logging
import msgpack

logging.basicConfig(level=logging.DEBUG)


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


class Recipe(object):
    def __init__(self, fps):
        pass

    def pack(self):
        raise NotImplementedError()

    @staticmethod
    def unpack(data):
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()


class SimpleRecipe(Recipe):
    version = 0
    fps = []

    def __init__(self, fps):
        self.fps = fps

    def get_size(self):
        off, length, _ = self.fps[-1]
        return off + length

    def make_header(self):
        return (self.version,)

    def __iter__(self):
        return self.fps.__iter__()

    def __len__(self):
        return len(self.fps)

    def extents_in_range(self, length, offset):
        return get_extents_in_range(self.fps, length, offset)

    def pack(self):
        return msgpack.packb((self.make_header(), self.fps))

    @staticmethod
    def unpack(data):
        header, fps = msgpack.unpackb(data, use_list=False)

        r = SimpleRecipe(fps)

        if header[0] != r.version:
            raise RuntimeError("Unsupported version")

        return r
