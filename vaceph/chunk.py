#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

import time
import logging
import multiprocessing.dummy as multiprocessing

from functools import partial

import recipe

logging.basicConfig(level=logging.DEBUG)


def make_index_version():
    """ Make version number for index enties """
    return int(time.time()*1000)


#
# Chunk generators
#

def chunk_iter(file_obj, chunk_size=4 * 1024**2):
    """ Iterate fixed sized chunks in a file """
    return iter(lambda: file_obj.read(chunk_size), '')


def static_chunker(file_, chunk_size):
    """
    Return generator with static chunked extents + data
    for file_
    """
    return ((i*chunk_size, chunk_size, chunk)
            for i, chunk in
            enumerate(chunk_iter(file_, chunk_size)))

class Chunker(object):
    """
    Chunker: Ceph Striper replacement that uses CAS and recipes

    Idea:
        - Write file in chunks to CAS namespace
        - Create recipe for chunks those chunks
        - Add recipe entry to name's omap
        - Return key of that omap entry (=version)
    """

    __version__ = "vaceph-chunker-0.1"

    log = logging.getLogger("Chunker")
    chunk_size = 4 * 1024**2
    cas_worker = None
    chunker = partial(static_chunker, chunk_size=chunk_size)

    def __init__(self, cas_obj, index_io_ctx):
        """
        cas_obj: CAS object for objects and recipes
        index_io_ctx: io_ctx for the recipe index (name -> recipe mappings)

        NOTE: don't use the same io ctx for cas and index!
        """

        self.cas = cas_obj
        self.recipe = recipe.SimpleRecipeMaker
        self.index_io_ctx = index_io_ctx
        self.index_io_ctx.set_namespace("INDEX")

    def _write_chunks(self, chunks):
        """
        ( (off, size, data) ) ↦ ( (off, size, fingerprint) )
        """
        self.log.debug("Writing chunks to CAS pool")
        pool = multiprocessing.Pool(8)

        def cas_put_wrapper(args):
            off, size, chunk = args
            return (off, size, self.cas.put(chunk))

        return pool.map(cas_put_wrapper, chunks, chunksize=16)

    def write_full(self, name, file_):
        """
        Write all data in file_ to a CAS pool
        and return version number
        """

        self.log.debug("Writing file: %r", file_)

        chunks = self.chunker(file_)
        fps = self._write_chunks(chunks)

        recipe_obj_name = self.cas.put(self.recipe.pack(fps))
        index_version_key = str(make_index_version())

        self.log.debug("Saving recipe: [%s] %s -> %s",
                       name, index_version_key, recipe_obj_name)

        w_op = self.index_io_ctx.create_write_op()
        self.index_io_ctx.set_omap(w_op, (index_version_key,),
                                   (recipe_obj_name,))
        self.index_io_ctx.operate_write_op(w_op, name)
        self.index_io_ctx.release_write_op(w_op)

        return index_version_key

    def _versions_and_recipes(self, name):
        r_op = self.index_io_ctx.create_read_op()
        vals, _ = self.index_io_ctx.get_omap_vals(r_op, "", "", -1)
        self.index_io_ctx.operate_read_op(r_op, name)
        self.index_io_ctx.release_read_op(r_op)

        return vals

    def versions(self, name):
        """
        Return list of name's versions
        """
        versions = [x[0] for x in self._versions_and_recipes(name)]
        return versions

    def head_version(self, name):
        """
        Return last version
        """
        return max(self.versions(name))

    def read_full(self, name, file_, version="HEAD"):
        """
        Write all data belonging to name to file
        """

        if version == "HEAD":
            recipe_obj = max(self._versions_and_recipes(name))[1]
        else:
            recipe_obj = dict(self._versions_and_recipes(name))[version]

        fps = self.recipe.unpack(self.cas.get(recipe_obj))
        self.log.debug("Retrieved recipe: %d extents", len(fps))

        # chunks = ((off, size, self.cas.get(fp))
        #           for off, size, fp in fps)

        bytes_written = 0
        bytes_retrieved = 0

        for off, size, fp in fps:
            chunk = self.cas.get(fp, off=0, size=size)

            self.log.debug("Writing extent: %d:%d (%d)", off, size, len(chunk))

            file_.seek(off)
            file_.write(chunk[:size])

            bytes_written += size
            bytes_retrieved += len(chunk)

        self.log.debug("Wrote %d bytes / Retrieved %d bytes",
                       bytes_written, bytes_retrieved)
        file_.flush()

        return bytes_written
