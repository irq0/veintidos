#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

import time
import logging
import threading
import mmap
import math
import multiprocessing.dummy as multiprocessing

from functools import partial

import recipe


CHUNKER_MAX_OUTSTANDING = 100  # * 4MB
WRITER_MAX_THREADS = 8
WRITER_THREAD_CHUNKSIZE = 16


def make_index_version():
    """ Make version number for index enties """
    return int(time.time()*1000)


#
# Chunk generators
#


def static_chunker(file_, chunk_size):
    """
    Return generator with static chunked extents + data
    for file_
    """
    log = logging.getLogger("static_chunker")

    def mmap_chunker():
        """Chunker for files"""

        log.info("Using mmap chunker")
        chunks = int(math.ceil(float((len(mm)) / float(chunk_size))))
        log.debug("mmap file: size=%s, chunks=%s", len(mm), chunks)

        size = chunk_size
        rest = len(mm)
        if len(mm) < chunk_size:
            size = len(mm)

        for chunk_num in xrange(chunks):
            start = chunk_num * chunk_size

            yield (start,
                   size,
                   lambda s=start: mm[s:s+chunk_size])

            rest -= size
            size = min(rest, chunk_size)

        # can't close the mmap here since mm regions may still be accessed
        # using the chunk function above
#        mm.close()

    def fallback_chunker():
        """
        Chunker for anything not supporting mmap like streams
        """
        log.info("Using fallback chunker")
        log.debug("Fallback chunker settings: max_outstanding=%s",
                  CHUNKER_MAX_OUTSTANDING)

        chunk_num = 0
        outstanding = threading.BoundedSemaphore(value=CHUNKER_MAX_OUTSTANDING)

        def make_chunk_func(chunk):
            def chunk_func():
                log.debug("Realizing chunk: len=%s", len(chunk))
                outstanding.release()
                return chunk
            return chunk_func

        start = 0
        while True:
            outstanding.acquire()

            chunk = file_.read(chunk_size)
            size = len(chunk)
            log.debug("Read chunk: start=%s, num=%s, size=%s",
                      start, chunk_num, size)

            if chunk:
                yield (start,
                       size,
                       make_chunk_func(chunk))

                chunk_num += 1
                if size < chunk_size:
                    break

                start = chunk_num * chunk_size
            else:
                break

    try:
        mm = mmap.mmap(file_.fileno(), 0, mmap.MAP_SHARED, mmap.PROT_READ)
        return mmap_chunker()
    except:
        return fallback_chunker()


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
        self.recipe = recipe.SimpleRecipe
        self.index_io_ctx = index_io_ctx
        self.index_io_ctx.set_namespace("INDEX")

    def _cas_put_wrapper(self, args):
        off, size, chunk_func = args

        self.log.debug("Chunk writer worker [%s]: %s:%s",
                       threading.current_thread().getName(), off, size)

        chunk = chunk_func()

        assert len(chunk) == size, \
            "extent.size != chunk size ({} vs. {})".format(size, len(chunk))

        return (off, size, self.cas.put(chunk))

    def _write_chunks(self, chunks):
        """
        ( (off, size, data) ) ↦ ( (off, size, fingerprint) )
        """
        self.log.debug("Starting chunk writers")
        pool = multiprocessing.Pool(WRITER_MAX_THREADS)

        result = pool.imap(self._cas_put_wrapper, chunks, chunksize=WRITER_THREAD_CHUNKSIZE)
        self.log.debug("Starting chunk writers: Finished. Waiting for execution")

        return list(result)

    def write_full(self, name, file_):
        """
        Write all data in file_ to a CAS pool
        and return version number
        """

        self.log.debug("Writing data [%r]: %r", name, file_)

        chunks = self.chunker(file_)
        recipe = self.recipe(self._write_chunks(chunks))

        recipe_obj_name = self.cas.put(recipe.pack())
        index_version_key = str(make_index_version())

        self.log.debug("Saving recipe [%s]: %s -> %s",
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
        versions = self.versions(name)
        if versions:
            return max(self.versions(name))

    def _resolve_recipe_obj_from_version(self, name, version):
        vs_and_rs = self._versions_and_recipes(name)

        if not vs_and_rs:
            return None

        if version == "HEAD":
            return max(self._versions_and_recipes(name))
        else:
            return (version, dict(self._versions_and_recipes(name))[version])

    def read_full(self, name, out_file, version="HEAD"):
        """
        Write all data belonging to name to file
        """
        version, recipe_obj = self._resolve_recipe_obj_from_version(name, version)

        self.log.debug("Reading version %r of object %r", version, name)

        recipe = self.recipe.unpack(self.cas.get(recipe_obj, size=100*1024**2))
        self.log.debug("Retrieved recipe: %d extents", len(recipe))

        bytes_written = 0
        bytes_retrieved = 0

        for off, size, fp in recipe:
            chunk = self.cas.get(fp, off=0, size=size)

            self.log.debug("Writing extent: %d:%d (%d)", off, size, len(chunk))

            out_file.seek(off)
            out_file.write(chunk[:size])

            bytes_written += size
            bytes_retrieved += len(chunk)

        self.log.debug("Wrote %d bytes / Retrieved %d bytes",
                       bytes_written, bytes_retrieved)
        out_file.flush()

        return bytes_written

    def read(self, name, length, offset, version="HEAD"):
        """
        Return offset:length part of chunked file name
        """
        version, recipe_obj = self._resolve_recipe_obj_from_version(
            name, version)

        self.log.debug("Reading version %r of object %r: %d:%d", version, name, offset, length)

        recipe = self.recipe.unpack(self.cas.get(recipe_obj))
        self.log.debug("Retrieved recipe: %d extents", len(recipe))

        bufs = []
        extents = recipe.extents_in_range(length, offset)
        orig_offset = offset
        end = min(offset+length, recipe.get_size())

        # 2 phase algorithm:
        # 1. get (partial) chunks from CAS pool

        for extent_offset, extent_length, fp in extents:
            local_offset = max(offset, extent_offset)
            extent_end = extent_length + extent_offset

            chunk_offset = max(offset-extent_offset, 0)
            chunk_length = min(end-local_offset, extent_end-local_offset)

            chunk = self.cas.get(fp, off=chunk_offset,
                                 size=chunk_length)

            bufs.append((offset, chunk[:chunk_length]))

            offset += chunk_length

        # 2. Concatenate (partial) chunks and zero-fill gaps
        result = ""
        offset = orig_offset
        first_off, _ = bufs[0]

        self.log.debug("Reconstructing file from the following chunks: %r",
                       [(off, len(buf)) for off, buf in bufs])

        if offset < first_off:
            self.log.debug("Zero fill: %d:%d", offset, (first_off - offset))
            result += "\x00" * (first_off - offset)
            offset = first_off

        for current, next in zip(bufs, bufs[1:]):
            c_off, c_buf = current
            c_end = (c_off + len(c_buf))
            n_off, n_buf = next

            self.log.debug("Chunk fill: %d:%d", offset, len(c_buf))
            result += c_buf
            offset += len(c_buf)

            if c_end < n_off:
                self.log.debug("Zero fill: %d:%d", offset, (n_off - c_end))
                result += "\x00" * (n_off - c_end)

        last_off, last_buf = bufs[-1]
        last_end = last_off + len(last_buf)

        self.log.debug("Chunk fill: %d:%d", offset, len(last_buf))
        result += last_buf
        offset += len(last_buf)

        if last_end < end:
            self.log.debug("Zero fill: %d:%d", offset,
                           ((offset + length) - last_end))
            result += "\x00" * ((offset + length) - last_end)

        return result

    def remove_version(self, name, version="HEAD"):
        """
        Remove version of name

        Decreases refcount of all objects in recipe
        """
        version, recipe_obj = self._resolve_recipe_obj_from_version(name, version)

        self.log.debug("Removing version %r of object %r", version, name)
        recipe = self.recipe.unpack(self.cas.get(recipe_obj))

        for _, _, fp in recipe:
            self.cas.down(fp)

        w_op = self.index_io_ctx.create_write_op()
        self.index_io_ctx.remove_omap_keys(w_op, (version,))
        self.index_io_ctx.operate_write_op(w_op, name)
        self.index_io_ctx.release_write_op(w_op)

        self.cas.down(recipe_obj)

    def remove_all_versions(self, name):
        """
        Remove all versions of an object and
        the index object itself
        Decreases refcount of all objects in recipe
        """
        todo = list(self._versions_and_recipes(name))
        self.log.debug("Removing ALL of object [%r]: %r", name, todo)

        for version, _ in todo:
            self.remove_version(name, version)

        self.index_io_ctx.remove_object(name)
