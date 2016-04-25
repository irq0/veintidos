#!/usr/bin/env python

#LD_LIBRARY_PATH="/srv/ceph-devel/src/src/.libs:/srv/ceph-devel/src/src/build/lib.linux-x86_64-2.7" ipython


import os
import sys

import json

import glob

import magic
import re

from graph_tool.all import *

from ceph_argparse import \
    concise_sig, descsort, parse_json_funcsigs, \
    matchnum, validate_command, find_cmd_target, \
    send_command, json_command

def setup_ceph_client():
    cluster = rados.Rados(name="client.admin", clustername="cd10000",
                                 conffile=sys.argv[1])
    cluster.connect()

    return cluster

def get_pythonlib_dir():
    """Returns the name of a distutils build directory"""
    import sysconfig
    f = "lib.{platform}-{version[0]}.{version[1]}"
    name = f.format(platform=sysconfig.get_platform(),
                    version=sys.version_info)
    return name

def setup_ceph_local_dev():
    ceph_dev_dir = "/srv/ceph-devel/src/src"
    os.environ["LD_LIBRARY_PATH"] = ":".join((os.path.join(ceph_dev_dir, ".libs"),
                                              os.path.join(ceph_dev_dir, 'build', get_pythonlib_dir())))
    sys.path.insert(0, os.path.join(ceph_dev_dir, "pybind"))
    sys.path.insert(0, os.path.join(ceph_dev_dir, 'build', get_pythonlib_dir()))
    sys.path.insert(0, os.path.join(ceph_dev_dir, '.libs'))

    import rados

    cluster = rados.Rados(name="client.admin", clustername="ceph", conffile="/srv/ceph-devel/src/src/ceph.conf")
    cluster.connect()

    ioctx = cluster.open_ioctx("rbd")

    return cluster, ioctx

from stat import *

def mode_to_string(mode):
    if S_ISDIR(mode):
        return "DIR"
    elif S_ISREG(mode):
        return "REG"
    elif S_ISCHR(mode) or S_ISBLK(mode):
        return "DEV"
    elif S_ISFIFO(mode):
        return "FIF"
    elif S_ISLNK(mode):
        return "LNK"
    elif S_ISSOCK(mode):
        return "SOC"
    else:
        return "UNK"

def scan(path):
    path_regex = re.compile("(/proc/.*|/sys/.*|/dev/.*|/tmp/.*)")

    g = Graph(directed=False)
    tree = Graph(directed=False)
    props = {
        "path" : g.new_vertex_property("string"),
        "size" : g.new_vertex_property("int64_t"),
        "dir_size" : g.new_vertex_property("int64_t"),
        "hierarchy_size" : g.new_vertex_property("int64_t"),
        "node_type" : g.new_vertex_property("string"),
        "magic_string" : g.new_vertex_property("string"),
        "mode" : g.new_vertex_property("int32_t"),
        "uid" : g.new_vertex_property("int32_t"),
        "gid" : g.new_vertex_property("int32_t"),
        "atime" : g.new_vertex_property("int32_t"),
        "mtime" : g.new_vertex_property("int32_t"),
        "ctime" : g.new_vertex_property("int32_t"),
    }

    with magic.Magic(flags=magic.MAGIC_MIME_TYPE|magic.MAGIC_MIME_ENCODING) \
         as m:

        def add_file_properties(v, path):
            stat = os.lstat(path)

            props["node_type"][v] = mode_to_string(stat.st_mode)
            props["path"][v] = path
            props["size"][v] = stat.st_size
            props["magic_string"][v] = m.id_filename(path)
            props["mode"][v] = stat.st_mode
            props["uid"][v] = stat.st_uid
            props["gid"][v] = stat.st_gid
            props["atime"][v] = stat.st_atime
            props["mtime"][v] = stat.st_mtime
            props["ctime"][v] = stat.st_ctime

        def new_node(path):
            v = g.add_vertex()
            add_file_properties(v, path)
            return v

        parent = g.add_vertex()
        add_file_properties(parent, path)

        for root, dirs, files in os.walk(path):
            if path_regex.match(root):
                print >>sys.stderr, "ignoring:", root
                continue

            dir_v = new_node(root)
            g.add_edge(parent, dir_v)

            this_dir_size = 0
            for f in files:
                path = os.path.join(root, f)
                v = new_node(path)
                this_dir_size += props["size"][v]
                g.add_edge(v, dir_v)

            props["dir_size"][dir_v] = this_dir_size
            parent = dir_v

    return g, props


def props_to_vprops(g, props):
    vprops = {
        "shape" : g.new_vertex_property("string"),
        "size" : g.new_vertex_property("int"),
        "text" : g.new_vertex_property("string"),
    }

    type_to_shape = {
        "DIR" : "triangle",
        "REG" : "circle",
        "LNK" : "square",
        "DEV" : "double_octagon",
        "FIF" : "double_pentagon",
        "SOC" : "double_hexagon",
        "UNK" : "pie",
    }

    for v in g.vertices():
        vprops["shape"][v] = type_to_shape[props["node_type"][v]]
        if props["node_type"][v] == "DIR":
            vprops["text"][v] = props["path"][v]
        else:
            vprops["text"][v] = os.path.basename(props["path"][v])

    return vprops

def main():
    ceph_cluster = setup_ceph_client()
