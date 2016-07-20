#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ✓

import snappy
import fileinput

buf = []
for line in fileinput.input():
    buf.append(line)

print snappy.uncompress("".join(buf))
