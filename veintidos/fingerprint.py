#!/usr/bin/env python
# -*- coding: utf-8 -*-
# UTF-8? ✓

import hashlib

"""
Fingerprinting functions. Used by [[cas.py]]
"""


# === Fingerprint ===

def fingerprint(data):
    """
    Return tuple `(algorithm name, fingerprint)` of `data`

    Currently only supports SHA-256
    """
    h = hashlib.sha256()
    h.update(data)

    return "SHA-256", h.hexdigest()
