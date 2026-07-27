# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Mach-O architecture inspection for correct macOS wheel tagging.

The macOS Python wheels bundle a Go-built ``libadbc_driver_spark.dylib`` as
package data. Each release runner (Intel vs. Apple Silicon) produces a
*single-architecture* library, so a wheel must be tagged with the architecture
its bundled library actually contains (``x86_64`` or ``arm64``) -- never the
interpreter's own ``universal2`` tag, which would falsely advertise both
architectures and let ``pip`` install an incompatible wheel (see issue #9).

This module has no third-party dependencies (only the standard library) so it
can run inside an isolated ``python -m build`` environment, and it deliberately
avoids importing the parent package so ``setup.py`` can load it before the
package's runtime dependencies are installed.
"""

import struct
from collections.abc import Iterable

# Mach-O CPU type constants (see <mach/machine.h>).
_CPU_ARCH_ABI64 = 0x01000000
_CPU_TYPE_X86 = 0x00000007
_CPU_TYPE_ARM = 0x0000000C

_ARCH_BY_CPUTYPE = {
    _CPU_TYPE_X86 | _CPU_ARCH_ABI64: "x86_64",
    _CPU_TYPE_ARM | _CPU_ARCH_ABI64: "arm64",
    _CPU_TYPE_X86: "i386",
    _CPU_TYPE_ARM: "arm",
}

# Thin Mach-O magics -> (struct endianness prefix). MH_MAGIC/MH_CIGAM are the
# 32-bit forms; the *_64 variants are 64-bit. Only the byte order matters for
# reading the cputype field that follows the magic.
_THIN_MAGICS = {
    0xFEEDFACE: "<",  # MH_MAGIC     (little-endian host)
    0xFEEDFACF: "<",  # MH_MAGIC_64
    0xCEFAEDFE: ">",  # MH_CIGAM     (byte-swapped)
    0xCFFAEDFE: ">",  # MH_CIGAM_64
}

# Fat (universal) archive magics. These are always stored big-endian.
_FAT_MAGIC = 0xCAFEBABE
_FAT_MAGIC_64 = 0xCAFEBABF


def mach_o_architectures(path: str) -> set[str]:
    """Return the set of architectures contained in a Mach-O file.

    Recognizes both thin Mach-O files and fat/universal archives. Returns a set
    of architecture names such as ``{"x86_64"}``, ``{"arm64"}`` or
    ``{"x86_64", "arm64"}``. Unknown CPU types are ignored; a file that is not
    Mach-O (or is too short) yields an empty set.
    """
    with open(path, "rb") as fh:
        header = fh.read(4096)
    if len(header) < 8:
        return set()

    magic_be = struct.unpack(">I", header[:4])[0]

    # Fat/universal archive: a big-endian fat_header followed by nfat_arch
    # entries. The 64-bit variant widens the offset/size fields, so the entry
    # stride differs, but the cputype we care about stays a leading int32.
    if magic_be in (_FAT_MAGIC, _FAT_MAGIC_64):
        nfat = struct.unpack(">I", header[4:8])[0]
        stride = 32 if magic_be == _FAT_MAGIC_64 else 20
        archs: set[str] = set()
        offset = 8
        for _ in range(nfat):
            if offset + 4 > len(header):
                break
            cputype = struct.unpack(">I", header[offset : offset + 4])[0]
            name = _ARCH_BY_CPUTYPE.get(cputype)
            if name:
                archs.add(name)
            offset += stride
        return archs

    # Thin Mach-O: magic then cputype (int32) in the file's byte order.
    magic_le = struct.unpack("<I", header[:4])[0]
    for magic in (magic_le, magic_be):
        endian = _THIN_MAGICS.get(magic)
        if endian is not None:
            cputype = struct.unpack(endian + "I", header[4:8])[0]
            name = _ARCH_BY_CPUTYPE.get(cputype)
            return {name} if name else set()

    return set()


def platform_tag_arch(archs: Iterable[str]) -> str:
    """Map a set of Mach-O architectures to a wheel platform arch token.

    ``{"x86_64", "arm64"}`` -> ``"universal2"``; a single architecture maps to
    its own name. Raises :class:`ValueError` for an empty or unsupported set so
    a mistagged wheel can never be produced silently.
    """
    arch_set = set(archs)
    if arch_set == {"x86_64", "arm64"}:
        return "universal2"
    if len(arch_set) == 1:
        return next(iter(arch_set))
    raise ValueError(f"cannot derive a macOS wheel arch from {sorted(arch_set)!r}")


def retag_macos_platform(plat: str, archs: Iterable[str]) -> str:
    """Rewrite a ``macosx_*`` wheel platform tag to match ``archs``.

    ``retag_macos_platform("macosx_11_0_universal2", {"arm64"})`` returns
    ``"macosx_11_0_arm64"``. Non-macOS platform tags, and tags whose arch we
    cannot derive, are returned unchanged.
    """
    if not plat.startswith("macosx_"):
        return plat
    try:
        arch = platform_tag_arch(archs)
    except ValueError:
        return plat
    # A macOS tag is "macosx_<major>_<minor>_<arch>". The arch itself can
    # contain underscores (e.g. "x86_64"), so split off exactly the version
    # prefix (the first three underscore-separated fields) and swap the rest.
    parts = plat.split("_", 3)
    if len(parts) < 4:
        return plat
    parts[3] = arch
    return "_".join(parts)
