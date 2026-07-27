# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the License).

"""Tests for Mach-O architecture detection and macOS wheel retagging.

These guard against the regression in issue #9, where single-architecture macOS
wheels were tagged ``universal2`` and ``pip`` installed an x86_64-only wheel on
an arm64 Mac (``dlopen`` then failed with "incompatible architecture").

The Mach-O headers are synthesized in-process, so the tests need no compiler,
no network and no real ``.dylib``.
"""

import struct

import pytest

from adbc_driver_spark import _macho

_CPU_ARCH_ABI64 = 0x01000000
_CPU_TYPE_X86 = 0x00000007
_CPU_TYPE_ARM = 0x0000000C
_CPU_TYPE_X86_64 = _CPU_TYPE_X86 | _CPU_ARCH_ABI64
_CPU_TYPE_ARM64 = _CPU_TYPE_ARM | _CPU_ARCH_ABI64


def _thin(magic: int, endian: str, cputype: int) -> bytes:
    # magic, cputype, cpusubtype, filetype, ... — we only need the first two
    # fields to be well formed; pad the rest of a mach_header_64 with zeros.
    return struct.pack(endian + "II", magic, cputype) + b"\x00" * 24


def _fat(cputypes, magic=0xCAFEBABE) -> bytes:
    # fat_header: magic, nfat_arch (both big-endian), then one fat_arch per
    # slice: cputype, cpusubtype, offset, size, align.
    out = struct.pack(">II", magic, len(cputypes))
    for ct in cputypes:
        out += struct.pack(">IIIII", ct, 0, 0, 0, 0)
    return out


def _write(tmp_path, data: bytes):
    p = tmp_path / "lib.dylib"
    p.write_bytes(data)
    return str(p)


# --- mach_o_architectures -------------------------------------------------


def test_thin_x86_64_little_endian(tmp_path):
    path = _write(tmp_path, _thin(0xFEEDFACF, "<", _CPU_TYPE_X86_64))
    assert _macho.mach_o_architectures(path) == {"x86_64"}


def test_thin_arm64_little_endian(tmp_path):
    path = _write(tmp_path, _thin(0xFEEDFACF, "<", _CPU_TYPE_ARM64))
    assert _macho.mach_o_architectures(path) == {"arm64"}


def test_thin_32bit_magic(tmp_path):
    path = _write(tmp_path, _thin(0xFEEDFACE, "<", _CPU_TYPE_X86))
    assert _macho.mach_o_architectures(path) == {"i386"}


def test_thin_big_endian_byteswapped(tmp_path):
    # A big-endian Mach-O stores the MH_MAGIC_64 constant in big-endian byte
    # order (on disk: FE ED FA CF); read little-endian that is MH_CIGAM_64. The
    # cputype is likewise big-endian and must still resolve.
    path = _write(tmp_path, _thin(0xFEEDFACF, ">", _CPU_TYPE_ARM64))
    assert _macho.mach_o_architectures(path) == {"arm64"}


def test_fat_universal2(tmp_path):
    path = _write(tmp_path, _fat([_CPU_TYPE_X86_64, _CPU_TYPE_ARM64]))
    assert _macho.mach_o_architectures(path) == {"x86_64", "arm64"}


def test_fat_64bit_magic(tmp_path):
    # fat_arch_64 widens offset/size/align, so the entry stride is 32 bytes.
    magic = 0xCAFEBABF
    out = struct.pack(">II", magic, 2)
    for ct in (_CPU_TYPE_X86_64, _CPU_TYPE_ARM64):
        out += struct.pack(">IIQQII", ct, 0, 0, 0, 0, 0)
    path = _write(tmp_path, out)
    assert _macho.mach_o_architectures(path) == {"x86_64", "arm64"}


def test_not_mach_o(tmp_path):
    path = _write(tmp_path, b"\x7fELF" + b"\x00" * 60)
    assert _macho.mach_o_architectures(path) == set()


def test_too_short(tmp_path):
    path = _write(tmp_path, b"\xff\xff")
    assert _macho.mach_o_architectures(path) == set()


# --- platform_tag_arch ----------------------------------------------------


def test_platform_tag_arch_single():
    assert _macho.platform_tag_arch({"arm64"}) == "arm64"
    assert _macho.platform_tag_arch({"x86_64"}) == "x86_64"


def test_platform_tag_arch_universal2():
    assert _macho.platform_tag_arch({"x86_64", "arm64"}) == "universal2"


def test_platform_tag_arch_empty_raises():
    with pytest.raises(ValueError):
        _macho.platform_tag_arch(set())


# --- retag_macos_platform -------------------------------------------------


def test_retag_universal2_to_arm64():
    assert (
        _macho.retag_macos_platform("macosx_11_0_universal2", {"arm64"})
        == "macosx_11_0_arm64"
    )


def test_retag_universal2_to_x86_64():
    # The x86_64 arch itself contains an underscore; the version prefix must
    # still be preserved intact.
    assert (
        _macho.retag_macos_platform("macosx_15_0_universal2", {"x86_64"})
        == "macosx_15_0_x86_64"
    )


def test_retag_keeps_universal2_when_both_present():
    assert (
        _macho.retag_macos_platform("macosx_11_0_universal2", {"x86_64", "arm64"})
        == "macosx_11_0_universal2"
    )


def test_retag_leaves_non_macos_untouched():
    for plat in ("manylinux_2_34_x86_64", "win_amd64", "linux_aarch64"):
        assert _macho.retag_macos_platform(plat, {"arm64"}) == plat


def test_retag_unknown_arch_left_untouched():
    assert (
        _macho.retag_macos_platform("macosx_11_0_universal2", set())
        == "macosx_11_0_universal2"
    )
