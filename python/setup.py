# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build shim for adbc-driver-spark.

All project metadata lives in ``pyproject.toml``; this file exists only to mark
the distribution as binary. The package bundles a platform-specific compiled
shared library (``libadbc_driver_spark.{so,dylib,dll}``) as package data, but it
has no ``ext_modules``, so by default setuptools would emit a misleading
``py3-none-any`` (pure-Python) wheel. Overriding ``has_ext_modules`` forces a
correct platform wheel tag (e.g. ``macosx_*_x86_64``, ``manylinux_*_x86_64``,
``win_amd64``), which is also what ``auditwheel`` / ``delocate`` require during
the release workflow.

On macOS there is an extra hazard: the release runners build a *single*
architecture library each (Intel or Apple Silicon), but the python.org
interpreter reports a ``universal2`` platform tag. Left alone, ``bdist_wheel``
would stamp that ``universal2`` tag onto a single-arch wheel, so ``pip`` would
happily install an x86_64-only wheel on an arm64 Mac and ``dlopen`` would then
fail with "incompatible architecture" (issue #9). ``get_tag`` below inspects the
bundled ``.dylib`` and rewrites the platform tag to the architecture the library
actually contains.
"""

import os

from setuptools import setup
from setuptools.dist import Distribution

try:  # setuptools >= 70 vendors bdist_wheel
    from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # older setuptools: fall back to the wheel package
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


class BinaryDistribution(Distribution):
    """A Distribution that always reports platform-specific contents."""

    def has_ext_modules(self) -> bool:  # noqa: D102
        return True

    def is_pure(self) -> bool:  # noqa: D102
        return False


class bdist_wheel(_bdist_wheel):
    """Emit a ``py3-none-<platform>`` wheel.

    The bundled shared library is platform-specific but does not link against
    libpython, so a single wheel is valid for every CPython 3 version. We force
    a platform tag (root_is_pure = False) while keeping the Python/ABI tags
    version-agnostic, so we ship one wheel per platform rather than one per
    interpreter.
    """

    def finalize_options(self) -> None:  # noqa: D102
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):  # noqa: D102
        _python, _abi, plat = super().get_tag()
        return "py3", "none", _retag_macos(plat)


def _retag_macos(plat: str) -> str:
    """Correct a macOS ``universal2`` tag to the bundled library's real arch.

    On non-macOS platforms, or when no ``.dylib`` is bundled, the tag is
    returned unchanged. If a ``.dylib`` is present its Mach-O architecture wins,
    so a single-arch library can never ship under a ``universal2`` tag.
    """
    if not plat.startswith("macosx_"):
        return plat

    # Load the Mach-O helper directly by path so this works inside an isolated
    # PEP 517 build environment, where the package is not yet importable.
    import importlib.util

    here = os.path.dirname(os.path.abspath(__file__))
    macho_path = os.path.join(here, "adbc_driver_spark", "_macho.py")
    spec = importlib.util.spec_from_file_location("_adbc_spark_macho", macho_path)
    if spec is None or spec.loader is None:
        return plat
    macho = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(macho)

    dylib = os.path.join(here, "adbc_driver_spark", "libadbc_driver_spark.dylib")
    if not os.path.isfile(dylib):
        return plat
    archs = macho.mach_o_architectures(dylib)
    if not archs:
        return plat
    return macho.retag_macos_platform(plat, archs)


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": bdist_wheel})
