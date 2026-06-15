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
"""

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
        return "py3", "none", plat


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": bdist_wheel})
