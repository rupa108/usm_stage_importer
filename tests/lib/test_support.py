# -*- coding: utf-8 -*-
"""Shared test harness for exercising :mod:`stage_importer_framework`.

This module centralizes everything needed to run the framework *without* the
real USM runtime:

* :func:`install_fake_usm_module` registers a fake ``de.usu.s3.api`` package in
  ``sys.modules`` so the framework's top-level imports succeed.
* :func:`install_fake_vm_builtin` seeds the global ``VM`` / ``transaction`` the
  framework expects (``VM`` is referenced at *import time*, so it must exist as a
  builtin before the framework is imported).
* :func:`bootstrap` performs both steps in the correct order and returns the VM.
* :func:`reset_environment` clears the VM stores and installs a fresh
  transaction; tests call it from ``setUp`` for isolation.

The order matters: callers must invoke :func:`bootstrap` (or the two install
functions) *before* importing :mod:`stage_importer_framework`.
"""

import sys
import types

from . import fake_bo


def _builtins_module():
    try:
        import __builtin__ as builtins  # Python 2 / Jython
    except ImportError:  # pragma: no cover - Python 3 fallback
        import builtins
    return builtins


def install_fake_usm_module():
    """Register a fake ``de.usu.s3.api`` package tree in ``sys.modules``.

    The framework does ``from de.usu.s3.api import ApiBObject, ApiTransaction,
    ApiBOType`` at import time. None of those symbols carry behaviour the tests
    rely on (the real work goes through ``VM``), so simple placeholder classes
    are enough to satisfy the import.
    """
    # Build the nested package path de -> de.usu -> de.usu.s3 -> de.usu.s3.api
    for name in ("de", "de.usu", "de.usu.s3"):
        if name not in sys.modules:
            mod = types.ModuleType(name)
            mod.__path__ = []  # mark as package
            sys.modules[name] = mod

    api = types.ModuleType("de.usu.s3.api")
    api.ApiBObject = fake_bo.FakeBo
    api.ApiTransaction = fake_bo.FakeTransaction
    api.ApiBOType = fake_bo.FakeBOType
    sys.modules["de.usu.s3.api"] = api
    # Wire the attribute onto the parent package too.
    sys.modules["de.usu.s3"].api = api


def install_fake_vm_builtin():
    """Expose a global ``VM`` the way the Jython USM runtime would.

    ``stage_importer_framework`` references ``VM`` at *import time* (e.g. as the
    default value of a function argument: ``trl_type=VM.TRL_CURRENT``), so it has
    to exist as a builtin before the module is imported. The orchestrator also
    falls back to a module-level ``transaction`` global. We seed both with the
    fakes; individual tests rebind them to fresh instances via
    :func:`reset_environment`.
    """
    builtins = _builtins_module()

    vm = fake_bo.FakeVMApi()
    # Register type metadata up front. Processor classes resolve their
    # `target_type` against this VM at *import time* (via the metaclass), so the
    # same VM instance must stay in use for the lifetime of the test module; we
    # only reset its stores between tests for isolation.
    vm.configure_type("System", business_key_attr="systemname")
    builtins.VM = vm
    builtins.transaction = fake_bo.FakeTransaction()
    return vm


def bootstrap():
    """Install the fake USM module and global ``VM`` and return the VM.

    Must be called *before* importing :mod:`stage_importer_framework`.
    """
    install_fake_usm_module()
    return install_fake_vm_builtin()


def reset_environment(vm):
    """Reset state for a single test and return a fresh transaction.

    Reuses the single import-time ``vm`` (processor classes cached their
    ``target_type`` against it) but clears its stores for isolation, and rebinds
    the global ``transaction`` so commit counters start from zero.
    """
    vm.reset()
    tr = fake_bo.FakeTransaction()
    _builtins_module().transaction = tr
    return tr
