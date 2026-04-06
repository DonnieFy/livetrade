# -*- coding: utf-8 -*-
"""
Livetrade — 策略包自动发现入口

导入此包时自动加载 strategies/ 目录下所有策略模块。
"""

import os
import importlib
import pkgutil

_pkg_dir = os.path.dirname(__file__)

for _importer, _modname, _ispkg in pkgutil.iter_modules([_pkg_dir]):
    importlib.import_module(f"{__name__}.{_modname}")
