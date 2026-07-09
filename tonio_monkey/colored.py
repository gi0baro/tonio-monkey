import importlib

from ._impl._colored import patches as __avail_impls


for patch in __avail_impls:
    try:
        mod = importlib.import_module(f"tonio_monkey._impl._colored._{patch}")
        pkg = getattr(mod, patch)
    except AttributeError, ImportError:
        continue
    globals()[patch] = pkg
