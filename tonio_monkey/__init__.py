import importlib

from ._impl import patches as __avail_impls


for patch in __avail_impls:
    try:
        mod = importlib.import_module(f"tonio_monkey._impl._{patch}")
        pkg = getattr(mod, patch)
    except (AttributeError, ImportError):
        continue
    globals()[patch] = pkg
