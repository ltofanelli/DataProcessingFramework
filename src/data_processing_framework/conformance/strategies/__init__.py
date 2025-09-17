"""
Módulo de processadores específicos
"""

from .conformance_strategies import (IncrementalUpdate,
        IncrementalUpdateWithSCD2,
        IncrementalAppend,
        IncrementalOverwrite,
        #FullUpdate,
        #FullUpdateWithSCD2,
        FullOverwrite
)

__all__ = [
        "IncrementalUpdate",
        "IncrementalUpdateWithSCD2",
        "IncrementalAppend",
        "IncrementalOverwrite",
        #"FullUpdate",
        #"FullUpdateWithSCD2",
        "FullOverwrite"
    ]