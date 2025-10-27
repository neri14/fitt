import argparse

from dataclasses import dataclass
from typing import Callable

@dataclass
class Tool:
    name: str
    description: str
    add_argparser: Callable[[argparse._SubParsersAction], None]
    main: Callable[..., int]

    def __call__(self, **kwargs) -> int: # type: ignore[no-untyped-def]
        return self.main(**kwargs)
