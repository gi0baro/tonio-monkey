# TonIO-Monkey

Monkey-patch utilities for [TonIO](https://github.com/gi0baro/tonio).

## Available patches

TonIO-Monkey provides patches for the following packages:

- [httpx](https://pypi.org/project/httpx/) (*colored* only)
- [psycopg](https://pypi.org/project/psycopg/) (*colored* only)

## Installation

You can install TonIO-Monkey using pip or other package managers, specifying the Python packages you need patches for as extra dependencies:

    $ pip install tonio-monkey[httpx]

## Usage

Once you installed TonIO-Moneky with the relevant extras, you can simply import the target package from TonIO-Monkey:

```python
from tonio_monkey.colored import httpx
```

## License

TonIO-Monkey is released under the BSD License.
