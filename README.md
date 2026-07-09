# TonIO-Monkey

Monkey-patch utilities for [TonIO](https://github.com/gi0baro/tonio).    
TonIO-Monkey lets you use popular asyncio packages with TonIO runtime.

> **Note:** the vast majority of code in this project is LLM generated.

## Available patches

TonIO-Monkey provides patches for the following packages:

- [asgiref](https://pypi.org/project/asgiref/) (*colored* only)
- [django](https://pypi.org/project/django/) (*colored* only)
- [httpx](https://pypi.org/project/httpx/) (*colored* only)
- [httpx2](https://pypi.org/project/httpx2/) (*colored* only)
- [psycopg](https://pypi.org/project/psycopg/) (*colored* only)
- [redis](https://pypi.org/project/redis/) (*colored* only)
- [websockets](https://pypi.org/project/websockets/) (client only, *colored* only)

## Installation

You can install TonIO-Monkey using pip or other package managers, specifying the Python packages you need patches for as extra dependencies:

    $ pip install tonio-monkey[httpx]

## Usage

Once you installed TonIO-Moneky with the relevant extras, you can simply import the target package from TonIO-Monkey:

```python
import tonio.colored as tonio
from tonio_monkey.colored import httpx

@tonio.main
async def main():
    async with httpx.AsyncClient() as client:
        r = await client.get('http://www.example.com')
```

## License

TonIO-Monkey is released under the BSD License.
