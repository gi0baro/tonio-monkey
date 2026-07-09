"""Minimal Django project used by the colored django integration tests.

Importable as a settings `ROOT_URLCONF`/`MIDDLEWARE` target, so it doubles as
the URLconf module. Call `setup()` once before issuing requests.
"""

from __future__ import annotations

import threading

from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.urls import path


def setup() -> None:
    from django.conf import settings

    # Activate the colored patches first (idempotent) so rebinds such as
    # `asgiref.testing.ApplicationCommunicator` are in place before any test
    # does `from asgiref.testing import ...`.
    from tonio_monkey.colored import django as _django  # noqa: F401

    if settings.configured:
        return
    settings.configure(
        DEBUG=True,
        DATABASES={},
        INSTALLED_APPS=[],
        USE_TZ=True,
        SECRET_KEY="tonio-monkey-test",
        ALLOWED_HOSTS=["*"],
        ROOT_URLCONF=__name__,
        MIDDLEWARE=[f"{__name__}.StampMiddleware"],
    )
    import django

    django.setup()


class StampMiddleware:
    """A plain *sync* middleware.

    Under the async handler Django adapts it via `sync_to_async`, so exercising
    it puts the asgiref adapter on the request path.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        response["X-Tonio-Thread"] = threading.current_thread().name
        return response


def sync_view(request):
    return HttpResponse(f"sync:{threading.current_thread().name}")


async def async_view(request):
    return JsonResponse({"hello": "async"})


async def async_stream_view(request):
    async def body():
        for idx in range(3):
            yield f"chunk{idx}".encode()

    return StreamingHttpResponse(body())


async def slow_view(request):
    import tonio.colored as tonio

    # Long enough that a client disconnect always wins the handler race.
    await tonio.sleep(30)
    return HttpResponse("slow")


urlpatterns = [
    path("sync/", sync_view),
    path("async/", async_view),
    path("stream/", async_stream_view),
    path("slow/", slow_view),
]
