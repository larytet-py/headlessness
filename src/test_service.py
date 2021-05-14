import pytest
from requests import post
import traceback
from aiohttp import web


@pytest.hookimpl(tryfirst=True)
def pytest_keyboard_interrupt(excinfo):
    traceback.format_exc()


@pytest.mark.asyncio
async def setup_module(module):
    pass


@pytest.mark.asyncio
async def teardown_module(module):
    pass


async def hello(request):
    return web.Response(text="Hello, world")


async def test_hello(aiohttp_client, loop):
    app = web.Application()
    app.router.add_get("/", hello)
    client = await aiohttp_client(app)
    resp = await client.get("/")
    assert resp.status == 200
    text = await resp.text()
    assert "Hello, world" in text


@pytest.mark.asyncio
async def t1st_post():
    url = "http://0.0.0.0:8081/futch?url=http%3A%2F%2Fgoogle.com&transaction_id=2"
    request_result = post(url)
    assert (
        request_result.status_code == 400
    ), f"Got response for {url} {request_result.status_code} {request_result.text}"

    url = "http://0.0.0.0:8081/fetch?url=http%3A%2F%2Fgoogle.com&transaction_id=1"
    try:
        request_result = post(url)
    except Exception:
        assert False, "Exception"
    assert (
        request_result.status_code == 200
    ), f"Got response for {url} {request_result.status_code} {request_result.text}"
