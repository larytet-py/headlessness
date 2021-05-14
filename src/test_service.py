import pytest
from requests import post
import traceback


@pytest.hookimpl(tryfirst=True)
def pytest_keyboard_interrupt(excinfo):
    traceback.format_exc()


@pytest.mark.asyncio
async def setup_module(module):
    pass


@pytest.mark.asyncio
async def teardown_module(module):
    pass


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
