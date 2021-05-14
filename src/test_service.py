import pytest
import traceback
from service import (
    create_logger,
    create_server,
    HeadlessnessServer,
)


@pytest.hookimpl(tryfirst=True)
def pytest_keyboard_interrupt(excinfo):
    traceback.format_exc()


async def test_post(aiohttp_client, loop):
    logger = create_logger()
    app, http_interface, http_port = create_server(logger, True)
    logger.info(f"Serving on {http_interface}:{http_port}")

    url = "/futch?url=http%3A%2F%2Fgoogle.com&transaction_id=2"
    client = await aiohttp_client(app)
    resp = await client.post(url)
    assert resp.status == 404, f"Got response for {url} {resp.status} {resp.text}"

    url = "/fetch?url=http%3A%2F%2Fgoogle.com&transaction_id=1"
    try:
        resp = await client.post(url)
    except Exception as e:
        assert False, f"Exception {e}"

    assert resp.status == 200, f"Got response for {url} {resp.status} {resp.text}"
    await HeadlessnessServer.browser.close()
