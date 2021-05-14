import asyncio
from urllib.parse import urlparse, parse_qs
import logging
from threading import Semaphore
from time import time
from os import environ
from dataclasses import dataclass
import json
from aiohttp import web

from process_url import (
    Page,
    generate_report,
    AdBlock,
)


@dataclass
class Statistics:
    timer_1s: int = 0

    throttle_latency: float = 0
    throttle_latency_max: float = 0
    throttle_failed: int = 0
    throttle_ok: int = 0
    throttle_pending_max: int = 0
    throttle_pending: int = 0
    bad_url_parameters: int = 0

    resp_200: int = 0
    resp_400: int = 0
    unknow_post: int = 0


_statistics = Statistics()


class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, transaction_id):
        super(LoggerAdapter, self).__init__(logger, {})
        self._transaction_id = transaction_id

    def process(self, msg, kwargs):
        return "[%s] %s" % (self._transaction_id, msg), kwargs


def _get_url_parameter(parameters, name, default=""):
    if default is None:
        return parameters.get(name, [None])[0]
    if isinstance(default, str):
        return parameters.get(name, [default])[0]
    if isinstance(default, int):
        res = parameters.get(name, [str(default)])[0]
        return int(res)
    if isinstance(default, float):
        res = parameters.get(name, [str(default)])[0]
        return float(res)


class HeadlessnessServer:
    _throttle_max, _throttle = 1, Semaphore(1)

    def __init__(self, logger, request):
        self._logger, self._ad_block = logger, request.app["ad_block"]
        # default process at most 1 query

    @staticmethod
    def _check_throttle():
        time_start = time()
        if not HeadlessnessServer._throttle.acquire(blocking=True, timeout=40.0):
            _statistics.throttle_failed += 1
            return False

        _statistics.throttle_ok += 1
        elapsed_time = time() - time_start
        _statistics.throttle_latency = elapsed_time
        _statistics.throttle_latency_max = max(
            _statistics.throttle_latency_max, _statistics.throttle_latency_max
        )
        _statistics.throttle_pending = (
            HeadlessnessServer._throttle_max - HeadlessnessServer._throttle._value
        )
        _statistics.throttle_pending_max = max(
            _statistics.throttle_pending_max, _statistics.throttle_pending
        )
        return True

    async def _fetch_page(self, results):
        """
        Called after if os.fork() == 0
        """
        self._logger.info(f"Fetching {self._url}")
        page = Page(
            logger=self._logger,
            timeout=self._timeout,
            keep_alive=False,
            ad_block=self._ad_block,
        )
        try:
            await page.load_page(self._transaction_id, self._url)
        except Exception as e:
            return str(e)

        report = generate_report(self._url, self._transaction_id, page)
        report_str = json.dumps(report, indent=2)
        results["report"] = report_str
        return None

    async def _process_post(self, parsed_url):
        if parsed_url.path not in ["/fetch"]:
            _statistics.unknow_post += 1
            err_msg = f"Unknown post {parsed_url.path}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        parameters = parse_qs(parsed_url.query)
        self._url = _get_url_parameter(parameters, "url", None)
        if self._url is None:
            _statistics.bad_url_parameters += 1
            err_msg = f"Missing URL parameter {parsed_url.path}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        self._timeout = _get_url_parameter(parameters, "timeout", 30.0)
        self._transaction_id = _get_url_parameter(parameters, "transaction_id")

        results = {}
        error = await self._fetch_page(results)
        report = results.get("report", f"Failed for {self._url}, results={results}")
        if error is not None:
            err_msg = f"Fetch failed for {self._url}: {error}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        error = results.get("error", None)
        if error is not None:
            err_msg = f"Fetch failed for {self._url}: {error}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        return web.HTTPSuccessful(text=report)

    @staticmethod
    async def do_POST(request):
        parsed_url = urlparse(request.path_qs)
        parameters = parse_qs(parsed_url.query)

        transaction_id = _get_url_parameter(parameters, "transaction_id")
        logger = LoggerAdapter(request.app["logger"], transaction_id)

        logger.debug(
            "POST request, path %s, headers %s",
            str(request.path_qs),
            str(request.headers),
        )
        if not HeadlessnessServer._check_throttle():
            err_msg = "Too many requests"
            logger.error(err_msg)
            raise web.HTTPNotFound(reason=err_msg)

        headlessness_server = HeadlessnessServer(logger, request)
        response = await headlessness_server._process_post(parsed_url)
        HeadlessnessServer._throttle.release()
        raise response


def create_logger():
    logger = logging.getLogger("headlessness")
    logger_format = "%(levelname)s:%(filename)s:%(lineno)d:%(message)s"
    logging.basicConfig(format=logger_format)
    loglevel = environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(loglevel)
    logger.debug("I am using debug log level")
    return logger


def create_server(logger):
    """
    https://docs.aiohttp.org/en/v0.22.4/web.html
    """

    http_port = int(environ.get("PORT", "8081"))
    http_interface = environ.get("INTERFACE", "0.0.0.0")

    app = web.Application()
    app["ad_block"] = AdBlock(["./ads-servers.txt", "./ads-servers.he.txt"])
    app["logger"] = logger

    app.router.add_post("/fetch", HeadlessnessServer.do_POST)
    loop = asyncio.get_event_loop()
    handler = app.make_handler()
    f = loop.create_server(handler, http_interface, http_port)
    srv = loop.run_until_complete(f)
    return srv, loop, http_interface, http_port


def main():
    logger = create_logger()
    _, loop, http_interface, http_port = create_server(logger)
    logger.info(f"Serving on {http_interface}:{http_port}")
    loop.run_forever()


if __name__ == "__main__":
    main()
