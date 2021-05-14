import asyncio
from urllib.parse import urlparse, parse_qs
import asyncio
import logging
from threading import Semaphore, Thread
from time import time, sleep
from os import environ
from dataclasses import dataclass
import json
from aiohttp import web

from process_url import (
    Page,
    generate_report,
    AdBlock,
)
from fork import AsyncCall


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
    def __init__(self, logger):
        self.logger = logger
        self.ad_block = AdBlock(["./ads-servers.txt", "./ads-servers.he.txt"])
        # default process at most 1 query
        self._throttle_max, self._throttle = 1, Semaphore(1)

    def _check_throttle(self):
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

    def _fetch_page(self, data, results):
        """
        Called after if os.fork() == 0
        """
        self.logger.info(f"Fetching {url}")
        page = Page(
            self._logger, timeout=self.timeout, keep_alive=False, ad_block=self.ad_block
        )
        await page.load_page(transaction_id, url)
        report = generate_report(url, transaction_id, page)
        report_str = json.dumps(report, indent=2)
        results["report"] = report_str

    def _process_post(self, parsed_url):
        if parsed_url.path not in ["/fetch"]:
            _statistics.unknow_post += 1
            err_msg = f"Unknown post {parsed_url.path}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        parameters = parse_qs(parsed_url.query)
        url = _get_url_parameter(parameters, "url", None)
        if url is None:
            _statistics.bad_url_parameters += 1
            err_msg = f"Missing URL parameter {parsed_url.path}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        self.timeout = _get_url_parameter(parameters, "timeout", 30.0)

        # asyncio releise on signal, requires main thread
        # I use os.fork + UNIX pipe magic
        error, results = AsyncCall(self.logger)(self.timeout, self._fetch_page, {})
        report = results.get("report", f"Failed for {url}, results={results}")
        if error is not None:
            err_msg = f"Fetch failed for {url}: {error}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        error = results.get("error", None)
        if error is not None:
            err_msg = f"Fetch failed for {url}: {error}"
            self._logger.error(err_msg)
            return web.HTTPNotFound(reason=err_msg)

        return web.HTTPSuccessful(reason=report)

    async def do_POST(self, request):
        parsed_url = urlparse(request.path_qs)
        parameters = parse_qs(parsed_url.query)

        transaction_id = _get_url_parameter(parameters, "transaction_id")
        self._logger = LoggerAdapter(self.logger,transaction_id)
        self._logger.debug(
            "POST request, path %s, headers %s", str(self.path), str(self.headers)
        )

        if not self._check_throttle():
            err_msg = "Too many requests"
            self._logger.error(err_msg)
            raise web.HTTPNotFound(reason=err_msg)

        response = self._process_post(parsed_url)
        HeadlessnessServer._throttle.release()
        raise response

def main():
    logger = logging.getLogger("headlessness")
    logger_format = "%(levelname)s:%(filename)s:%(lineno)d:%(message)s"
    logging.basicConfig(format=logger_format)
    loglevel = environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(loglevel)
    logger.debug("I am using debug log level")

    http_port = int(environ.get("PORT", 8081))
    http_interface = environ.get("INTERFACE", "0.0.0.0")
    logger.info(f"I am listening {http_interface}:{http_port}")


    headlessness_server = HeadlessnessServer()
    app = web.Application()
    app.router.add_post('/fetch', headlessness_server.do_POST)
    web.run_app(app, host=http_interface, port=http_port)

if __name__ == "__main__":
    is_running = True
    main()
    while is_running:
        sleep(1.0)
        _statistics.timer_1s += 1
