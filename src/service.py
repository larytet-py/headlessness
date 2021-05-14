import asyncio
from urllib.parse import urlparse, parse_qs
import easyargs
import logging
from time import time
from os import environ
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import json
from aiohttp import web

from process_url import (
    Page,
    generate_report,
    AdBlock,
    get_browser,
)


@dataclass
@dataclass_json
class Statistics:
    e2e_latency: float = 0
    e2e_latency_max: float = 0

    throttle_latency: float = 0
    throttle_latency_max: float = 0
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
    _throttle_max = 10
    _throttle = asyncio.Semaphore(_throttle_max)

    browser = None
    main_lock = asyncio.Lock()

    def __init__(self, logger, request):
        self._logger, self._ad_block = logger, request.app["ad_block"]
        # default process at most 1 query

    async def _fetch_page(self, headless):
        """
        Called after if os.fork() == 0
        """
        self._logger.info(f"Fetching {self._url}")
        page = Page(
            logger=self._logger,
            timeout=self._timeout,
            ad_block=self._ad_block,
        )
        browser = HeadlessnessServer.browser
        close_browser = False
        if browser is None:
            browser = await get_browser(headless=headless)
            close_browser = True

        result = True
        try:
            await page.load_page(
                request_id=self._transaction_id,
                url=self._url,
                browser=browser,
                headless=headless,
            )
        except Exception as e:
            result = False
            report_str = str(e)

        if close_browser:
            await browser.close()

        if result:
            report = generate_report(self._url, self._transaction_id, page)
            report_str = json.dumps(report, indent=2)

        return result, report_str

    async def _process_post(self, parsed_url, headless):
        if parsed_url.path not in ["/fetch"]:
            _statistics.unknow_post += 1
            err_msg = f"Unknown post {parsed_url.path}"
            self._logger.error(err_msg)
            _statistics.resp_400 += 1
            return web.HTTPNotFound(reason=err_msg)

        parameters = parse_qs(parsed_url.query)
        self._url = _get_url_parameter(parameters, "url", None)
        if self._url is None:
            _statistics.bad_url_parameters += 1
            err_msg = f"Missing URL parameter {parsed_url.path}"
            self._logger.error(err_msg)
            _statistics.resp_400 += 1
            return web.HTTPNotFound(reason=err_msg)

        self._timeout = _get_url_parameter(parameters, "timeout", 30.0)
        self._transaction_id = _get_url_parameter(parameters, "transaction_id")

        ok, report = await self._fetch_page(headless)
        if not ok:
            err_msg = f"Fetch failed for {self._url}: {report}"
            self._logger.error(err_msg)
            _statistics.resp_400 += 1
            return web.HTTPNotFound(reason=err_msg)

        _statistics.resp_200 += 1
        return web.HTTPOk(text=report)

    @staticmethod
    async def do_fetch(request):
        time_start = time()

        headless = request.app["headless"]
        async with HeadlessnessServer.main_lock:
            if HeadlessnessServer.browser is None:
                HeadlessnessServer.browser = await get_browser(headless=headless)

        parsed_url = urlparse(request.path_qs)
        parameters = parse_qs(parsed_url.query)
        transaction_id = _get_url_parameter(parameters, "transaction_id")
        logger = LoggerAdapter(request.app["logger"], transaction_id)

        logger.info(
            "POST request, path %s, headers %s",
            str(request.path_qs),
            str(request.headers),
        )

        headlessness_server = HeadlessnessServer(logger, request)
        time_start = time()
        async with HeadlessnessServer._throttle:
            _statistics.throttle_latency = time() - time_start
            _statistics.throttle_latency_max = max(
                _statistics.throttle_latency_max, _statistics.throttle_latency
            )
            response = await headlessness_server._process_post(parsed_url, headless)

        _statistics.e2e_latency = time() - time_start
        _statistics.e2e_latency_max = max(
            _statistics.e2e_latency_max, _statistics.e2e_latency
        )
        raise response

    @staticmethod
    async def do_stats(request):
        parsed_url = urlparse(request.path_qs)
        parameters = parse_qs(parsed_url.query)
        format = _get_url_parameter(parameters, "format", "text")
        if format == "json":
            text = _statistics.to_json()
            raise web.HTTPOk(text=text)

        d = _statistics.to_dict()
        text = ""
        for key, value in d.items():
            if isinstance(value, int):
                s = "{: <22} {: >8}\n".format(key, str(value))
            else:
                s = "{: <22} {: >3.2f}\n".format(key, value)
            text += s
        raise web.HTTPOk(text=text)


def create_logger():
    logger = logging.getLogger("headlessness")
    logger_format = "%(levelname)s:%(filename)s:%(lineno)d:%(message)s"
    logging.basicConfig(format=logger_format)
    loglevel = environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(loglevel)
    logger.debug("I am using debug log level")
    return logger


def create_server(logger, headless):
    """
    https://docs.aiohttp.org/en/v0.22.4/web.html
    """

    http_port = int(environ.get("PORT", "8081"))
    http_interface = environ.get("INTERFACE", "0.0.0.0")

    app = web.Application()
    app["ad_block"] = AdBlock(["./ads-servers.txt", "./ads-servers.he.txt"])
    app["logger"] = logger
    app["headless"] = headless

    app.router.add_post("/fetch", HeadlessnessServer.do_fetch)
    app.router.add_get("/stats", HeadlessnessServer.do_stats)
    app.router.add_get("/", HeadlessnessServer.do_stats)
    return app, http_interface, http_port


@easyargs
def main(headless=False):
    logger = create_logger()
    app, http_interface, http_port = create_server(logger, headless)

    loop = asyncio.get_event_loop()
    handler = app.make_handler()
    f = loop.create_server(handler, http_interface, http_port)
    srv = loop.run_until_complete(f)
    logger.info(f"Serving on {http_interface}:{http_port} {srv}")
    loop.run_forever()


if __name__ == "__main__":
    main()
