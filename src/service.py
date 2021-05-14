from urllib.parse import urlparse, parse_qs
import asyncio
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Semaphore, Thread
from time import time, sleep
from os import environ
from dataclasses import dataclass
import json

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


class HeadlessnessServer(BaseHTTPRequestHandler):
    def __init__(self, logger):
        self.logger = logger
        self.ad_block = AdBlock(["./ads-servers.txt", "./ads-servers.he.txt"])

    def __call__(self, *args, **kwargs):
        """
        See https://stackoverflow.com/questions/21631799/how-can-i-pass-parameters-to-a-requesthandler
        """
        super().__init__(*args, **kwargs)

    # default process at most 1 query
    _throttle_max, _throttle = 1, Semaphore(1)

    def _400(self, msg):
        try:
            self.send_response(400)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(msg.encode("utf-8"))
            _statistics.resp_400 += 1
        except Exception as e:
            self._logger.error(f"Faied to write to remote {e}")

    def _200(self, msg):
        try:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(msg.encode("utf-8"))
            _statistics.resp_200 += 1
        except Exception as e:
            self._logger.error(f"Faied to write to remote {e}")

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
        self.logger.info(f"Fetching {self.url}")
        page = Page(
            self._logger, timeout=self.timeout, keep_alive=False, ad_block=self.ad_block
        )
        asyncio_loop = asyncio.get_event_loop()
        asyncio_loop.run_until_complete(page.load_page(self._transaction_id, self.url))

        report = generate_report(self.url, self._transaction_id, page)
        report_str = json.dumps(report, indent=2)
        results["report"] = report_str

    def _process_post(self, parsed_url):
        if parsed_url.path not in ["/fetch"]:
            _statistics.unknow_post += 1
            err_msg = f"Unknown post {parsed_url.path}"
            self._logger.error(err_msg)
            self._400(err_msg)
            return

        parameters = parse_qs(parsed_url.query)
        self.url = _get_url_parameter(parameters, "url", None)
        if self.url is None:
            _statistics.bad_url_parameters += 1
            err_msg = f"Missing URL parameter {parsed_url.path}"
            self._logger.error(err_msg)
            self._400(err_msg)
            return

        self.timeout = _get_url_parameter(parameters, "timeout", 30.0)

        # asyncio releise on signal, requires main thread
        # I use os.fork + UNIX pipe magic
        error, results = AsyncCall(self.logger)(self.timeout, self._fetch_page, {})
        report = results.get("report", f"Failed for {self.url}, results={results}")
        if error is not None:
            err_msg = f"Fetch failed for {self.url}: {error}"
            self._logger.error(err_msg)
            self._400(err_msg)
            return

        error = results.get("error", None)
        if error is not None:
            err_msg = f"Fetch failed for {self.url}: {error}"
            self._logger.error(err_msg)
            self._400(err_msg)
            return

        self._200(report)

    def do_POST(self):
        parsed_url = urlparse(self.path)
        parameters = parse_qs(parsed_url.query)

        self._transaction_id = _get_url_parameter(parameters, "transaction_id")
        self._logger = LoggerAdapter(self.logger, self._transaction_id)
        self._logger.debug(
            "POST request, path %s, headers %s", str(self.path), str(self.headers)
        )

        if not self._check_throttle():
            err_msg = "Too many requests"
            self._logger.error(err_msg)
            self._400(err_msg)
            return

        self._process_post(parsed_url)
        HeadlessnessServer._throttle.release()


def shutdown():
    global is_running
    is_running = False


def main():
    logger = logging.getLogger("headlessness")
    logger_format = "%(levelname)s:%(filename)s:%(lineno)d:%(message)s"
    logging.basicConfig(format=logger_format)
    loglevel = environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(loglevel)
    logger.debug("I am using debug log level")

    http_port = int(environ.get("PORT", 8081))
    http_interface = environ.get("INTERFACE", "0.0.0.0")
    http_server = ThreadingHTTPServer(
        (http_interface, http_port), HeadlessnessServer(logger)
    )
    http_server_thread = Thread(target=http_server.serve_forever)
    http_server_thread.start()
    logger.info(f"I am listening {http_interface}:{http_port}")
    return http_server_thread, http_server


if __name__ == "__main__":
    is_running = True
    http_server_thread, http_server = main()
    while is_running:
        sleep(1.0)
        _statistics.timer_1s += 1
    http_server.shutdown()
    http_server_thread.join()
