import asyncio
from pyppeteer import (
    launch,
    errors,
)
import easyargs
import logging
import json
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from pprint import PrettyPrinter
from datetime import datetime, timedelta
from urllib.parse import urlparse
from tempfile import mkdtemp
from contextlib import contextmanager
from shutil import rmtree
from os import path
from base64 import b64encode
from time import sleep
from os import environ

pretty_printer = PrettyPrinter(indent=4)


@contextmanager
def defer_close(thing):
    try:
        yield thing
    finally:
        thing.close()


@contextmanager
def tmpdir():
    try:
        temp_dir = mkdtemp(suffix="images")
        yield temp_dir
    finally:
        rmtree(temp_dir)


class AdBlock:
    def __init__(self, filenames):
        self.hosts = set()
        for filename in filenames:
            self._load(filename)

    def _load(self, filename):
        with open(filename) as f:
            for line in f:
                columns = line.split()
                if len(columns) < 2:
                    continue
                if columns[0] != "0.0.0.0":
                    continue
                self.hosts.add(columns[1])

    @staticmethod
    def _get_tld(hostname, count=2):
        words = hostname.split(".")
        if len(words) > count:
            words = words[-count:]
            return ".".join(words)
        return hostname

    def is_ad(self, hostname):
        return (
            hostname in self.hosts
            or AdBlock._get_tld(hostname, 2) in self.hosts
            or AdBlock._get_tld(hostname, 3) in self.hosts
        )


class AdBlockDummy:
    def is_ad(self, _):
        return False


async def get_browser(headless=True):
    chrome_args = ["--no-sandbox"]
    disable_cache = True  # does not appear to impact the performance
    if disable_cache:
        chrome_args += [
            "--disk-cache-size=0",
            "--disable-application-cache",
            "--enable-aggressive-domstorage-flushing",
        ]
    else:
        chrome_args += [
            f"--disk-cache-size={100_000_000}",
        ]

    browser = await launch(
        {
            "headless": headless,
            "args": chrome_args,
            "executablePath": "/usr/bin/google-chrome-stable",
            "logLevel": logging.ERROR,
        }
    )
    return browser


@dataclass_json
@dataclass
class RequestInfo:
    url: str = None
    host: str = None
    method: str = None
    status: int = None
    ts_request: datetime = None
    ts_response: datetime = None
    ts_start: datetime = None
    ts_last: datetime = None
    elapsed: float = 0.0
    is_ad: bool = False


class EventHandler:
    def __init__(self, ad_block, logger=None):
        self.requests_info = {}
        self.redirects = []
        self._ad_block = ad_block
        self.ts_start = datetime.now()
        if logger is None:
            logger = logging.getLogger("headlessness")
        self._logger = logger
        self._lock = asyncio.Lock()

    async def _process_request(self, r):
        if not hasattr(r, "url"):
            self._logger.error(f"request is missing url {r.__dir__()}")
            return True

        url = r.url

        if not hasattr(r, "_requestId"):
            self._logger.error(f"requestID is None for {url} {r.__dir__()}")
            return True

        request_id = r._requestId
        if request_id in self.requests_info:
            self._logger.error(
                f"requestID {request_id} is already in self.requests_info for {url}: {self.requests_info[request_id]}"
            )

        parsed_url = urlparse(url)
        is_ad = self._ad_block.is_ad(parsed_url.netloc)

        requests_info = RequestInfo(
            method=r.method,
            url=url,
            host=parsed_url.netloc,
            ts_request=datetime.now(),
            is_ad=is_ad,
        )
        self.requests_info[request_id] = requests_info

        if is_ad:
            return False

        return True

    async def _process_response(self, r):
        if not hasattr(r, "url"):
            self._logger.error(f"response is missing url {r.__dir__()}")
            return

        url = r.url
        if not r.request:
            self._logger.error(f"request is missing in the response for {url}")
            return

        if not hasattr(r.request, "_requestId"):
            self._logger.error(f"requestID is missing in response for {url}")
            return
        request_id = r.request._requestId

        if request_id not in self.requests_info:
            self._logger.error(
                f"requestID {request_id} is missing in map of requests for {url}"
            )
            return

        request_info = self.requests_info[request_id]
        if request_info.is_ad:
            self._logger.debug(f"got response for an ad {url}")

        request_info.status = r.status
        request_info.ts_response = datetime.now()
        request_info.elapsed = (
            request_info.ts_response - request_info.ts_request
        ).total_seconds()
        self.ts_last = datetime.now()
        self.requests_info[request_id] = request_info

    async def request_interception(self, r):
        # https://github.com/pyppeteer/pyppeteer/issues/198
        r.__setattr__("_allowInterception", True)
        async with self._lock:
            keep_going = await self._process_request(r)
        if keep_going:
            return await r.continue_()

        self._logger.debug(f"aborted ad {r.url}")
        return await r.abort()

    async def response_interception(self, r):
        r.__setattr__("_allowInterception", True)
        async with self._lock:
            await self._process_response(r)
        return

    async def request_will_be_sent(self, e):
        if "type" not in e:
            self._logger.error(f"request type is missing in {e}")
            return
        if "documentURL" not in e:
            self._logger.error(f"documentURL is missing in {e}")
            return

        request_type = e["type"]
        if request_type != "Document":
            return

        self._logger.debug(f"Redirect {pretty_printer.pformat(e)}")
        async with self._lock:
            self.redirects.append(e["documentURL"])


class Page:
    def __init__(self, logger=None, timeout=60.0, ad_block=AdBlockDummy()):
        self._timeout, self._ad_block = timeout, ad_block
        # Add stop page https://github.com/puppeteer/puppeteer/issues/3238
        self.content, self.screenshot, self._browser = None, None, None
        if logger is None:
            logger = logging.getLogger("headlessness")
        self._logger = logger
        self.event_handler = EventHandler(ad_block, self._logger)

    # https://stackoverflow.com/questions/48986851/puppeteer-get-request-redirects
    async def _get_page(self):
        page = await self._browser.newPage()

        # "True" is deafult value
        # 30-50% reduction in processing time
        await page.setCacheEnabled(enabled=True)

        # https://github.com/pyppeteer/pyppeteer/issues/198
        # await page.setRequestInterception(True)
        page.on(
            "request",
            lambda r: asyncio.ensure_future(self.event_handler.request_interception(r)),
        )

        page.on(
            "response",
            lambda r: asyncio.ensure_future(
                self.event_handler.response_interception(r)
            ),
        )

        client = await page.target.createCDPSession()
        await client.send("Network.enable")
        client.on(
            "Network.requestWillBeSent",
            lambda e: asyncio.ensure_future(self.event_handler.request_will_be_sent(e)),
        )
        return page

    async def _take_screenshot_until_succeds(self, page, url):
        """
        Page.goto waits until the 'load' event. Load can be an empty page with a JS
        triggering a redirect. The screnshot will fail with
        'pyppeteer.errors.NetworkError: Protocol error (Page.captureScreenshot): Cannot take screenshot with 0 width.`
        I am trying again until the allotted processing time ends
        """
        wait_until = self.event_handler.ts_start + timedelta(seconds=self._timeout)
        while datetime.now() < wait_until:
            screenshot = await self._take_screenshot(page, url)
            if screenshot is not None:
                return screenshot
            sleep(0.5)
            self._logger.info(
                f"Taking screenshot {url} failed, now is {datetime.now()}, trying until {wait_until}"
            )
        self._logger.info(
            f"Aborted screenshot for {url}, now is {datetime.now()}, trying until {wait_until}"
        )
        return None

    async def _take_screenshot(self, page, url):
        with tmpdir() as temp_dir:
            filename = path.join(temp_dir, "image.png")
            try:
                await page.screenshot({"path": filename, "fullPage": True})
            except errors.NetworkError:
                self._logger.exception(f"Failed to get screenshot for {url}")
                return None

            with open(filename, mode="r+b") as f:
                data = f.read()
                return b64encode(data).decode("utf-8")

    async def load_page(self, request_id, url, browser, headless):
        self._browser = browser
        page = await self._get_page()
        try:
            # page.timeout() accepts milliseconds
            await page.goto(
                url,
                {
                    "timeout": int(self._timeout * 1000),
                    "waitUntil": ["load"],  # "networkidle0"],
                },
            )
        except errors.TimeoutError:
            self._logger.exception(f"Failed to load {url}")

        self.screenshot = await self._take_screenshot_until_succeds(page, url)

        try:
            self.content = await page.content()
        except errors.NetworkError:
            self._logger.exception(f"Failed to get content for {url}")

        self._logger.info(f"Completed {url}")
        await page.close()

        return


def generate_report(url, request_id, page):
    event_handler = page.event_handler
    requests_info = event_handler.requests_info
    serializable_requests = []
    slow_responses = set()
    ads = set()
    info = {}
    for _, orig_request in requests_info.items():
        request = RequestInfo(**orig_request.__dict__)
        if request.ts_request:
            request.ts_request = request.ts_request.strftime("%m/%d/%Y %H:%M:%S.%f")
        if request.ts_response:
            request.ts_response = request.ts_response.strftime("%m/%d/%Y %H:%M:%S.%f")
        if request.elapsed > 5.0:
            slow_responses.add(f"0.0.0.0 {request.host}")
        if request.is_ad:
            ads.add(request.url)
        d = request.to_dict()
        serializable_requests.append(d)

    info["url"] = url
    info["request_id"] = request_id
    info["requests"] = serializable_requests
    info["redirects"] = event_handler.redirects
    info["slow_responses"] = list(slow_responses)
    info["ads"] = list(ads)

    # Try https://codebeautify.org/base64-to-image-converter
    if page.screenshot:
        info["screenshot"] = page.screenshot

    # Try https://www.base64decode.org/
    if page.content:
        info["content"] = b64encode(page.content.encode("utf-8")).decode("utf-8")

    info["elapsed"] = (event_handler.ts_last - event_handler.ts_start).total_seconds()
    return info


def dump_har(report, indent):
    entries = []
    for r in report["requests"]:
        request = {
            "request": {"method": r["method"], "url": r["url"]},
            "response": {"status": r["status"]},
            "timings": {
                "blocked": -1,
                "dns": -1,
                "ssl": -1,
                "connect": -1,
                "send": -1,
                "receive": -1,
                "wait": r["elapsed"],
            },
            "_is_ad": r["is_ad"],
            "_host": r["host"],
        }
        entries.append(request)

    creator = {"name": "Headless Chrome", "version": "0.0.0"}
    log = {
        "version": "1.2",
        "creator": creator,
        "_request_id": report["request_id"],
        "_redirects": report["redirects"],
        "_url": report["url"],
        "_slow_responses": report["slow_responses"],
        "_ads": report["ads"],
        "entries": entries,
    }
    if "screenshot" in report:
        log["_screenshot"] = report["screenshot"]
    if "content" in report:
        log["_content"] = report["content"]

    return json.dumps({"log": log}, indent=2)


def create_logger():
    logger = logging.getLogger("headlessness")
    logger_format = "%(levelname)s:%(filename)s:%(lineno)d:%(message)s"
    logging.basicConfig(format=logger_format)
    loglevel = environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(loglevel)
    logger.debug("I am using debug log level")
    return logger


class PageTrace:
    def __init__(self, logger):
        self._logger = logger
        self._active_pages = set()
        self._for_removal = set()
        self._lock = asyncio.Lock()

    async def add(self, page):
        async with self._lock:
            self._active_pages.add(page)

    async def rm(self, page, browser):
        async with self._lock:
            self._for_removal.add(page)
            await self._cleanup(browser)

    async def _cleanup(self, browser):
        open_pages = await browser.pages()
        _for_removal_new = set()
        for open_page in open_pages:
            if open_page not in self._for_removal:
                continue
            self._logger.error(
                f"Found stuck page for {open_page.url}, isClosed {open_page.isClosed()}"
            )
            await open_page.close()
            _for_removal_new.add(open_page)
        self._for_removal = _for_removal_new


@easyargs
def main(
    url="http://www.google.com",
    request_id=None,
    timeout=5.0,
    headless=False,
    report_type="json",
):
    ad_block = AdBlock(["./ads-servers.txt", "./ads-servers.he.txt"])
    logger = create_logger()
    logger.debug("I am using debug log level")

    logger.info(
        f"Starting Chrome for {url}, headless={headless}, report_type={report_type}"
    )

    page = Page(logger=logger, timeout=timeout, ad_block=ad_block)
    loop = asyncio.get_event_loop()
    browser = loop.run_until_complete(get_browser(headless))
    loop.run_until_complete(page.load_page(request_id, url, browser, headless))

    report = generate_report(url, request_id, page)
    if report_type == "json":
        report_str = json.dumps(report, indent=2)
    if report_type == "har":
        report_str = dump_har(report, indent=2)
    print(f"{report_str}")

    while not headless:
        sleep(1.0)
    browser.close()


if __name__ == "__main__":
    main()
