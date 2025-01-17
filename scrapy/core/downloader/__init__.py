from __future__ import annotations

import random
import warnings
from collections import deque
from datetime import datetime
from time import time
from typing import TYPE_CHECKING, Any, TypeVar, cast

from twisted.internet import task
from twisted.internet.defer import Deferred

from scrapy import Request, Spider, signals
from scrapy.core.downloader.handlers import DownloadHandlers
from scrapy.core.downloader.middleware import DownloaderMiddlewareManager
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.resolver import dnscache
from scrapy.signalmanager import SignalManager
from scrapy.utils.defer import mustbe_deferred
from scrapy.utils.httpobj import urlparse_cached

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from scrapy.http import Response
    from scrapy.settings import BaseSettings


_T = TypeVar("_T")


class Slot:
    """Downloader slot"""

    def __init__(
        self,
        concurrency: int,
        delay: float,
        randomize_delay: bool,
        *,
        throttle: bool | None = None,
    ):
        self.concurrency: int = concurrency
        self.delay: float = delay
        self.randomize_delay: bool = randomize_delay
        self.throttle = throttle

        self.active: set[Request] = set()
        self.queue: deque[tuple[Request, Deferred[Response]]] = deque()
        self.transferring: set[Request] = set()
        self.lastseen: float = 0
        self.latercall = None

    def free_transfer_slots(self) -> int:
        return self.concurrency - len(self.transferring)

    def download_delay(self) -> float:
        if self.randomize_delay:
            return random.uniform(0.5 * self.delay, 1.5 * self.delay)  # nosec
        return self.delay

    def close(self) -> None:
        if self.latercall and self.latercall.active():
            self.latercall.cancel()

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return (
            f"{cls_name}(concurrency={self.concurrency!r}, "
            f"delay={self.delay:.2f}, "
            f"randomize_delay={self.randomize_delay!r}, "
            f"throttle={self.throttle!r})"
        )

    def __str__(self) -> str:
        return (
            f"<downloader.Slot concurrency={self.concurrency!r} "
            f"delay={self.delay:.2f} randomize_delay={self.randomize_delay!r} "
            f"throttle={self.throttle!r} "
            f"len(active)={len(self.active)} len(queue)={len(self.queue)} "
            f"len(transferring)={len(self.transferring)} "
            f"lastseen={datetime.fromtimestamp(self.lastseen).isoformat()}>"
        )


def _get_concurrency_delay(
    concurrency: int, spider: Spider, settings: BaseSettings
) -> tuple[int, float]:
    delay: float = settings.getfloat("DOWNLOAD_DELAY")
    if hasattr(spider, "download_delay"):
        delay = spider.download_delay

    if hasattr(spider, "max_concurrent_requests"):
        concurrency = spider.max_concurrent_requests

    return concurrency, delay


class Downloader:
    DOWNLOAD_SLOT = "download_slot"

    def __init__(self, crawler: Crawler):
        # 在初始化过程中，主要是初始化了下载处理器、下载器中间件管理器以及从配置文件中拿到抓取请求控制的相关参数。
        self.settings: BaseSettings = crawler.settings  # 同样的 拿到settings对象
        self.signals: SignalManager = crawler.signals
        self.slots: dict[str, Slot] = {}
        self.active: set[Request] = set()
        self.handlers: DownloadHandlers = DownloadHandlers(crawler)  # 初始化DownloadHandlers下载处理器
        self.total_concurrency: int = self.settings.getint("CONCURRENT_REQUESTS")  # 从配置中获取设置的并发数
        self.domain_concurrency: int = self.settings.getint(
            "CONCURRENT_REQUESTS_PER_DOMAIN"
        )  # 同一域名并发数
        self.ip_concurrency: int = self.settings.getint("CONCURRENT_REQUESTS_PER_IP")  # 同一IP并发数
        self.randomize_delay: bool = self.settings.getbool("RANDOMIZE_DOWNLOAD_DELAY")  # 随机延迟下载时间
        self.middleware: DownloaderMiddlewareManager = (
            DownloaderMiddlewareManager.from_crawler(crawler)
        )  # 初始化下载器中间件
        self._slot_gc_loop: task.LoopingCall = task.LoopingCall(self._slot_gc)
        self._slot_gc_loop.start(60)
        self.per_slot_settings: dict[str, dict[str, Any]] = self.settings.getdict(
            "DOWNLOAD_SLOTS", {}
        )

    def fetch(self, request: Request, spider: Spider) -> Deferred[Response | Request]:
        def _deactivate(response: _T) -> _T:
            # 下载结束后删除此记录
            self.active.remove(request)
            return response

        # 下载前记录处理中的请求
        self.active.add(request)
        # 调用下载器中间件download 并注册下载成功的回调方法是self._enqueue_request
        dfd: Deferred[Response | Request] = self.middleware.download(
            self._enqueue_request, request, spider
        )
        # 注册结束回调
        return dfd.addBoth(_deactivate)

    def needs_backout(self) -> bool:
        return len(self.active) >= self.total_concurrency

    def _get_slot(self, request: Request, spider: Spider) -> tuple[str, Slot]:
        key = self.get_slot_key(request)
        if key not in self.slots:
            slot_settings = self.per_slot_settings.get(key, {})
            conc = (
                self.ip_concurrency if self.ip_concurrency else self.domain_concurrency
            )
            conc, delay = _get_concurrency_delay(conc, spider, self.settings)
            conc, delay = (
                slot_settings.get("concurrency", conc),
                slot_settings.get("delay", delay),
            )
            randomize_delay = slot_settings.get("randomize_delay", self.randomize_delay)
            throttle = slot_settings.get("throttle", None)
            new_slot = Slot(conc, delay, randomize_delay, throttle=throttle)
            self.slots[key] = new_slot

        return key, self.slots[key]

    def get_slot_key(self, request: Request) -> str:
        if self.DOWNLOAD_SLOT in request.meta:
            return cast(str, request.meta[self.DOWNLOAD_SLOT])

        key = urlparse_cached(request).hostname or ""
        if self.ip_concurrency:
            key = dnscache.get(key, key)

        return key

    def _get_slot_key(self, request: Request, spider: Spider | None) -> str:
        warnings.warn(
            "Use of this protected method is deprecated. Consider using its corresponding public method get_slot_key() instead.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return self.get_slot_key(request)

    def _enqueue_request(self, request: Request, spider: Spider) -> Deferred[Response]:
        # 加入下载请求队列
        key, slot = self._get_slot(request, spider)
        request.meta[self.DOWNLOAD_SLOT] = key

        def _deactivate(response: Response) -> Response:
            slot.active.remove(request)
            return response

        slot.active.add(request)
        self.signals.send_catch_log(
            signal=signals.request_reached_downloader, request=request, spider=spider
        )
        deferred: Deferred[Response] = Deferred().addBoth(_deactivate)
        slot.queue.append((request, deferred))
        self._process_queue(spider, slot)
        return deferred

    def _process_queue(self, spider: Spider, slot: Slot) -> None:
        from twisted.internet import reactor

        if slot.latercall and slot.latercall.active():
            return

        # Delay queue processing if a download_delay is configured
        now = time()
        delay = slot.download_delay()
        if delay:
            penalty = delay - now + slot.lastseen
            if penalty > 0:
                slot.latercall = reactor.callLater(
                    penalty, self._process_queue, spider, slot
                )
                return

        # Process enqueued requests if there are free slots to transfer for this slot
        while slot.queue and slot.free_transfer_slots() > 0:
            slot.lastseen = now
            request, deferred = slot.queue.popleft()
            dfd = self._download(slot, request, spider)
            dfd.chainDeferred(deferred)
            # prevent burst if inter-request delays were configured
            if delay:
                self._process_queue(spider, slot)
                break

    def _download(
        self, slot: Slot, request: Request, spider: Spider
    ) -> Deferred[Response]:
        # The order is very important for the following deferreds. Do not change!

        # 1. Create the download deferred
        dfd: Deferred[Response] = mustbe_deferred(
            self.handlers.download_request, request, spider
        )

        # 2. Notify response_downloaded listeners about the recent download
        # before querying queue for next request
        def _downloaded(response: Response) -> Response:
            self.signals.send_catch_log(
                signal=signals.response_downloaded,
                response=response,
                request=request,
                spider=spider,
            )
            return response

        dfd.addCallback(_downloaded)

        # 3. After response arrives, remove the request from transferring
        # state to free up the transferring slot so it can be used by the
        # following requests (perhaps those which came from the downloader
        # middleware itself)
        slot.transferring.add(request)

        def finish_transferring(_: _T) -> _T:
            slot.transferring.remove(request)
            self._process_queue(spider, slot)
            self.signals.send_catch_log(
                signal=signals.request_left_downloader, request=request, spider=spider
            )
            return _

        return dfd.addBoth(finish_transferring)

    def close(self) -> None:
        self._slot_gc_loop.stop()
        for slot in self.slots.values():
            slot.close()

    def _slot_gc(self, age: float = 60) -> None:
        mintime = time() - age
        for key, slot in list(self.slots.items()):
            if not slot.active and slot.lastseen + slot.delay < mintime:
                self.slots.pop(key).close()
