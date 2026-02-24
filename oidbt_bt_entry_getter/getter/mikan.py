import asyncio
import xml.etree.ElementTree
from typing import TYPE_CHECKING, override

import httpx
from bencode2 import BencodeDecodeError
from oidbt_torrent import Torrent
from pydantic import ValidationError

from ..log import log
from ..utils import get_size_str
from .base import Base_bt_entry_getter

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class Mikan_bt_entry_getter(Base_bt_entry_getter):
    class Website_entry_data_mikan(Base_bt_entry_getter.Website_entry_data, table=True):
        pass

    @property
    def page_link_head(self) -> str:
        return "https://mikanani.me/Home/Episode/"

    @classmethod
    def get_website_name(cls) -> str:
        return "mikan"

    @override
    def website_entry_to_data(
        self,
        *,
        website_entry: Base_bt_entry_getter.Website_entry,
        id_list: list[int],
    ) -> Mikan_bt_entry_getter.Website_entry_data_mikan:
        magnet = b""
        try:
            torrent = Torrent(website_entry.torrent)
        except BencodeDecodeError as e:
            log.error(
                "Bencode 解码错误: {} from {} {}",
                e,
                website_entry.title,
                website_entry.page_link,
            )
        else:
            magnet = torrent.get_magnet(tr=False).encode()

        return self.Website_entry_data_mikan(
            page_link_point=website_entry.page_link.removeprefix(self.page_link_head),
            magnet=magnet,
            match_id_list=id_list,
        )

    @override
    async def get_website_entry(
        self,
        *,
        sleep_time: int,
        fast_skip: bool,
    ) -> AsyncGenerator[Mikan_bt_entry_getter.Website_entry]:
        torrent_num: int = 0
        """记录本次循环下载的 torrent 数量"""

        class _req_fialed(Exception):
            pass

        class _req_end(Exception):
            pass

        async def _req(
            page_num: int, /
        ) -> AsyncGenerator[Mikan_bt_entry_getter.Website_entry]:
            """返回 None 表示 page 超出范围"""
            nonlocal torrent_num
            torrent_num_new = torrent_num
            try:
                log.debug("{} 开始请求第 {} 页", self.__class__.__name__, page_num)
                url = f"https://mikanani.me/RSS/Classic/{page_num}"
                response = await self.client.get(url)
                log.debug(
                    "{} 请求头: {} {}",
                    self.__class__.__name__,
                    url,
                    response.request.headers,
                    print_level=log.LogLevel._detail,
                )
                response.raise_for_status()
                log.debug(
                    "{} 响应头: {} {} {}",
                    self.__class__.__name__,
                    response.http_version,
                    response.status_code,
                    response.headers,
                    print_level=log.LogLevel._detail,
                )

                xml_data = xml.etree.ElementTree.fromstring(response.text)
                xml_data_channel = xml_data.find("channel")
                assert xml_data_channel is not None, (
                    f"{self.__class__.__name__} RSS 的 XML 结构没有 <channel>"
                )
                xml_data_channel_items = xml_data_channel.findall("item")
                if xml_data_channel_items is None:
                    # 没有 item 意味着翻页结束
                    raise _req_end
                for item in xml_data_channel_items:
                    title = item.find("title")
                    assert title is not None, (
                        f"{self.__class__.__name__} RSS 的 XML 结构 <item> 中没有 <title>"
                    )
                    title = title.text
                    assert title is not None, (
                        f"{self.__class__.__name__} RSS 的 XML 结构 <item><title> 没有内容"
                    )

                    link = item.find("link")
                    assert link is not None, (
                        f"{self.__class__.__name__} RSS 的 XML 结构 <item> 中没有 <link>"
                    )
                    page_link = link.text
                    assert page_link is not None, (
                        f"{self.__class__.__name__} RSS 的 XML 结构 <item><link> 没有内容"
                    )

                    enclosure = item.find("enclosure")
                    assert enclosure is not None, (
                        f"{self.__class__.__name__} RSS 的 XML 结构 <item> 中没有 <enclosure>"
                    )
                    torrent_url = enclosure.get("url")
                    assert torrent_url is not None, (
                        f"{self.__class__.__name__} RSS 的 XML 结构 <item><enclosure> 没有属性 url"
                    )

                    if fast_skip and await self.get_data(
                        page_link.removeprefix(self.page_link_head)
                    ):
                        log.debug(
                            "{} 跳过第 {} 个种子 {}",
                            self.__class__.__name__,
                            torrent_num_new,
                            title,
                        )
                        torrent_num_new += 1
                        continue

                    log.debug(
                        "{} 开始下载 {}",
                        self.__class__.__name__,
                        title,
                    )
                    async with self.client.stream(
                        "GET", torrent_url
                    ) as torrent_response:
                        log.debug(
                            "{} 请求头: {} {}",
                            self.__class__.__name__,
                            torrent_url,
                            torrent_response.request.headers,
                            print_level=log.LogLevel._detail,
                        )
                        try:
                            torrent_response.raise_for_status()
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code == 404:
                                log.warning(
                                    "{} 404 响应，可能不存在此文件，跳过此文件下载: {}",
                                    self.__class__.__name__,
                                    f"{title} {page_link}",
                                )
                                torrent_num_new += 1
                                continue
                            raise
                        log.debug(
                            "{} 响应头: {} {} {}",
                            self.__class__.__name__,
                            torrent_response.http_version,
                            torrent_response.status_code,
                            torrent_response.headers,
                            print_level=log.LogLevel._detail,
                        )
                        torrent = await torrent_response.aread()

                    torrent_num_new += 1
                    log.debug(
                        "{} 下载第 {} 个 torrent 体积: {} from {} {}",
                        self.__class__.__name__,
                        torrent_num_new,
                        get_size_str(torrent),
                        title,
                        torrent_url,
                    )

                    await asyncio.sleep(sleep_time)

                    yield self.Website_entry(
                        title=title,
                        page_link=page_link,
                        torrent=torrent,
                    )

                torrent_num = torrent_num_new

            except httpx.HTTPStatusError as e:
                log.error(
                    "{} 状态码错误: {}", self.__class__.__name__, e.response.status_code
                )
                raise _req_fialed from e
            except httpx.ConnectError as e:
                log.error("{} 连接失败: {!r}", self.__class__.__name__, e)
                raise _req_fialed from e
            except httpx.TimeoutException as e:
                log.warning("{} 请求超时", self.__class__.__name__)
                raise _req_fialed from e
            except httpx.ReadError as e:
                log.warning("{} 未知错误: {} {!r}", self.__class__.__name__, e, e)
                raise _req_fialed from e
            except ValidationError as e:
                log.error("{} 类型错误: {!r}", self.__class__.__name__, e)
                raise

        page_num: int = 1
        while True:
            try:
                async for website_entry in _req(page_num):
                    yield website_entry
            except _req_fialed as e:
                log.warning("{} 单次请求失败: {}", self.__class__.__name__, e)
                continue
            except _req_end:
                log.debug("{} 本次翻页循环结束", self.__class__.__name__)
                break

            await asyncio.sleep(sleep_time)
            page_num += 1

    @property
    def Data_class(self):
        return self.Website_entry_data_mikan
