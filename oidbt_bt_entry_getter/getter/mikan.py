import asyncio
import re
import xml.etree.ElementTree
from html.parser import HTMLParser
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
    @override
    def page_link_head(self) -> str:
        return "https://mikanani.me/Home/Episode/"

    @override
    async def match_ani_special(self, html_text: str, /) -> list[int]:
        to_link = re.search(r"/Home/Bangumi/\d+", html_text)
        if to_link is None:
            log.debug(
                "{} {} 没找到跳转链接",
                self.__class__.__name__,
                self.match_ani_special.__name__,
            )
            return []
        to_link = "https://mikanani.me" + to_link.group()
        log.debug("to_link = {}", to_link)
        while True:
            try:
                response = await self.req(to_link)
            except self.req_fialed:
                log.error(
                    "{} {} 请求失败",
                    self.__class__.__name__,
                    self.match_ani_special.__name__,
                )
                continue
            break

        class _HTMLParser(HTMLParser):
            """AI 写的解析逻辑"""

            def __init__(self) -> None:
                super().__init__()
                self.a_tag = None
                self.in_p = False
                self.p_class_match = False

            def handle_starttag(self, tag, attrs) -> None:
                attrs_dict = dict(attrs)
                if tag == "p" and attrs_dict.get("class") == "bangumi-info":
                    self.in_p = True
                elif tag == "a" and self.in_p:
                    self.a_tag = attrs_dict

            def handle_endtag(self, tag) -> None:
                if tag == "p":
                    self.in_p = False

        parser = _HTMLParser()
        parser.feed(response.text)

        a_tag = parser.a_tag
        if a_tag is None:
            log.error(
                "{} {} 没找到 a 标签",
                self.__class__.__name__,
                self.match_ani_special.__name__,
            )
            return []

        href = a_tag.get("href")
        if href is None:
            log.error(
                "{} {} a 标签没有 href",
                self.__class__.__name__,
                self.match_ani_special.__name__,
            )
            return []

        res = await self.match_ani_from_text(href)
        if len(res) != 1:
            log.error(
                "{} {} a 标签的 href 格式错误: {}",
                self.__class__.__name__,
                self.match_ani_special.__name__,
                href,
            )

        return res

    @override
    def website_entry_to_data(
        self,
        *,
        website_entry: Base_bt_entry_getter.Website_entry,
        id_list: list[int],
        only_refresh_data: Mikan_bt_entry_getter.Website_entry_data | None = None,
    ) -> Mikan_bt_entry_getter.Website_entry_data_mikan:
        if not (
            magnet := b"" if only_refresh_data is None else only_refresh_data.magnet
        ):
            try:
                torrent = Torrent(website_entry.torrent)
            except BencodeDecodeError as e:
                log.error(
                    "Bencode 解码错误: {} from {} {}",
                    e,
                    website_entry.title,
                    website_entry.page_link,
                    deep=True,
                )
            else:
                magnet = torrent.get_magnet(tr=False).encode()

        return self.Website_entry_data_mikan(
            title=website_entry.title,
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

        class req_end(Exception):
            pass

        async def _req(
            page_num: int, /
        ) -> AsyncGenerator[Mikan_bt_entry_getter.Website_entry]:
            nonlocal torrent_num
            torrent_num_new = torrent_num
            try:
                log.debug("{} 开始请求第 {} 页", self.__class__.__name__, page_num)
                url = f"https://mikanani.me/RSS/Classic/{page_num}"
                response = await self.req(url)

                xml_data = xml.etree.ElementTree.fromstring(response.text)
                xml_data_channel = xml_data.find("channel")
                assert xml_data_channel is not None, (
                    f"{self.__class__.__name__} RSS 的 XML 结构没有 <channel>"
                )
                xml_data_channel_items = xml_data_channel.findall("item")
                if xml_data_channel_items is None:
                    # 没有 item 意味着翻页结束
                    raise req_end
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

                    if (
                        _data := await self.get_data(
                            page_link.removeprefix(self.page_link_head)
                        )
                    ) and (fast_skip or _data.magnet):
                        # fast_skip 模式跳过空种数据库条目，但非 fast_skip 模式重下空种条目
                        log.debug(
                            "{} 跳过下载第 {} 个种子 {} {}",
                            self.__class__.__name__,
                            torrent_num_new,
                            title,
                            page_link,
                            print_level=log.LogLevel._detail,
                        )
                        if not fast_skip:
                            # 跳过下载种子，但刷新数据库条目非种子信息，因为 mikan 的修改只能修改信息不能修改种子
                            yield self.Website_entry(
                                title=title,
                                page_link=page_link,
                                torrent=b"",
                                only_refresh=True,
                            )
                        torrent_num_new += 1
                        continue

                    log.debug(
                        "{} 开始下载 {}",
                        self.__class__.__name__,
                        title,
                    )
                    not_skip_download: bool = True
                    while not_skip_download:
                        try:
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
                                torrent_response.raise_for_status()
                                torrent = await torrent_response.aread()
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code == 404:
                                log.warning(
                                    "{} 404 响应，可能不存在此文件，跳过此文件下载: {}",
                                    self.__class__.__name__,
                                    f"{title} {page_link}",
                                )
                                torrent_num_new += 1
                                not_skip_download = False
                                continue
                            raise
                        except (
                            httpx.NetworkError,
                            httpx.RemoteProtocolError,
                            httpx.ReadTimeout,
                        ) as e:
                            log.warning(
                                "{} 下载失败，重试: {} {!r}",
                                self.__class__.__name__,
                                e,
                                e,
                            )
                            continue

                        log.debug(
                            "{} 响应头: {} {} {}",
                            self.__class__.__name__,
                            torrent_response.http_version,
                            torrent_response.status_code,
                            torrent_response.headers,
                            print_level=log.LogLevel._detail,
                        )

                        break
                    else:
                        break

                    torrent_num_new += 1
                    log.debug(
                        "{} 下载第 {:,} 个 torrent 体积: {} from {} {}",
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

            except ValidationError as e:
                log.error("{} 类型错误: {!r}", self.__class__.__name__, e, deep=True)
                raise

        page_num: int = 1
        """页数"""
        skip_page_num: int = 1
        """跳过的页数"""
        pre_page_num: int = 1
        """上次请求的页数"""
        while True:
            all_skip: bool = True and fast_skip
            try:
                async for website_entry in _req(page_num):
                    yield website_entry
                    all_skip = False
            except self.req_fialed as e:
                log.warning("{} 单次请求失败: {}", self.__class__.__name__, e)
                continue
            except req_end:
                if page_num - pre_page_num == 1:
                    log.info(
                        "{} 本次翻页循环结束，一共 {} 页",
                        self.__class__.__name__,
                        pre_page_num,
                    )
                    break
                all_skip = False

            await asyncio.sleep(sleep_time)
            # 全部跳过则每次多跳，直到没有全跳时，从上次请求的位置开始重置
            _page_num = page_num
            if all_skip:
                skip_page_num *= 2
                page_num += skip_page_num
            else:
                skip_page_num = 1
                page_num = min(page_num, pre_page_num) + 1
            pre_page_num = _page_num

    @property
    def Data_class(self):
        return self.Website_entry_data_mikan
