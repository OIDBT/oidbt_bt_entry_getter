import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, ClassVar, Literal, NoReturn

import httpx
import zstandard
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import (
    JSON,
    Column,
    Field,
    LargeBinary,
    SQLModel,
    TypeDecorator,
    and_,
    create_engine,
    delete,
    update,
)
from sqlmodel.ext.asyncio.session import AsyncSession

from ..log import log
from ..utils import get_size_str

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence

    from oidbt_bangumi_ani_getter import Bangumi_ani_getter
    from sqlalchemy import Dialect


class CompressedBinary(TypeDecorator):
    impl = LargeBinary

    ZSTD_LEVEL: ClassVar = 22

    def process_bind_param(
        self,
        value: bytes | str | None,
        dialect: Dialect,  # noqa: ARG002
    ) -> bytes | None:
        if not value:
            return None
        if isinstance(value, str):
            value = value.encode()
        comp = zstandard.compress(value, level=self.ZSTD_LEVEL)
        log.debug(
            "{} {} level 压缩体积变化 {} -> {}",
            self.__class__.__name__,
            self.ZSTD_LEVEL,
            get_size_str(value),
            get_size_str(comp),
        )
        return comp

    def process_result_value(
        self,
        value: bytes | None,
        dialect: Dialect,  # noqa: ARG002
    ) -> bytes | None:
        if not value:
            return None
        return zstandard.decompress(value)


class Base_bt_entry_getter(ABC):
    DATABASE_LOCK: ClassVar = asyncio.Lock()
    UNREFRESHED_THRESHOLD: ClassVar = 20

    def __init__(
        self,
        *,
        database_filename: str,
        proxy: httpx._types.ProxyTypes | None = None,
        timeout: httpx._types.TimeoutTypes = 10,
    ) -> None:
        self.client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,  # 允许重定向
            proxy=proxy,
            timeout=timeout,
        )
        """HTTP Client"""
        if not database_filename.endswith(".db"):
            database_filename += ".db"
        self.sync_engine = create_engine(f"sqlite:///{database_filename}")
        """同步 database engine"""
        self.async_engine = create_async_engine(
            f"sqlite+aiosqlite:///{database_filename}"
        )
        """异步 database engine"""
        self.__class__.Website_entry_data.metadata.create_all(self.sync_engine)

    def __del__(self) -> None:
        asyncio.run(self.client.aclose())

    def match_ani(
        self,
        *,
        website_entry: Website_entry,
        bgm_ani_datas: Sequence[Bangumi_ani_getter.Bangumi_ani_data],
    ) -> list[int]:
        """
        根据 BT 发布页标题匹配 Bangumi ID

        匹配度优先级: 原名 > 中文名 > 别名
        同级内优先级: 匹配文本长度

        :return: Bangumi ID, 按匹配度排序
        """

        @dataclass(slots=True, kw_only=True)
        class Match_item:
            id: int
            match_len: int

        def sorted_match_list(match_list: Sequence[Match_item]) -> list[Match_item]:
            return sorted(match_list, key=lambda it: it.match_len, reverse=True)

        strip_name_trans_table = str.maketrans(
            {
                " ": "",
                # 全角标点 → 半角标点
                "，": ",",
                "。": ".",
                "？": "?",
                "！": "!",
                "：": ":",
                "；": ";",
                "、": ",",
                "「": '"',
                "」": '"',
                "『": '"',
                "』": '"',
                "【": "[",
                "】": "]",
                "（": "(",
                "）": ")",
                "《": "<",
                "》": ">",
                "〈": "<",
                "〉": ">",
                "｛": "{",
                "｝": "}",
                "［": "[",
                "］": "]",
                "〔": "(",
                "〕": ")",
                "〖": "[",
                "〗": "]",
                "〘": "(",
                "〙": ")",
                "〚": "[",
                "〛": "]",
                "〜": "~",
                "〝": '"',
                "〞": '"',
                "〟": '"',
                "–": "-",
                "—": "-",
                "‥": "..",
                "…": "...",
                "‧": ".",
                "﹏": "_",
                "＂": '"',
                "＇": "'",
                "＄": "$",
                "％": "%",
                "＆": "&",
                "＠": "@",
                "＃": "#",
                "＊": "*",
                "＋": "+",
                "－": "-",
                "＝": "=",
                "／": "/",
                "＼": "\\",
                "｜": "|",
                "＾": "^",
                "＿": "_",
                "｀": "`",
                "～": "~",
                # 全角空格 → 半角空格
                "　": " ",
                # 全角数字 0-9 → 半角数字
                "０": "0",
                "１": "1",
                "２": "2",
                "３": "3",
                "４": "4",
                "５": "5",
                "６": "6",
                "７": "7",
                "８": "8",
                "９": "9",
                # 全角字母 A-Z → 半角字母
                "Ａ": "A",
                "Ｂ": "B",
                "Ｃ": "C",
                "Ｄ": "D",
                "Ｅ": "E",
                "Ｆ": "F",
                "Ｇ": "G",
                "Ｈ": "H",
                "Ｉ": "I",
                "Ｊ": "J",
                "Ｋ": "K",
                "Ｌ": "L",
                "Ｍ": "M",
                "Ｎ": "N",
                "Ｏ": "O",
                "Ｐ": "P",
                "Ｑ": "Q",
                "Ｒ": "R",
                "Ｓ": "S",
                "Ｔ": "T",
                "Ｕ": "U",
                "Ｖ": "V",
                "Ｗ": "W",
                "Ｘ": "X",
                "Ｙ": "Y",
                "Ｚ": "Z",
                # 全角字母 a-z → 半角字母
                "ａ": "a",
                "ｂ": "b",
                "ｃ": "c",
                "ｄ": "d",
                "ｅ": "e",
                "ｆ": "f",
                "ｇ": "g",
                "ｈ": "h",
                "ｉ": "i",
                "ｊ": "j",
                "ｋ": "k",
                "ｌ": "l",
                "ｍ": "m",
                "ｎ": "n",
                "ｏ": "o",
                "ｐ": "p",
                "ｑ": "q",
                "ｒ": "r",
                "ｓ": "s",
                "ｔ": "t",
                "ｕ": "u",
                "ｖ": "v",
                "ｗ": "w",
                "ｘ": "x",
                "ｙ": "y",
                "ｚ": "z",
            }
        )

        @cache
        def strip_name(name: str) -> str:
            return "".join(name.translate(strip_name_trans_table).strip().split())

        title: str = website_entry.title

        match_list_name: list[Match_item] = []
        match_list_name_cn: list[Match_item] = []
        match_list_name_alias: list[Match_item] = []
        for data in bgm_ani_datas:
            new_title = strip_name(title)

            if data.name:
                if data.name in title:
                    match_list_name.append(
                        Match_item(id=data.id, match_len=len(data.name))
                    )
                elif (new_name := strip_name(data.name)) in new_title:
                    match_list_name.append(
                        Match_item(id=data.id, match_len=len(new_name))
                    )

            if data.name_cn:
                if data.name_cn in title:
                    match_list_name_cn.append(
                        Match_item(id=data.id, match_len=len(data.name_cn))
                    )
                elif (new_name := strip_name(data.name)) in new_title:
                    match_list_name_cn.append(
                        Match_item(id=data.id, match_len=len(new_name))
                    )

            for n in data.name_alias:
                if n:
                    if n in title:
                        match_list_name_alias.append(
                            Match_item(id=data.id, match_len=len(n))
                        )
                    elif (new_name := strip_name(n)) in new_title:
                        match_list_name_alias.append(
                            Match_item(id=data.id, match_len=len(new_name))
                        )

        return [
            it.id
            for it in (
                *sorted_match_list(match_list_name),
                *sorted_match_list(match_list_name_cn),
                *sorted_match_list(match_list_name_alias),
            )
        ]

    @abstractmethod
    def website_entry_to_data(
        self,
        *,
        website_entry: Website_entry,
        id_list: list[int],
    ) -> Website_entry_data:
        """条目处理为SQL条目"""
        ...

    async def auto_req(
        self,
        *,
        bgm_ani_datas_getter: Callable[
            ..., Awaitable[Sequence[Bangumi_ani_getter.Bangumi_ani_data]]
        ],
    ) -> NoReturn:
        """自动循环爬取"""
        cycle_num: int = 1
        sleep_time: Literal[1, 10] = 1
        bgm_ani_datas = await bgm_ani_datas_getter()
        while True:
            log.info("{} 进入第 {} 次循环", self.__class__.__name__, cycle_num)

            await self._del_data_unrefreshed()

            async for website_entry in self.get_website_entry(
                sleep_time=sleep_time,
                fast_skip=cycle_num == 1,
            ):
                id_list: list[int] = self.match_ani(
                    website_entry=website_entry, bgm_ani_datas=bgm_ani_datas
                )
                await self.save_data(
                    self.website_entry_to_data(
                        website_entry=website_entry, id_list=id_list
                    )
                )

            await self._add_data_unrefreshed_count()

            cycle_num += 1
            sleep_time = 10
            bgm_ani_datas = await bgm_ani_datas_getter()

    class Website_entry(BaseModel):
        title: str
        """BT 发布页的标题"""
        page_link: str
        """条目网页链接"""
        torrent: bytes
        """种子文件数据"""

    @abstractmethod
    def get_website_entry(
        self,
        *,
        sleep_time: int,
        fast_skip: bool,
    ) -> AsyncGenerator[Website_entry]:
        """
        从头到尾循环一遍网站的条目

        :param sleep_time: sec
        :param fast_skip: 数据库内已有的种子不下载
        """
        ...

    @property
    @abstractmethod
    def page_link_head(self) -> str: ...

    class Website_entry_data(SQLModel):
        page_link_point: str = Field(
            description="条目网页链接的信息部分，可与头部拼接为完整链接",
            primary_key=True,
        )
        magnet: bytes | None = Field(
            description="手动从种子文件解析的磁链", sa_column=Column(CompressedBinary)
        )
        match_id_list: list[int] = Field(
            description="匹配的 Bangumi ID 列表", sa_column=Column(JSON)
        )

        unrefreshed_count: int = Field(
            description="数据未刷新次数，长时间未更新则判定为已删除条目，将删除该条数据",
            default=0,
        )

    @property
    @abstractmethod
    def Data_class(self) -> type[Website_entry_data]:
        return self.Website_entry_data

    async def save_data(
        self,
        *datas: Website_entry_data,
        refresh: bool = True,
    ) -> None:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            for data in datas:
                if refresh:
                    stmt = delete(self.Data_class).where(
                        and_(self.Data_class.page_link_point == data.page_link_point)
                    )
                    await session.exec(stmt)
                    session.add(data)
                else:
                    await session.merge(data)
            await session.commit()

    async def get_data(self, primary_k_v: str, /) -> Website_entry_data | None:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            return await session.get(self.Data_class, primary_k_v)

    async def _add_data_unrefreshed_count(self) -> None:
        """将所有未刷新次数 +1"""
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            stmt = update(self.Data_class).values(
                unrefreshed_count=self.Data_class.unrefreshed_count + 1
            )
            await session.exec(stmt)
            await session.commit()

    async def _del_data_unrefreshed(self) -> None:
        """删除长时间未更新的数据"""
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            stmt = delete(self.Data_class).where(
                and_(self.Data_class.unrefreshed_count > self.UNREFRESHED_THRESHOLD)
            )
            await session.exec(stmt)
            await session.commit()
