import asyncio
import datetime
import re
from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn, override

import httpx
import sqlmodel
import zstandard
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import (
    Column,
    LargeBinary,
    SQLModel,
    String,
    TypeDecorator,
    and_,
    create_engine,
    delete,
    func,
    select,
)
from sqlmodel.ext.asyncio.session import AsyncSession

from ..log import log
from ..utils import get_size_str

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Sequence

    from oidbt_bangumi_ani_getter import Bangumi_ani_getter
    from sqlalchemy import Dialect


class IdList(TypeDecorator):
    impl = String
    cache_ok = True

    @override
    def process_bind_param(
        self,
        value: list[int] | None,
        dialect: Dialect,
    ) -> str | None:
        if not value:
            return None
        return ",".join(map(str, value))

    @override
    def process_result_value(
        self,
        value: str | None,
        dialect: Dialect,
    ) -> list[int] | None:
        if not value:
            return None
        return list(map(int, value.split(",")))


class CompressedBinary(TypeDecorator):
    impl = LargeBinary
    cache_ok = True

    ZSTD_LEVEL: ClassVar = 22

    @override
    def process_bind_param(
        self,
        value: bytes | str | None,
        dialect: Dialect,
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
            print_level=log.LogLevel._detail,
        )
        return comp

    @override
    def process_result_value(
        self,
        value: bytes | None,
        dialect: Dialect,
    ) -> bytes | None:
        if not value:
            return None
        return zstandard.decompress(value)


class LockMeta(ABCMeta):
    """为每个子类自动添加类级别的 lock"""

    def __new__(mcls, name, bases, namespace, /, **kwargs: Any) -> LockMeta:
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        cls.REQ_LOCK = asyncio.Lock()  # pyright: ignore[reportAttributeAccessIssue]
        return cls


class Base_bt_entry_getter(ABC, metaclass=LockMeta):
    DATABASE_LOCK: ClassVar = asyncio.Lock()
    UPDATE_DEL_THRESHOLD: ClassVar = datetime.timedelta(days=30)
    REQ_LOCK: ClassVar[asyncio.Lock]
    """每个子类一个 lock"""

    def __init__(
        self,
        *,
        database_filename: str,
        proxy: httpx._types.ProxyTypes | None = None,
        timeout: httpx._types.TimeoutTypes = 10,
        cookies: dict[str, str] | None = None,
        email: str | None = None,
    ) -> None:
        assert isinstance(self.REQ_LOCK, asyncio.Lock)

        self.client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,  # 允许重定向
            proxy=proxy,
            timeout=timeout,
            headers={k: v for k, v in {"From": email}.items() if v is not None},
        )
        """HTTP Client"""
        self.cookies = cookies

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
        try:
            asyncio.get_running_loop()
            asyncio.create_task(self.client.aclose())  # noqa: RUF006
        except RuntimeError:
            asyncio.run(self.client.aclose())

    class req_fialed(Exception):
        pass

    async def req(self, url: str, /) -> httpx.Response:
        req_num: int = 1

        async def _req() -> httpx.Response:
            try:
                async with self.REQ_LOCK:
                    response = await self.client.get(
                        url, cookies=self.cookies if req_num > 2 else None
                    )
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

            except httpx.HTTPStatusError as e:
                log.error(
                    "{} 状态码错误: {} {}",
                    self.__class__.__name__,
                    e.response.status_code,
                    e.request.url,
                )
                raise self.req_fialed from e
            except httpx.ConnectError as e:
                log.error(
                    "{} 连接失败: {!r} {}", self.__class__.__name__, e, e.request.url
                )
                raise self.req_fialed from e
            except httpx.RemoteProtocolError as e:
                # 大概率代理异常导致的
                log.warning(
                    "{} 服务器违反协议: {!r} {}",
                    self.__class__.__name__,
                    e,
                    e.request.url,
                )
                raise self.req_fialed from e
            except httpx.TimeoutException as e:
                log.warning("{} 请求超时 {}", self.__class__.__name__, e.request.url)
                raise self.req_fialed from e
            except httpx.NetworkError as e:
                log.error(
                    "{} 未知网络错误: {} {!r} {}",
                    self.__class__.__name__,
                    e,
                    e,
                    e.request.url,
                    deep=True,
                )
                raise self.req_fialed from e

            else:
                return response

        while True:
            try:
                return await _req()
            except self.req_fialed:
                log.warning(
                    "{} {} 重试 {} 次",
                    self.__class__.__name__,
                    self.req.__name__,
                    req_num,
                )
                if req_num > 9:
                    log.error(
                        "{} {} 重试次数过高，放弃重试",
                        self.__class__.__name__,
                        self.req.__name__,
                    )
                    raise
                req_num += 1
                continue

    async def match_ani_from_text(self, html_text: str, /) -> list[int]:
        """
        直接从网页文本匹配 Bangumi ID

        :return: Bangumi ID list
        """
        return list(
            map(
                int,
                re.findall(
                    r"(?:bgm\.tv|bangumi\.tv|chii\.in)/subject/(\d+)", html_text
                ),
            )
        )

    @abstractmethod
    async def match_ani_special(self, html_text: str, /) -> list[int]:
        """
        特殊匹配规则，例如部分网站有记录 Bangumi ID

        :return: Bangumi ID list
        """
        ...

    async def match_ani(
        self,
        *,
        website_entry: Website_entry,
        bgm_ani_datas: Sequence[Bangumi_ani_getter.Bangumi_ani_data],
    ) -> list[int]:
        """
        根据 BT 发布页标题匹配 Bangumi ID

        匹配度优先级: 特殊匹配 > 原名 > 中文名 > 别名
        同级内优先级: 匹配文本长度

        :return: Bangumi ID, 按匹配度排序
        """

        @dataclass(slots=True, kw_only=True)
        class Match_item:
            id: int
            match_len: int
            rank: int

            def __hash__(self) -> int:
                return hash((self.id, self.match_len, self.rank))

        def sorted_match_list(match_list: Iterable[Match_item]) -> list[Match_item]:
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
            return "".join(
                name.translate(strip_name_trans_table).strip().split()
            ).lower()

        title: str = website_entry.title
        new_title = strip_name(title)

        match_list_name: set[Match_item] = set()
        for data in bgm_ani_datas:

            def _add_match(
                name: str, data: Bangumi_ani_getter.Bangumi_ani_data
            ) -> None:
                if name:
                    if name in title:
                        match_list_name.add(
                            Match_item(id=data.id, match_len=len(name), rank=data.rank)
                        )
                    elif (new_name := strip_name(name)) in new_title:
                        match_list_name.add(
                            Match_item(
                                id=data.id, match_len=len(new_name), rank=data.rank
                            )
                        )

            _add_match(data.name, data)
            _add_match(data.name_cn, data)

            for n in data.name_alias:
                _add_match(n, data)

        # 特殊匹配
        _url = website_entry.page_link
        response = None
        while True:
            try:
                response = await self.req(_url)
            except self.req_fialed:
                log.error(
                    "{} 请求失败 {}",
                    self.__class__.__name__,
                    _url,
                )
                continue
            break

        if response is None:
            match_ani_special_ids = []
            match_ani_from_text_ids = []
        else:
            match_ani_special_ids = await self.match_ani_special(response.text)
            match_ani_from_text_ids = await self.match_ani_from_text(response.text)

        match_ani_from_name = sorted_match_list(match_list_name)
        match_ani_from_name_ids = [
            it.id
            for it in (
                [
                    it
                    for it in match_ani_from_name
                    if (it.rank != 0 and it.match_len >= 3)
                ]
                or match_ani_from_name[:1]
            )
        ]  # 去除尾部过短和冷门匹配项

        log.debug(
            "{} match_ani_special: {} {} {}",
            self.__class__.__name__,
            match_ani_special_ids,
            match_ani_from_text_ids,
            match_ani_from_name_ids,
        )

        return list(
            dict.fromkeys(
                match_ani_special_ids
                + match_ani_from_text_ids
                + match_ani_from_name_ids
            )
        )

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
        bgm_ani_all_data_getter: Callable[
            ..., Awaitable[Sequence[Bangumi_ani_getter.Bangumi_ani_data]]
        ],
    ) -> NoReturn:
        """自动循环爬取"""
        cycle_num: int = 1
        sleep_time: Literal[0, 1] = 0
        while True:
            log.info("{} 进入第 {} 次循环", self.__class__.__name__, cycle_num)

            # 提前删除旧数据，自动跳过算法会重新下载刚才删除的旧数据
            await self._del_data_unrefreshed()

            bgm_ani_all_data = await bgm_ani_all_data_getter()
            async for website_entry in self.get_website_entry(sleep_time=sleep_time):
                id_list: list[int] = await self.match_ani(
                    website_entry=website_entry, bgm_ani_datas=bgm_ani_all_data
                )
                await self.save_data(
                    self.website_entry_to_data(
                        website_entry=website_entry, id_list=id_list
                    )
                )

            cycle_num += 1
            sleep_time = 1
            bgm_ani_all_data = await bgm_ani_all_data_getter()

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
        """
        SQL 表基类

        getter 子类必须继承一个不同命名的子表类
        """

        title: str
        page_link_point: str = sqlmodel.Field(
            description="条目网页链接的信息部分，可与头部拼接为完整链接",
            primary_key=True,
        )
        magnet: bytes | None = sqlmodel.Field(
            description="手动从种子文件解析的磁链", sa_column=Column(CompressedBinary)
        )
        match_id_list: list[int] | None = sqlmodel.Field(
            description="匹配的 Bangumi ID 列表", sa_column=Column(IdList)
        )

        update_time: datetime.datetime = sqlmodel.Field(
            description="刷新时间", default_factory=datetime.datetime.now
        )

    @property
    @abstractmethod
    def Data_class(self) -> type[Website_entry_data]:
        """返回当前类对应的 SQL 表类"""
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

    async def get_all_data(self) -> Sequence[Website_entry_data]:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            return (await session.exec(select(self.Data_class))).all()

    async def get_all_data_len(self) -> int:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            statement = select(func.count()).select_from(self.Data_class)
            return (await session.exec(statement)).one()

    async def get_data(self, primary_k_v: str | None, /) -> Website_entry_data | None:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            return await session.get(self.Data_class, primary_k_v)

    async def _del_data_unrefreshed(self) -> None:
        """删除长时间未更新的数据"""
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            stmt = delete(self.Data_class).where(
                and_(
                    datetime.datetime.now() - self.Data_class.update_time
                    > self.UPDATE_DEL_THRESHOLD
                )
            )
            await session.exec(stmt)
            await session.commit()
