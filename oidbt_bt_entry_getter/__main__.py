import asyncio


async def run_example():
    import urllib.request

    from oidbt_bangumi_ani_getter import Bangumi_ani_getter

    from .getter import Mikan_bt_entry_getter
    from .log import log

    log.print_level = log.LogLevel.debug

    proxies: dict[str, str] = urllib.request.getproxies()
    proxy_url = proxies.get("http")
    if proxy_url is not None:
        proxy_url = proxy_url.split("//", maxsplit=1)[-1].strip()
        proxy_url = f"socks5://{proxy_url}"

    DATABASE_FILENAME = "OIDBT_SQLite"

    bangumi_ani_getter = Bangumi_ani_getter(
        database_filename=DATABASE_FILENAME,
        proxy=proxy_url,
    )

    mikan_bt_entry_getter = Mikan_bt_entry_getter(
        database_filename=DATABASE_FILENAME, proxy=proxy_url
    )
    await asyncio.gather(
        bangumi_ani_getter.auto_req(),
        mikan_bt_entry_getter.auto_req(
            bgm_ani_all_data_getter=bangumi_ani_getter.get_all_data
        ),
    )


if __name__ == "__main__":
    asyncio.run(run_example())
