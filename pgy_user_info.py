import time
import requests
import polars as pl
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("xhs_kol")
cookies = {
    "a1": "19841b7dbc2g4lp8jzlz23cqyhhzikeupa7rpm2lr30000355340",
    "webId": "7bacca47ddd6d7a320bb3130e5c4c7a7",
    "gid": "yjY4yDWddSUDyjY4yDWfD06JSJk41UYVu1uJW1FqSAvxxFq8u3Td7U888q22q488dDidY2Ki",
    "customerClientId": "132884442829052",
    "abRequestId": "7bacca47ddd6d7a320bb3130e5c4c7a7",
    "web_session": "04006976ad20a7f1cddf1eb0a83a4b9d18e288",
    "customer-sso-sid": "68c517550667680375341062jrwinjykep5feekj",
    "solar.beaker.session.id": "AT-68c5175506676846698496006whgtshmwluc059k",
    "access-token-pgy.xiaohongshu.com": "customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k",
    "access-token-pgy.beta.xiaohongshu.com": "customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k",
    "webBuild": "4.81.0",
    "acw_tc": "0a42136b17582931272037341e6b95825e5b627d6a935449c4b31ee6f7db42",
    "xsecappid": "ratlin",
    "websectiga": "7750c37de43b7be9de8ed9ff8ea0e576519e8cd2157322eb972ecb429a7735d4",
    "sec_poison_id": "1ffe4e69-c3cc-4883-873f-5dc0fd197151",
    "loadts": "1758293128903",
}

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en,zh-CN;q=0.9,zh;q=0.8",
    "cache-control": "max-age=0",
    "if-modified-since": "Thu, 18 Sep 2025 09:08:20 GMT",
    "if-none-match": 'W/"4062899099"',
    "priority": "u=0, i",
    "referer": "https://pgy.xiaohongshu.com/",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    # 'cookie': 'a1=19841b7dbc2g4lp8jzlz23cqyhhzikeupa7rpm2lr30000355340; webId=7bacca47ddd6d7a320bb3130e5c4c7a7; gid=yjY4yDWddSUDyjY4yDWfD06JSJk41UYVu1uJW1FqSAvxxFq8u3Td7U888q22q488dDidY2Ki; customerClientId=132884442829052; abRequestId=7bacca47ddd6d7a320bb3130e5c4c7a7; web_session=04006976ad20a7f1cddf1eb0a83a4b9d18e288; customer-sso-sid=68c517550667680375341062jrwinjykep5feekj; solar.beaker.session.id=AT-68c5175506676846698496006whgtshmwluc059k; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k; webBuild=4.81.0; acw_tc=0a42136b17582931272037341e6b95825e5b627d6a935449c4b31ee6f7db42; xsecappid=ratlin; websectiga=7750c37de43b7be9de8ed9ff8ea0e576519e8cd2157322eb972ecb429a7735d4; sec_poison_id=1ffe4e69-c3cc-4883-873f-5dc0fd197151; loadts=1758293128903',
}


def _to_polars_df(rows) -> pl.DataFrame:
    """
    列表→Polars DataFrame，并把列表型字段转为字符串，便于落地Excel。
    也可以根据需要改为 JSON 序列化存储。
    """

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows, strict=False)

    return df


def write_excel_safely(df: pl.DataFrame, path: str) -> None:
    """
    优先用 Polars 写 Excel；失败则自动降级为 Pandas。
    """
    if df.is_empty():
        logger.info("没有数据，不写出文件。")
        return
    try:
        # 需要 polars[excel] 或相应依赖
        df.write_excel(path)
        logger.info("已写出：%s (polars)", path)
    except Exception as e:
        logger.warning("polars 写 Excel 失败，尝试使用 pandas。原因：%s", e)
        try:
            import pandas as pd

            df.to_pandas().to_excel(path, index=False)
            logger.info("已写出：%s (pandas)", path)
        except Exception as e2:
            logger.error("pandas 写 Excel 也失败：%s", e2)

            raise


all_rs = []
all_urls = [
    "https://www.xiaohongshu.com/user/profile/635e9c7e000000001901f583",
    "https://www.xiaohongshu.com/user/profile/60b0ac43000000000101e199",
    "https://www.xiaohongshu.com/user/profile/5e08aa440000000001004ee1",
    "https://www.xiaohongshu.com/user/profile/67c6916c000000000601374e",
    "https://www.xiaohongshu.com/user/profile/6849cf41000000001d00a7f4",
    "https://www.xiaohongshu.com/user/profile/6732c26c000000001c018875",
    "https://www.xiaohongshu.com/user/profile/66a0ccd3000000002401e242",
    "https://www.xiaohongshu.com/user/profile/67eb4cf7000000000e0101bf",
    "https://www.xiaohongshu.com/user/profile/6155ad92000000000201a9ce",
    "https://www.xiaohongshu.com/user/profile/67e0ad8a000000000a03d447",
    "https://www.xiaohongshu.com/user/profile/6660fe4800000000030315d4",
    "https://www.xiaohongshu.com/user/profile/67470357000000001c01a864",
    "https://www.xiaohongshu.com/user/profile/634fc560000000001802f44d",
    "https://www.xiaohongshu.com/user/profile/635125b6000000001802c181",
    "https://www.xiaohongshu.com/user/profile/5d4c02dc000000001602fc72",
    "https://www.xiaohongshu.com/user/profile/558f5a6cb7ba22679da22e34",
    "https://www.xiaohongshu.com/user/profile/67124848000000000d0271e3",
    "https://www.xiaohongshu.com/user/profile/636d59de000000001f014049",
    "https://www.xiaohongshu.com/user/profile/66dfe748000000001d0218f5",
    "https://www.xiaohongshu.com/user/profile/671ca8f5000000001d0239a2",
    "https://www.xiaohongshu.com/user/profile/6069d564000000000100a347",
    "https://www.xiaohongshu.com/user/profile/66249ce60000000007006f11",
    "https://www.xiaohongshu.com/user/profile/63e3678100000000270280dd",
    "https://www.xiaohongshu.com/user/profile/65117b910000000023026ff1",
    "https://www.xiaohongshu.com/user/profile/587bb9fd6a6a694a40af2ded",
    "https://www.xiaohongshu.com/user/profile/65d78474000000000d02482f",
    "https://www.xiaohongshu.com/user/profile/66a1f898000000000d027945",
    "https://www.xiaohongshu.com/user/profile/618dc6b30000000010007f09",
    "https://www.xiaohongshu.com/user/profile/66f4274e000000000b0320fc",
    "https://www.xiaohongshu.com/user/profile/66e0399e000000001e00a670",
    "https://www.xiaohongshu.com/user/profile/631b2d15000000002303e497",
    "https://www.xiaohongshu.com/user/profile/5ee42ade000000000101df2d",
    "https://www.xiaohongshu.com/user/profile/5d172dd7000000001001eb17",
    "https://www.xiaohongshu.com/user/profile/614b718b000000001f03cfeb",
    "https://www.xiaohongshu.com/user/profile/60ffff9500000000010081b7",
    "https://www.xiaohongshu.com/user/profile/6017eca50000000001005e0c",
    "https://www.xiaohongshu.com/user/profile/5c1a40840000000005006b0c",
]
for url in all_urls:
    user_id = url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    response = requests.get(
        "https://pgy.xiaohongshu.com/api/solar/cooperator/user/blogger/" + user_id,
        cookies=cookies,
        headers=headers,
    )
    res = response.json()
    it = res["data"]
    user_id = it.get("userId", "")
    all_rs.append(
        {
            "pgy_home_url": f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{user_id}"
            if user_id
            else "",
            "xsh_home_url": f"https://www.xiaohongshu.com/user/profile/{user_id}"
            if user_id
            else "",
            "userId": user_id,
            "name": it.get("name", ""),
            "redId": it.get("redId", ""),
            "location": it.get("location", ""),
            "personalTags": it.get("personalTags", []),
            "picturePrice": it.get("picturePrice", None),
            "videoPrice": it.get("videoPrice", None),
            "businessNoteCount": it.get("businessNoteCount", None),
            "contentTags": it.get("contentTags", []),
            "featureTags": it.get("featureTags", []),
            "gender": it.get("gender", None),
            "tradeType": it.get("tradeType", ""),
            "fansNum": it.get("fansNum", None),
            "clickMidNum": it.get("clickMidNum", None),
            "videoClickMidNum": it.get("videoClickMidNum", None),
        }
    )
    time.sleep(1)
print(all_rs)
df = _to_polars_df(all_rs)
write_excel_safely(df, "xhs_kol.xlsx")
