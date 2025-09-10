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
    "x-user-id-pgy.xiaohongshu.com": "62c2868c000000001902b9ac",
    "web_session": "04006976ad20a7f1cddf1eb0a83a4b9d18e288",
    "customer-sso-sid": "68c517548079125061025793cwak1ofypi3olbg6",
    "solar.beaker.session.id": "AT-68c517548079125060829187ymrspiiv3zxdhpv2",
    "access-token-pgy.xiaohongshu.com": "customer.pgy.AT-68c517548079125060829187ymrspiiv3zxdhpv2",
    "access-token-pgy.beta.xiaohongshu.com": "customer.pgy.AT-68c517548079125060829187ymrspiiv3zxdhpv2",
    "xsecappid": "ratlin",
    "acw_tc": "0a42442417575089451803746e25f8d2a6afbce8a5622c1c7c3cee37e524d1",
    "loadts": "1757508944583",
    "websectiga": "cf46039d1971c7b9a650d87269f31ac8fe3bf71d61ebf9d9a0a87efb414b816c",
    "sec_poison_id": "11828f4f-66f7-4a1b-8c98-b7a36fc8c1d4",
}

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en,zh-CN;q=0.9,zh;q=0.8",
    "cache-control": "max-age=0",
    "if-modified-since": "Wed, 10 Sep 2025 11:18:00 GMT",
    "if-none-match": 'W/"478135275"',
    "priority": "u=0, i",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    # 'cookie': 'a1=19841b7dbc2g4lp8jzlz23cqyhhzikeupa7rpm2lr30000355340; webId=7bacca47ddd6d7a320bb3130e5c4c7a7; gid=yjY4yDWddSUDyjY4yDWfD06JSJk41UYVu1uJW1FqSAvxxFq8u3Td7U888q22q488dDidY2Ki; customerClientId=132884442829052; abRequestId=7bacca47ddd6d7a320bb3130e5c4c7a7; x-user-id-pgy.xiaohongshu.com=62c2868c000000001902b9ac; web_session=04006976ad20a7f1cddf1eb0a83a4b9d18e288; customer-sso-sid=68c517548079125061025793cwak1ofypi3olbg6; solar.beaker.session.id=AT-68c517548079125060829187ymrspiiv3zxdhpv2; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517548079125060829187ymrspiiv3zxdhpv2; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517548079125060829187ymrspiiv3zxdhpv2; xsecappid=ratlin; acw_tc=0a42442417575089451803746e25f8d2a6afbce8a5622c1c7c3cee37e524d1; loadts=1757508944583; websectiga=cf46039d1971c7b9a650d87269f31ac8fe3bf71d61ebf9d9a0a87efb414b816c; sec_poison_id=11828f4f-66f7-4a1b-8c98-b7a36fc8c1d4',
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
    "https://www.xiaohongshu.com/user/profile/5d39d9020000000016033a5b",
    "https://www.xiaohongshu.com/user/profile/61bc650100000000100083fc",
    "https://www.xiaohongshu.com/user/profile/5afce1d24eacab4a8b6f8b2f",
    "https://www.xiaohongshu.com/user/profile/65edafd3000000000500e845",
    "https://www.xiaohongshu.com/user/profile/647efcd9000000001100250a",
    "https://www.xiaohongshu.com/user/profile/64cf6ab4000000000e027b21",
    "https://www.xiaohongshu.com/user/profile/624909e9000000001000f93d",
    "https://www.xiaohongshu.com/user/profile/616a2c94000000001f03e105",
    "https://www.xiaohongshu.com/user/profile/60aa2ef300000000010078d7",
    "https://www.xiaohongshu.com/user/profile/60044eb6000000000100a8ec",
    "https://www.xiaohongshu.com/user/profile/5f2f58140000000001009d94",
    "https://www.xiaohongshu.com/user/profile/5c62cbe5000000001001afe6",
    "https://www.xiaohongshu.com/user/profile/648a5bf8000000001c02a584",
    "https://www.xiaohongshu.com/user/profile/63f5cf44000000001001eda2",
    "https://www.xiaohongshu.com/user/profile/66f389e9000000001d033f84",
    "https://www.xiaohongshu.com/user/profile/5a6d45634eacab0db37aa6bd",
    "https://www.xiaohongshu.com/user/profile/5f8662d10000000001005b88",
    "https://www.xiaohongshu.com/user/profile/6396d2d50000000026010c55",
    "https://www.xiaohongshu.com/user/profile/5817412e5e87e75f63ec8dc8",
    "https://www.xiaohongshu.com/user/profile/5f485cbf000000000100515d",
    "https://www.xiaohongshu.com/user/profile/5ddcd9aa000000000100844e",
    "https://www.xiaohongshu.com/user/profile/648a5bf8000000001c02a584",
    "https://www.xiaohongshu.com/user/profile/5d1f0ab3000000001100f8b2",
    "https://www.xiaohongshu.com/user/profile/5dfaeb4d000000000100a904",
    "https://www.xiaohongshu.com/user/profile/58ece4d350c4b41389cd88e8",
    "https://www.xiaohongshu.com/user/profile/609e2409000000000101c2cd",
    "https://www.xiaohongshu.com/user/profile/5a6adf3511be10641a11d733",
    "https://www.xiaohongshu.com/user/profile/5cde2975000000001002567b",
    "https://www.xiaohongshu.com/user/profile/5e177730000000000100898e",
    "https://www.xiaohongshu.com/user/profile/664d81b400000000070070ce",
    "https://www.xiaohongshu.com/user/profile/66e932f0000000001d021e07",
    "https://www.xiaohongshu.com/user/profile/5e3e19410000000001004ce3",
    "https://www.xiaohongshu.com/user/profile/58ae7e5d5e87e76e71e72930",
    "https://www.xiaohongshu.com/user/profile/5dde0eea0000000001005750",
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
