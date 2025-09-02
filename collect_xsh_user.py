from __future__ import annotations

import math
import time
import json
import logging

from typing import Dict, Any, Iterable, List, Generator, Optional

import requests
import polars as pl
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

BASE_URL = "https://pgy.xiaohongshu.com/api/solar/cooperator/blogger/v2"

# ========== 可调整参数 ==========
REQUEST_TIMEOUT = 15  # 单次请求超时秒数
RETRY_TOTAL = 5  # 失败重试次数
RETRY_BACKOFF = 0.5  # 重试退避系数
RETRY_STATUS = (429, 500, 502, 503, 504)
SLEEP_BETWEEN_PAGES = 0.15  # 翻页间隔，适度放慢防触发风控
MAX_PAGES: Optional[int] = None  # 为None表示不限制；也可以设一个上限避免误拉太多页
# =================================

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("xhs_kol")

cookies = {
    "abRequestId": "62ddc872-6aa4-52be-a801-9bdc7df8569b",
    "a1": "19768a94168phvedmkahlx6lxrfyua04srms8cggo50000220309",
    "webId": "86ef5f0e778ead5fe706f55202a2faf5",
    "gid": "yjWKY0j2JiM0yjWKY0j4yFSkKYUxhdf6T0x16klCK1CFiM28v7084Y888JJ8q8j8Djfdj2f0",
    "x-user-id-creator.xiaohongshu.com": "618f62ac000000001000d08b",
    "customerClientId": "135290779466254",
    "x-user-id-ark.xiaohongshu.com": "618f62ac000000001000d08b",
    "x-user-id-zhaoshang.xiaohongshu.com": "6676ec440000000007006f9e",
    "web_session": "040069b0fb21e41280dd2c959e3a4b017bcfdc",
    "access-token-creator.xiaohongshu.com": "customer.creator.AT-68c517543825570502821193lf0unesttqc6sbxj",
    "galaxy_creator_session_id": "1geKESv2R9DrRvEjUp1niTj3u7FFbqzaRtZx",
    "galaxy.creator.beaker.session.id": "1756433762993022025608",
    "x-user-id-pro.xiaohongshu.com": "618f62ac000000001000d08b",
    "access-token-pro.xiaohongshu.com": "customer.ares.AT-68c5175450013987413524516ixhjz1ix4m9y4uh",
    "access-token-pro.beta.xiaohongshu.com": "customer.ares.AT-68c5175450013987413524516ixhjz1ix4m9y4uh",
    "customer-sso-sid": "68c517545044374847339118c8usbiz3of7awbgt",
    "x-user-id-pgy.xiaohongshu.com": "59ebefa3e8ac2b2171a39d89",
    "solar.beaker.session.id": "AT-68c517545044374183919626b53iwa8xojodpxfy",
    "access-token-pgy.xiaohongshu.com": "customer.pgy.AT-68c517545044374183919626b53iwa8xojodpxfy",
    "access-token-pgy.beta.xiaohongshu.com": "customer.pgy.AT-68c517545044374183919626b53iwa8xojodpxfy",
    "webBuild": "4.79.0",
    "unread": "{%22ub%22:%2268ac76e2000000001b03e91f%22%2C%22ue%22:%2268b671b4000000001c00b665%22%2C%22uc%22:28}",
    "xsecappid": "xhs-pc-web",
    "loadts": "1756799285693",
    "acw_tc": "0a0d068317568002480328382e8c107f28bfe7ce02f3c9d36cf072df3a57c9",
    "websectiga": "10f9a40ba454a07755a08f27ef8194c53637eba4551cf9751c009d9afb564467",
    "sec_poison_id": "5f1b9186-15e6-4212-a121-05f007a32baa",
}

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en,en-US;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "authorization": "",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://pgy.xiaohongshu.com",
    "priority": "u=1, i",
    "referer": "https://pgy.xiaohongshu.com/solar/pre-trade/note/kol",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "x-b3-traceid": "c7be09f55bd99a06",
    "x-s": "1l9LO6wUOB4JOiMlZ6qJs2MpOBFpZBFl1BAKZBvL1lc3",
    "x-s-common": "2UQAPsHC+aIjqArjwjHjNsQhPsHCH0rjNsQhPaHCH0c1PahFHjIj2eHjwjQgynEDJ74AHjIj2ePjwjQUGgzVynhTq9YSJBIjNsQh+sHCH0Z1PshEw/rjNsQh+aHCH0rE+AGhG/DFP/GhqBY98nzTy9biJoW9JoYU8dSMG/ZFq7QTqAY08946+/ZIPeZUP0ZAPeDjNsQh+jHCHjHVHdW7H0ijHjIj2eWjwjQQPAYUaBzdq9k6qB4Q4fpA8b878FSet9RQzLlTcSiM8/+n4MYP8F8LagY/P9Ql4FpUzfpS2BcI8nT1GFbC/L88JdbFyrSiafp/JDMra7pFLDDAa7+8J7QgabmFz7Qjp0mcwp4fanD68p40+fp8qgzELLbILrDA+9p3JpH9LLI3+LSk+d+DJfpSL98lnLYl49IUqgcMc0mrcDShtMmozBD6qM8FyFSh8o+h4g4U+obFyLSi4nbQz/+SPFlnPrDApSzQcA4SPopFJeQmzBMA/o8Szb+NqM+c4ApQzg8Ayp8FaDRl4AYs4g4fLomD8pzBpFRQ2ezLanSM+Skc47Qc4gcMag8VGLlj87PAqgzhagYSqAbn4FYQy7pTanTB2npx87+8NM4L89L78p+l4BL6ze4AzB+IygmS8Bp8qDzFaLP98Lzn4AQQzLEAL7bFJBEVL7pwyS8Fag868nTl4e+0n04ApfuF8FSbL7SQyrplaBzopLShJpmO2fM6anS0nBpc4F8Q4fSePDHAqFzC+7+hpdzDagG98nc7+9p8ydL3anDM8/8gGDzwqg4xanYtqA+68gP9zo8SpbmF/f+p+fpr4gqMag88qoi6J9pD4g4eqeSmq98l4FQQPAc6agYTJo+l4o+YLo4Eq7+HGSkm4fLAqsRSzbm72rSe8g+3zemSL9pHyLSk+7+xGfRAP94UzDSk8BL94gqAanSUy7zM4BMF4gzBagYS8pzc4r8QyrkSyp8FJrS389LILoz/t7b7Lokc4MpQ4fY3agY0q0zdarr3aLESypmFyDSiqdzQyBRAydbFLrlILnb7qDTA8B808rSi2juU4g4yqdp7LFSe8o+3Loz/tFMN8/b0cg+LzomEanYOqA+s+7PAqgz1NM87JFS9J9p/Lo4panSN8nTAqDlQPA4SzeSN8p4n4bQQPA4ApSm7PAQDprYQzpkSag8tqAbU+nphw/+SyDMdq9zn4BpQyL8gagYk/nMl4B4Q4dkeanYdqA8Bze+opg8AydbFGLSb//QlpdqML/SD8p+TJ7+kqgzjGM8FGLDA/epQyrpCndpFJ74n4FYQPFRAyfMD8/bn47SdqgzjaLpVGnpn4b4QPFzNGf+BGFS3+7+f8e+APeSd8/8n4Mr3LozdanTkcDSh+d+npdc9anYw8nkrJgbQ4Dpoqf+VGLS9/n4QyrkSySm7tF4l4e86pdzFanTULrS9P7+8JnzS8bm7Lr4dLrYQybmDNM8FLSbn4BzQygbSanS32rDALnlQzLRSpDlwqAbrwr+7pd4UanYncnbl4MbUpd4kaLpd8gYD4LSQyFIM89hA8nzn4opsLozQcDS0LnFEq04QyMQm/bm7arS38oP9pgkEanD3/9Qc47pQy7kELgbFPnRn4bmQzgQmagGFcFSeLaTALo4gagYcGLSb+gP9pd4LqMk+zLSbqDRQ4DTAPeSSqAbc4FbQ4SDlag8r+nb8LemQ4fF9/BMHprS9J7+x/LRSL7p72LS9cg+hLozPaLpywrSe4d+rpd4CaL+aPfMl49zQcFTAyfbrLdSl4B46LozYag8w8/mc4BMQznzA8f4LaDS9+g+h8fpALM8Fpd+TN7+Lqg46q7ZAqA8n4rbQ2rEA2BIM8/bn4BQQyrYpanTTJLQc4747nSQzanD68LzM4FTAp/pS+diF+DQn4rEQyAz0anSmqM4V/LYQPA4S8S8FGFDAzFIjNsQhwaHCP/WEwePUw/DA+UIj2erIH0iINsQhP/rjwjQVygzSHdF=",
    "x-t": "1756801108524",
    # 'cookie': 'abRequestId=62ddc872-6aa4-52be-a801-9bdc7df8569b; a1=19768a94168phvedmkahlx6lxrfyua04srms8cggo50000220309; webId=86ef5f0e778ead5fe706f55202a2faf5; gid=yjWKY0j2JiM0yjWKY0j4yFSkKYUxhdf6T0x16klCK1CFiM28v7084Y888JJ8q8j8Djfdj2f0; x-user-id-creator.xiaohongshu.com=618f62ac000000001000d08b; customerClientId=135290779466254; x-user-id-ark.xiaohongshu.com=618f62ac000000001000d08b; x-user-id-zhaoshang.xiaohongshu.com=6676ec440000000007006f9e; web_session=040069b0fb21e41280dd2c959e3a4b017bcfdc; access-token-creator.xiaohongshu.com=customer.creator.AT-68c517543825570502821193lf0unesttqc6sbxj; galaxy_creator_session_id=1geKESv2R9DrRvEjUp1niTj3u7FFbqzaRtZx; galaxy.creator.beaker.session.id=1756433762993022025608; x-user-id-pro.xiaohongshu.com=618f62ac000000001000d08b; access-token-pro.xiaohongshu.com=customer.ares.AT-68c5175450013987413524516ixhjz1ix4m9y4uh; access-token-pro.beta.xiaohongshu.com=customer.ares.AT-68c5175450013987413524516ixhjz1ix4m9y4uh; customer-sso-sid=68c517545044374847339118c8usbiz3of7awbgt; x-user-id-pgy.xiaohongshu.com=59ebefa3e8ac2b2171a39d89; solar.beaker.session.id=AT-68c517545044374183919626b53iwa8xojodpxfy; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517545044374183919626b53iwa8xojodpxfy; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517545044374183919626b53iwa8xojodpxfy; webBuild=4.79.0; unread={%22ub%22:%2268ac76e2000000001b03e91f%22%2C%22ue%22:%2268b671b4000000001c00b665%22%2C%22uc%22:28}; xsecappid=xhs-pc-web; loadts=1756799285693; acw_tc=0a0d068317568002480328382e8c107f28bfe7ce02f3c9d36cf072df3a57c9; websectiga=10f9a40ba454a07755a08f27ef8194c53637eba4551cf9751c009d9afb564467; sec_poison_id=5f1b9186-15e6-4212-a121-05f007a32baa',
}


json_data = {
    "searchType": 1,
    # todo 这里需要修改
    "keyword": "探店 cos",
    "column": "clickNum",
    "sort": "desc",
    "pageNum": 1,
    "pageSize": 20,
    "brandUserId": "59ebefa3e8ac2b2171a39d89",
    "trackId": "kolGeneralSearch_d826f58a165f42b3b008e5eb8da0bd36",
    "marketTarget": None,
    "audienceGroup": [],
    "personalTags": [],
    "gender": None,
    "location": None,
    "signed": -1,
    "featureTags": [],
    "fansAge": 0,
    "fansGender": 0,
    "accumCommonImpMedinNum30d": [],
    "readMidNor30": [],
    "interMidNor30": [],
    "thousandLikePercent30": [],
    "noteType": 0,
    "notePriceLower": -1,
    "notePriceUpper": 1500,
    "videoPriceLower": -1,
    "videoPriceUpper": 1500,
    "progressOrderCnt": [],
    "tradeType": "不限",
    "tradeReportBrandIdSet": [],
    "excludedTradeReportBrandId": False,
    "estimateCpuv30d": [],
    "firstIndustry": "",
    "secondIndustry": "",
    "newHighQuality": 0,
    "filterIntention": False,
    "flagList": [
        {
            "flagType": "HAS_BRAND_COOP_BUYER_AUTH",
            "flagValue": "0",
        },
        {
            "flagType": "IS_HIGH_QUALITY",
            "flagValue": "0",
        },
    ],
    "activityCodes": [],
    "excludeLowActive": False,
    "fansNumUp": 0,
    "excludedTradeReportBrand": False,
    "excludedTradeInviteReportBrand": False,
}


def _make_session(headers: Dict[str, str], cookies: Dict[str, str]) -> requests.Session:
    """创建带重试的 Session。"""

    s = requests.Session()

    s.headers.update(headers)
    s.cookies.update(cookies)

    retry = Retry(
        total=RETRY_TOTAL,
        read=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_STATUS,
        allowed_methods=frozenset(["POST", "GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _post_json(session: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    """安全POST并返回JSON，包含状态码与JSON解析的校验。"""
    resp = session.post(BASE_URL, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        logger.error("响应非JSON，可疑的风控/登录失效：%s", e)
        raise
    return data


def _extract_rows(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """从接口返回的kol条目中提取我们需要的字段，容错缺失。"""

    rows: List[Dict[str, Any]] = []
    for it in items:
        user_id = it.get("userId", "")
        rows.append(
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
    return rows


def iter_pages(
    session: requests.Session, base_payload: Dict[str, Any]
) -> Generator[Dict[str, Any], None, None]:
    """
    按页迭代返回的KOL字典。
    - 自动计算总页数
    - 支持MAX_PAGES限制
    """

    # 确保不污染外部传入的payload
    payload = dict(base_payload)
    page_size = int(payload.get("pageSize", 20)) or 20
    payload["pageNum"] = 1

    # 先打一次，拿 total
    first = _post_json(session, payload)
    if not first.get("success"):
        raise RuntimeError(f"接口返回success=false，详情：{first}")

    data0 = first.get("data") or {}
    total = int(data0.get("total") or 0)
    kols = data0.get("kols") or []
    if total == 0 and not kols:
        logger.info("没有匹配的结果。")
        return

    # 计算总页数
    total_pages = math.ceil(total / page_size)
    if MAX_PAGES is not None:
        total_pages = min(total_pages, MAX_PAGES)

    logger.info("共 %s 条，预计 %s 页（pageSize=%s）。", total, total_pages, page_size)

    # 第1页
    for row in _extract_rows(kols):
        yield row

    # 后续页

    for page in range(2, total_pages + 1):
        payload["pageNum"] = page
        time.sleep(SLEEP_BETWEEN_PAGES)

        res = _post_json(session, payload)
        if not res.get("success"):
            logger.warning("第%s页 success=false，跳过。详情：%s", page, res)

            continue
        items = (res.get("data") or {}).get("kols") or []
        for row in _extract_rows(items):
            yield row


def _to_polars_df(rows: List[Dict[str, Any]]) -> pl.DataFrame:
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


def main() -> None:
    session = _make_session(headers=headers, cookies=cookies)

    # 复制你传入的 json_data，避免在生成器中污染外部对象
    base_payload = dict(json_data)

    # 强烈建议：自动用 pageSize 计算分页；pageNum 由逻辑控制
    base_payload.pop("pageNum", None)

    rows = list(iter_pages(session, base_payload))
    df = _to_polars_df(rows)
    write_excel_safely(df, "xhs_kol.xlsx")


if __name__ == "__main__":
    main()
