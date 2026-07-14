import json
import logging
import re
import time
from typing import Any

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# 日志设置
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("zhihu_kol")


# =========================
# 请求配置
# =========================

API_URL = (
    "https://cheese.zhihu.com/api/v1/customer/square/pu/detail"
    "?pu_id={pu_id}&square_type=invite"
)

# 这里填写你浏览器请求中的 Current-Id
CURRENT_ID = "795582"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) "
        "Gecko/20100101 Firefox/152.0"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.6,en;q=0.5",
    "Referer": "https://cheese.zhihu.com/",
    "Current-Id": CURRENT_ID,
}

# 如果接口出现 401、403 或提示未登录，
# 从浏览器复制请求 Cookie，按下面格式填写。
COOKIES = {
    # "z_c0": "你的 z_c0",
    # "d_c0": "你的 d_c0",
    # "SESSIONID": "你的 SESSIONID",
    "_xsrf": "Zd4DMEizEJUmMJAyInLsISU735N3flAV",
    "BEC": "d892da65acb7e34c89a3073e8fa2254f",
    "_zap": "a1e37612-85e5-456f-b552-6c46f46aee78",
    "d_c0": "uNAYHT0AmByPToaoBPTTs9TXXo7CcN37sGU=|1784016947",
    "captcha_session_v2": "2|1:0|10:1784017088|18:captcha_session_v2|88:cEVva0lvT3lyQTJWbXUwcStRVEdOZnJQTnhVZXR2YkppRlc0UVZKelpXTDU0ODgyODloT3Zkdy84SU05TWhtQw==|ffccc059c133d53a99482f3ae8ebbf76d8bc12683af34ce3f98ec67fb1f4a6ce",
    "Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49": "1784016948",
    "Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49": "1784018377",
    "HMACCOUNT": "5D353A490C1C96FA",
    "captcha_ticket_v2": "2|1:0|10:1784017101|17:captcha_ticket_v2|728:eyJ2YWxpZGF0ZSI6IkNOMzFfV25MR3lhdGRiQXB5ZmpXT0hybGU5cWhNVlQwME1LbE0xZUdURkdIKkZMcWxpcl9qb0FrcHRhM3ZDM2lXTTlHeWV3MWlvZDFONm5STmZmS0RmanUzM0RpUktzYlRpbmJEVy5YOURzb09MY3RJWE1PaERKNEEuWXpSUk9RRGJGR29XQ0syeTJlQzRraG9NR1plR2h0TklpMjZWQWdLU1laZmFpSmpoWEdqRjZycXNtTWJhLnFFSnZwclJkejRmTE1VOURlQS5YaGNRU3dJKlpOZE9tQWEzclRNWXJnYmVJZWl5KjNpdUsySUdHTjV0Nm5FTzZ5Qk05UGcwQWpodC42MzIyR0ZKNDVOUTlMbGRkdzZUQ25FM0tFcktRb0hTNFdFMU9pSnBTY3c5aXRKRmRLbmZmczJtRVFPdHNicWpBR0MxWWVjSV95ZWZZSG91YThwNXFueFRVRWdmMnIxWVNsTTRnUkM5c0ttVzJoZGVQVFk2eFR5NU5DZmVGTmJSeEZvV2g4dDl4a2xZbmNpUDZqNUpVTTNtZ0ptTEZfUUpfVnNRVFZtUEFzbnpwTy5OZmNKMGNKLnY0SUdsUWhGWXdfMG5DM0Znb0J4TmZjRW11NmdwRlUuY1d5SUNzQy51WEFiSHB4UTAzQjA1QUl4MTB2eWw5cHZVdi5iYW5VUThYeW82NEIqelk3N192X2lfMSJ9|49c48c94265f4e826fb9adc193bd513a84f3bfe0291a37d6e8e2ca293e28f9d4",
    "z_c0": "2|1:0|10:1784017145|4:z_c0|92:Mi4xaTdjX0RBQUFBQUM0MEJnZFBRQ1lIQ1lBQUFCZ0FsVk4tRDVEYXdCdk1xRUN3SXdvMEJIMDlSemlFZUpqbV91WFVn|3c5bd4f1be9c840858e3d8bdc2fe4b51d61f92552d975ff8c8f3d8fe84b66fcf",
    "SUBMIT_0": "3647c6b1-41ab-44c0-827c-95005cf9479e",
    "__zse_ck": "005_ULmlD=EdeTmpSTThrOTo7CWRzhA7yECnHu9hJFMCYB1W4FII9OhjWgGGwbpL0Ie4aU57/3lkxEyyXEcXluN7=efjKbGZfA4JrqdqE64o0omWdUkUt9PKqJs95vgeiDkt-6YTdROjyQSMeVWxi6YWHWNnDYRMeoJbHCfjBuFjHpC+8S2jO/BvLAHh5fvgrqJZ+O465BWWnLuC0mHMXzCEn89tDGSqdPiD5JsJUfj3902dEo9WcH4ntcozRc8ik63YV",
}


# =========================
# 输入账号
# =========================

# 支持以下格式：
# 1. https://cheese.zhihu.com/biz/PUDetails/338691
# 2. https://cheese.zhihu.com/api/v1/customer/square/pu/detail?pu_id=338691&square_type=invite
# 3. 338691

ALL_TARGETS = [
    # "338692",
    # "https://cheese.zhihu.com/biz/PUDetails/338693",
    "https://cheese.zhihu.com/biz/PUDetails/338691",
    "https://cheese.zhihu.com/biz/PUDetails/240268",
    "https://cheese.zhihu.com/biz/PUDetails/250276",
    "https://cheese.zhihu.com/biz/PUDetails/452262",
    "https://cheese.zhihu.com/biz/PUDetails/287489",
    "https://cheese.zhihu.com/biz/PUDetails/247312",
    "https://cheese.zhihu.com/biz/PUDetails/245872",
    "https://cheese.zhihu.com/biz/PUDetails/306090",
    "https://cheese.zhihu.com/biz/PUDetails/308800",
    "https://cheese.zhihu.com/biz/PUDetails/308800",
    "https://cheese.zhihu.com/biz/PUDetails/486271",
    "https://cheese.zhihu.com/biz/PUDetails/484975",
    "https://cheese.zhihu.com/biz/PUDetails/314058",
    "https://cheese.zhihu.com/biz/PUDetails/237228",
    "https://cheese.zhihu.com/biz/PUDetails/277084",
    "https://cheese.zhihu.com/biz/PUDetails/267550",
]

OUTPUT_PATH = "zhihu_kol.xlsx"


# =========================
# 工具函数
# =========================


def create_session() -> requests.Session:
    """
    创建带重试机制的请求会话。
    """
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(HEADERS)
    session.cookies.update(COOKIES)

    return session


def extract_pu_id(target: str | int) -> str:
    """
    从数字、知任务详情页链接或接口链接中提取 pu_id。
    """
    text = str(target).strip()

    if text.isdigit():
        return text

    patterns = [
        r"/PUDetails/(\d+)",
        r"[?&]pu_id=(\d+)",
        r"/pu/detail/(\d+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1)

    raise ValueError(f"无法从目标中提取 pu_id：{target}")


def cents_to_yuan(value: Any) -> float | None:
    """
    将接口中的分转换为元。

    例如：
    200900 -> 2009.00 元
    """
    if value in (None, "", "--"):
        return None

    try:
        return round(float(value) / 100, 2)
    except TypeError, ValueError:
        return None


def join_unique(values: list[str], separator: str = "、") -> str:
    """
    去重并拼接字符串。
    """
    result = []

    for value in values:
        value = str(value).strip()

        if value and value not in result:
            result.append(value)

    return separator.join(result)


def get_fields(data: dict[str, Any]) -> str:
    """
    提取达人领域。

    优先级：
    1. order_field_v2
    2. professional_background
    3. commercial_score_personal_v2
    4. background
    """
    fields = []

    for item in data.get("order_field_v2") or []:
        if isinstance(item, dict):
            field_name = item.get("field_name")
            if field_name:
                fields.append(field_name)

    if not fields:
        for item in data.get("professional_background") or []:
            if isinstance(item, dict):
                name = item.get("name")
                if name:
                    fields.append(name)

                bayes_field_name = item.get("bayes_field_name")
                if bayes_field_name:
                    fields.append(bayes_field_name)

    if not fields:
        for item in data.get("commercial_score_personal_v2") or []:
            if isinstance(item, dict):
                field_name = item.get("field_name")
                if field_name:
                    fields.append(field_name)

    if not fields and data.get("background"):
        fields.append(data["background"])

    return join_unique(fields)


def get_intro(data: dict[str, Any]) -> str:
    """
    提取简介。

    优先使用 short_intro；若为空则组合背景和职业信息。
    """
    short_intro = str(data.get("short_intro") or "").strip()

    if short_intro:
        return short_intro

    intro_parts = []

    background = str(data.get("background") or "").strip()
    if background:
        intro_parts.append(background)

    for item in data.get("professional_background") or []:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            if name:
                intro_parts.append(name)

    for item in data.get("family_role") or []:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            if name:
                intro_parts.append(name)

    return join_unique(intro_parts)


def get_location(data: dict[str, Any]) -> str:
    """
    拼接省份和城市。
    """
    province = str(data.get("province_name") or "").strip()
    city = str(data.get("city_name") or "").strip()

    if province and city:
        # 直辖市或省市名称相同时避免重复
        if province == city:
            return province

        return f"{province} {city}"

    return province or city


def get_price(data: dict[str, Any]) -> float | None:
    """
    提取知任务报价。

    优先选取：
    1. order_type=1 的 current_price
    2. 任意 offer 的 current_price
    3. order_price
    4. floor_price
    """
    offers = data.get("offer") or []

    if not isinstance(offers, list) or not offers:
        return None

    selected_offer = None

    # 优先寻找图文/文章类型的报价
    for offer in offers:
        if (
            isinstance(offer, dict)
            and offer.get("order_type") == 1
            and offer.get("current_price") not in (None, "", 0)
        ):
            selected_offer = offer
            break

    # 未找到时选择第一个存在价格的报价
    if selected_offer is None:
        for offer in offers:
            if not isinstance(offer, dict):
                continue

            if any(
                offer.get(key) not in (None, "", 0)
                for key in ("current_price", "order_price", "floor_price")
            ):
                selected_offer = offer
                break

    if selected_offer is None:
        return None

    raw_price = (
        selected_offer.get("current_price")
        or selected_offer.get("order_price")
        or selected_offer.get("floor_price")
    )

    return cents_to_yuan(raw_price)


def format_work_item(work: Any) -> str:
    """
    将单条合作作品转换成文本。
    兼容不同的 pu_works 返回字段。
    """
    if not isinstance(work, dict):
        return str(work).strip()

    title = ""

    title_keys = (
        "title",
        "name",
        "content_title",
        "work_title",
        "case_name",
        "brand_name",
        "goods_name",
    )

    for key in title_keys:
        value = work.get(key)

        if value:
            title = str(value).strip()
            break

    url = ""

    url_keys = (
        "url",
        "link",
        "content_url",
        "work_url",
        "jump_url",
    )

    for key in url_keys:
        value = work.get(key)

        if value:
            url = str(value).strip()
            break

    if title and url:
        return f"{title}：{url}"

    if title:
        return title

    if url:
        return url

    # 字段结构未知时，保留完整 JSON，避免数据丢失
    return json.dumps(work, ensure_ascii=False)


def get_cooperation_cases(data: dict[str, Any]) -> str:
    """
    提取合作案例。

    如果 pu_works 为空，但有历史合作次数，
    则显示“历史合作X次”。
    """
    works = data.get("pu_works") or []

    formatted_works = []

    if isinstance(works, list):
        for work in works:
            formatted = format_work_item(work)

            if formatted:
                formatted_works.append(formatted)

    if formatted_works:
        return "\n".join(formatted_works)

    order_times = data.get("order_times")

    if order_times not in (None, "", 0, "0"):
        return f"历史合作 {order_times} 次"

    return ""


def fetch_creator_detail(
    session: requests.Session,
    pu_id: str,
) -> dict[str, Any]:
    """
    请求单个知乎创作者详情。
    """
    api_url = API_URL.format(pu_id=pu_id)

    # 每个达人的详情页 Referer 更准确
    request_headers = {
        "Referer": f"https://cheese.zhihu.com/biz/PUDetails/{pu_id}",
    }

    response = session.get(
        api_url,
        headers=request_headers,
        timeout=20,
    )

    response.raise_for_status()

    try:
        result = response.json()
    except requests.JSONDecodeError as exc:
        raise RuntimeError(
            f"pu_id={pu_id} 返回内容不是 JSON：{response.text[:300]}"
        ) from exc

    if not result.get("is_success"):
        error = result.get("err") or {}
        error_code = error.get("code", "")
        error_message = error.get("msg", "未知错误")

        raise RuntimeError(
            f"pu_id={pu_id} 接口请求失败：{error_code} - {error_message}"
        )

    data = result.get("data")

    if not isinstance(data, dict):
        raise RuntimeError(f"pu_id={pu_id} 返回结果中没有有效 data")

    return data


def parse_creator(data: dict[str, Any], pu_id: str) -> dict[str, Any]:
    """
    将接口数据整理成最终 Excel 字段。
    """
    follower_count = data.get("follower_count")

    try:
        follower_wan = round(float(follower_count) / 10_000, 4)
    except TypeError, ValueError:
        follower_wan = None

    personal_url = str(data.get("url") or "").strip()

    # 接口没有返回主页时，尝试使用 hash_id
    if not personal_url:
        hash_id = str(data.get("hash_id") or "").strip()

        if hash_id:
            personal_url = f"https://www.zhihu.com/people/{hash_id}"

    return {
        # 这里按照常见媒介表习惯，将达人昵称作为“知乎ID”
        "知乎ID": data.get("name", ""),
        "个人链接": personal_url,
        "粉丝量：万": follower_wan,
        "领域": get_fields(data),
        "合作案例": get_cooperation_cases(data),
        "简介": get_intro(data),
        "知任务价（24号前有效）": get_price(data),
        "所在地": get_location(data),
        # 需要排查数据时，可以取消下面两行的注释
        # "PU_ID": data.get("uid") or pu_id,
        # "知乎Hash ID": data.get("hash_id", ""),
    }


def to_polars_df(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """
    列表转换为 Polars DataFrame。
    """
    if not rows:
        return pl.DataFrame()

    return pl.DataFrame(rows, strict=False)


def write_excel_safely(df: pl.DataFrame, path: str) -> None:
    """
    优先使用 Polars 写 Excel；
    失败时自动降级为 Pandas。
    """
    if df.is_empty():
        logger.info("没有有效数据，不写出文件。")
        return

    try:
        df.write_excel(
            path,
            worksheet="知乎达人",
            autofit=True,
        )
        logger.info("已写出：%s（Polars）", path)
        return

    except Exception as exc:
        logger.warning(
            "Polars 写 Excel 失败，尝试使用 Pandas。原因：%s",
            exc,
        )

    try:
        pandas_df = df.to_pandas()
        pandas_df.to_excel(
            path,
            index=False,
            sheet_name="知乎达人",
        )
        logger.info("已写出：%s（Pandas）", path)

    except Exception as exc:
        logger.error("Pandas 写 Excel 也失败：%s", exc)
        raise


# =========================
# 主程序
# =========================


def main() -> None:
    session = create_session()

    all_rows: list[dict[str, Any]] = []
    failed_targets: list[dict[str, str]] = []

    # 防止重复抓取
    seen_pu_ids = set()

    for index, target in enumerate(ALL_TARGETS, start=1):
        try:
            pu_id = extract_pu_id(target)

            if pu_id in seen_pu_ids:
                logger.info("跳过重复 pu_id：%s", pu_id)
                continue

            seen_pu_ids.add(pu_id)

            logger.info(
                "[%s/%s] 正在获取 pu_id=%s",
                index,
                len(ALL_TARGETS),
                pu_id,
            )

            data = fetch_creator_detail(session, pu_id)
            row = parse_creator(data, pu_id)

            all_rows.append(row)

            logger.info(
                "获取成功：%s，粉丝量=%s万，报价=%s元",
                row["知乎ID"],
                row["粉丝量：万"],
                row["知任务价（24号前有效）"],
            )

        except Exception as exc:
            logger.exception("获取失败：%s", target)

            failed_targets.append(
                {
                    "目标": str(target),
                    "失败原因": str(exc),
                }
            )

        # 控制请求频率
        time.sleep(1)

    df = to_polars_df(all_rows)

    if not df.is_empty():
        print(df)
        write_excel_safely(df, OUTPUT_PATH)

    if failed_targets:
        logger.warning(
            "共有 %s 个目标获取失败：\n%s",
            len(failed_targets),
            json.dumps(
                failed_targets,
                ensure_ascii=False,
                indent=2,
            ),
        )

        failed_df = pl.DataFrame(failed_targets, strict=False)
        failed_df.write_csv("zhihu_failed.csv")

        logger.info("失败记录已保存至：zhihu_failed.csv")


if __name__ == "__main__":
    main()
