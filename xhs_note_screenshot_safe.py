import datetime
import json
import logging
import os
import random
import re
import time
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


# =========================
# 配置区
# =========================
CONFIG = {
    # 功能开关
    "enable_user_info": False,
    "enable_screenshots": True,
    # 输入输出
    "urls_filename": "urls.txt",
    "output_filename_prefix": "xiaohongshu_notes",
    "screenshots_dir": "./screenshots",
    # 浏览器持久化目录
    # 首次运行时手动登录，后续复用该目录中的登录状态
    "chrome_profile_dir": "./chrome_profile",
    # 每次运行最多处理多少篇
    "max_notes_per_run": 15,
    # 两篇笔记之间的等待区间
    "min_interval": 12,
    "max_interval": 25,
    # 页面加载等待时间
    "page_load_timeout": 35,
    "element_wait_timeout": 20,
    # 普通网络错误最多重试次数
    # 验证码、访问频繁、登录异常不会自动重试
    "max_retries": 1,
    # 是否在启动时暂停，等待人工确认登录状态
    "pause_for_manual_login": True,
}


# =========================
# 日志
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# =========================
# 工具函数
# =========================
def save_to_excel(data_list: list[dict], filename_prefix: str) -> None:
    """将结果保存为带时间戳的 Excel 文件。"""
    if not data_list:
        logging.warning("没有有效数据可保存。")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.xlsx"

    try:
        df = pd.DataFrame(data_list)
        df.to_excel(filename, index=False)
        logging.info("成功保存到：%s", os.path.abspath(filename))
        logging.info("总记录数：%s", len(data_list))
    except (IOError, PermissionError) as exc:
        logging.error("保存 Excel 失败：%s，错误：%s", filename, exc)
    except Exception as exc:
        logging.exception("导出 Excel 时发生异常：%s", exc)


def extract_url_from_line(text: str) -> Optional[str]:
    """从一行文本中提取第一个 URL。"""
    pattern = r"https?://[^\s]+"
    urls = re.findall(pattern, text)
    return urls[0].strip() if urls else None


def normalize_url(url: str) -> str:
    """
    规范化链接并移除查询参数，减少同一篇笔记重复访问。

    例如：
    https://www.xiaohongshu.com/explore/xxx?xsec_token=...
    转为：
    https://www.xiaohongshu.com/explore/xxx
    """
    try:
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    except Exception:
        return url.split("?", 1)[0]


def extract_note_id(url: str) -> str:
    """从常见小红书 URL 中提取笔记 ID，用于去重和截图命名。"""
    patterns = [
        r"/explore/([a-zA-Z0-9]+)",
        r"/discovery/item/([a-zA-Z0-9]+)",
        r"/item/([a-zA-Z0-9]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    return ""


def read_urls_from_file(filename: str) -> list[str]:
    """读取、规范化并去重 URL。"""
    urls: list[str] = []
    seen: set[str] = set()

    try:
        with open(filename, "r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                stripped_line = line.strip()
                if not stripped_line:
                    continue

                url = extract_url_from_line(stripped_line)
                if not url:
                    logging.warning(
                        "第 %s 行未找到有效 URL：%s",
                        line_number,
                        stripped_line,
                    )
                    continue

                normalized = normalize_url(url)
                note_id = extract_note_id(normalized)
                dedupe_key = note_id or normalized

                if dedupe_key in seen:
                    logging.info("跳过重复链接：%s", normalized)
                    continue

                seen.add(dedupe_key)
                urls.append(normalized)

        return urls

    except FileNotFoundError:
        logging.error("找不到 URL 文件：%s", filename)
        return []
    except UnicodeDecodeError:
        logging.error("URL 文件不是 UTF-8 编码：%s", filename)
        return []
    except Exception as exc:
        logging.exception("读取 URL 文件失败：%s", exc)
        return []


# =========================
# 浏览器
# =========================
def setup_driver() -> Optional[webdriver.Chrome]:
    """初始化 Chrome，并复用本地浏览器登录状态。"""
    chrome_options = Options()

    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--disable-notifications")

    profile_dir = os.path.abspath(CONFIG["chrome_profile_dir"])
    os.makedirs(profile_dir, exist_ok=True)

    chrome_options.add_argument(f"--user-data-dir={profile_dir}")
    chrome_options.add_argument("--profile-directory=Default")
    chrome_options.add_experimental_option(
        "excludeSwitches",
        ["enable-logging"],
    )

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(CONFIG["page_load_timeout"])
        driver.set_window_size(1280, 900)
        return driver
    except WebDriverException as exc:
        logging.error(
            "初始化 WebDriver 失败。请确认 Chrome 和 Selenium 可正常使用。错误：%s",
            exc,
        )
        return None


def wait_for_document_ready(driver: webdriver.Chrome, timeout: int = 20) -> bool:
    """等待 document.readyState 至少达到 interactive。"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: (
                d.execute_script("return document.readyState")
                in ("interactive", "complete")
            )
        )
        return True
    except TimeoutException:
        return False


def wait_for_note_page(driver: webdriver.Chrome, timeout: int = 20) -> bool:
    """等待笔记主体、初始状态或常见页面主体出现。"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: (
                "window.__INITIAL_STATE__" in d.page_source
                or len(d.find_elements(By.CSS_SELECTOR, "#noteContainer")) > 0
                or len(d.find_elements(By.CSS_SELECTOR, ".note-container")) > 0
                or len(d.find_elements(By.TAG_NAME, "main")) > 0
            )
        )
        return True
    except TimeoutException:
        return False


def detect_access_issue(driver: webdriver.Chrome) -> Optional[str]:
    """
    检测当前可见页面是否真的出现验证、访问限制或强制登录。

    注意：
    不再搜索完整 page_source，因为正常页面的 JavaScript、埋点和接口配置
    也可能包含 captcha、验证码等字符串，容易造成误判。
    """
    try:
        current_url = (driver.current_url or "").lower()
        page_title = (driver.title or "").strip().lower()

        # innerText 主要返回用户实际可见的页面文字，能避开脚本源码中的关键词。
        visible_text = driver.execute_script(
            "return document.body ? document.body.innerText : '';"
        )
        visible_text = (visible_text or "").strip().lower()
    except Exception as exc:
        logging.debug("访问异常检测失败：%s", exc)
        return None

    # URL 已明确跳转到验证页面时，直接判断。
    captcha_url_tokens = (
        "captcha",
        "verify",
        "verification",
        "risk",
        "challenge",
    )
    if any(token in current_url for token in captcha_url_tokens):
        return "captcha_redirect"

    # 页面标题明确表示验证。
    captcha_title_tokens = (
        "安全验证",
        "身份验证",
        "验证码",
        "访问验证",
    )
    if any(token in page_title for token in captcha_title_tokens):
        return "captcha"

    # 验证页通常会同时出现两类可见提示，避免单个词误判。
    captcha_primary = (
        "请完成验证",
        "拖动滑块",
        "滑块验证",
        "安全验证",
        "身份验证",
    )
    captcha_secondary = (
        "验证码",
        "验证后继续",
        "重新验证",
        "点击验证",
        "拖动",
        "滑块",
    )

    has_primary = any(token in visible_text for token in captcha_primary)
    has_secondary = any(token in visible_text for token in captcha_secondary)

    if has_primary and has_secondary:
        return "captcha"

    rate_limit_tokens = (
        "访问频繁",
        "操作频繁",
        "当前访问存在异常",
        "请求异常",
        "请稍后再试",
        "系统繁忙，请稍后重试",
    )
    if any(token in visible_text for token in rate_limit_tokens):
        return "rate_limit"

    # 登录提示必须是页面的主要内容才判断，避免把普通登录入口误认为掉线。
    login_tokens = (
        "登录后查看",
        "请先登录",
        "扫码登录",
        "手机号登录",
    )
    matched_login_tokens = sum(token in visible_text for token in login_tokens)

    # 可见文字较少且包含多个登录提示，通常才是强制登录页或登录遮罩。
    if matched_login_tokens >= 2 and len(visible_text) < 2500:
        return "login_required"

    return None


def open_page_with_retry(
    driver: webdriver.Chrome,
    url: str,
    max_retries: int = 1,
) -> tuple[bool, Optional[str]]:
    """
    打开页面。

    返回：
    (是否成功, 异常类型)

    验证码、访问频繁、登录异常不会重试。
    """
    for attempt in range(max_retries + 1):
        try:
            driver.get(url)
            wait_for_document_ready(
                driver,
                timeout=CONFIG["element_wait_timeout"],
            )

            issue = detect_access_issue(driver)
            if issue:
                return False, issue

            loaded = wait_for_note_page(
                driver,
                timeout=CONFIG["element_wait_timeout"],
            )

            if loaded:
                return True, None

            issue = detect_access_issue(driver)
            if issue:
                return False, issue

            logging.warning(
                "页面主体未在规定时间内加载：%s",
                url,
            )

        except TimeoutException:
            logging.warning(
                "页面加载超时，第 %s 次尝试：%s",
                attempt + 1,
                url,
            )

        except WebDriverException as exc:
            logging.warning(
                "浏览器访问失败，第 %s 次尝试：%s",
                attempt + 1,
                exc,
            )

        if attempt < max_retries:
            delay = 8 * (2**attempt) + random.uniform(2, 5)
            logging.info(
                "普通网络错误，等待 %.1f 秒后重试。",
                delay,
            )
            time.sleep(delay)

    return False, "load_failed"


# =========================
# 页面解析
# =========================
def parse_initial_state(page_source: str) -> dict:
    """解析页面中的 window.__INITIAL_STATE__。"""
    patterns = [
        r"<script>\s*window\.__INITIAL_STATE__\s*=\s*(.*?)</script>",
        r"window\.__INITIAL_STATE__\s*=\s*(.*?)(?:</script>|;\s*$)",
    ]

    raw = None

    for pattern in patterns:
        match = re.search(pattern, page_source, re.S)
        if match:
            raw = match.group(1).strip()
            break

    if not raw:
        return {}

    raw = re.sub(r":undefined(?=[,}])", ":null", raw)
    raw = re.sub(r"\bundefined\b", "null", raw)

    if raw.endswith(";"):
        raw = raw[:-1]

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logging.warning("__INITIAL_STATE__ 解析失败：%s", exc)
        return {}


def convert_timestamp(value) -> str:
    """将秒或毫秒时间戳转换为可读时间。"""
    if value in (None, ""):
        return ""

    try:
        number = int(value)

        if number > 10_000_000_000:
            number = number / 1000

        return datetime.datetime.fromtimestamp(number).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError, TypeError, OSError:
        return str(value)


def parse_note_info(page_source: str, url: str) -> dict:
    """从初始状态中解析笔记信息。"""
    note_info = {
        "标题": "",
        "正文": "",
        "作者": "",
        "点赞数": 0,
        "收藏数": 0,
        "评论数": 0,
        "分享数": 0,
        "发布时间": "",
        "IP属地": "",
        "笔记ID": extract_note_id(url),
        "链接": url,
        "处理状态": "成功",
    }

    state = parse_initial_state(page_source)
    note_store = state.get("note", {})
    note_detail_map = note_store.get("noteDetailMap", {})

    if not note_detail_map:
        note_info["处理状态"] = "未解析到笔记数据"
        return note_info

    current_note_id = (
        note_store.get("currentNoteId")
        or note_store.get("firstNoteId")
        or extract_note_id(url)
    )

    if current_note_id and current_note_id in note_detail_map:
        detail = note_detail_map[current_note_id]
    else:
        detail = next(iter(note_detail_map.values()), {})

    note = detail.get("note", {}) or detail
    interact = note.get("interactInfo", {}) or {}
    user = note.get("user", {}) or {}

    desc = note.get("desc", "") or ""
    title = note.get("title", "") or ""
    fallback_title = desc.strip().split("\n")[0] if desc.strip() else "无标题"

    note_info.update(
        {
            "笔记ID": note.get("noteId", current_note_id or ""),
            "标题": title or fallback_title,
            "正文": desc,
            "作者": user.get("nickname", ""),
            "点赞数": interact.get("likedCount", 0),
            "收藏数": interact.get("collectedCount", 0),
            "评论数": interact.get("commentCount", 0),
            "分享数": interact.get("shareCount", 0),
            "发布时间": convert_timestamp(note.get("time", "")),
            "IP属地": note.get("ipLocation", ""),
        }
    )

    return note_info


# =========================
# 截图
# =========================
def safe_filename(text: str, max_length: int = 80) -> str:
    """生成适合文件名的字符串。"""
    cleaned = re.sub(r'[\\/:*?"<>|\r\n]+', "_", text).strip(" ._")
    return cleaned[:max_length] or "note"


def save_note_screenshot(
    driver: webdriver.Chrome,
    screenshot_path: str,
) -> bool:
    """
    优先截图笔记主体元素，找不到时退回当前窗口截图。
    """
    selectors = [
        # "#noteContainer",
        # ".note-container",
        # ".interaction-container",
        "main",
    ]

    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)

            for element in elements:
                if not element.is_displayed():
                    continue

                size = element.size
                if size.get("width", 0) < 300 or size.get("height", 0) < 200:
                    continue

                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});",
                    element,
                )
                time.sleep(1)
                element.screenshot(screenshot_path)
                return True

        except Exception as exc:
            logging.debug(
                "元素截图失败，选择器 %s：%s",
                selector,
                exc,
            )

    try:
        driver.save_screenshot(screenshot_path)
        return True
    except Exception as exc:
        logging.error("保存截图失败：%s", exc)
        return False


# =========================
# 可选：作者主页信息
# =========================
def parse_user_profile(
    driver: webdriver.Chrome,
    note_page_source: str,
) -> dict:
    """
    可选地访问作者主页并获取基本信息。

    注意：
    开启该功能会增加页面访问次数，默认关闭。
    """
    user_info = {
        "用户名": "",
        "用户ID": "",
        "粉丝量": "",
        "作者主页": "",
    }

    soup = BeautifulSoup(note_page_source, "html.parser")

    author_link_tag = soup.select_one("a.name") or soup.select_one(
        'a[href*="/user/profile/"]'
    )

    if not author_link_tag or not author_link_tag.get("href"):
        logging.warning("未找到作者主页链接。")
        return user_info

    href = author_link_tag["href"]

    if href.startswith("http"):
        profile_url = href
    else:
        profile_url = "https://www.xiaohongshu.com" + href

    user_info["作者主页"] = profile_url

    success, issue = open_page_with_retry(
        driver,
        profile_url,
        max_retries=0,
    )

    if not success:
        logging.warning(
            "作者主页打开失败：%s，原因：%s",
            profile_url,
            issue,
        )
        return user_info

    state = parse_initial_state(driver.page_source)
    user_page_data = state.get("user", {}).get("userPageData", {})
    basic_info = user_page_data.get("basicInfo", {}) or {}
    interactions = user_page_data.get("interactions", []) or []

    user_info["用户名"] = basic_info.get("nickname", "")
    user_info["用户ID"] = basic_info.get("redId", "")

    for item in interactions:
        if item.get("name") == "粉丝":
            user_info["粉丝量"] = item.get("count", "")
            break

    return user_info


# =========================
# 主处理流程
# =========================
def process_notes(
    note_urls: list[str],
    output_filename_prefix: str,
    **kwargs,
) -> None:
    enable_screenshots = kwargs.get("enable_screenshots", False)
    enable_user_info = kwargs.get("enable_user_info", False)
    screenshots_dir = kwargs.get("screenshots_dir", "./screenshots")
    max_notes_per_run = kwargs.get("max_notes_per_run", 15)
    min_interval = kwargs.get("min_interval", 12)
    max_interval = kwargs.get("max_interval", 25)
    max_retries = kwargs.get("max_retries", 1)

    driver = setup_driver()
    if not driver:
        return

    all_notes_data: list[dict] = []

    if enable_screenshots:
        os.makedirs(screenshots_dir, exist_ok=True)
        logging.info(
            "截图功能已开启，目录：%s",
            os.path.abspath(screenshots_dir),
        )

    urls_to_process = note_urls[:max_notes_per_run]

    if len(note_urls) > len(urls_to_process):
        logging.warning(
            "本次仅处理前 %s 篇，剩余 %s 篇请下次运行。",
            len(urls_to_process),
            len(note_urls) - len(urls_to_process),
        )

    try:
        driver.get("https://www.xiaohongshu.com")
        wait_for_document_ready(driver, timeout=20)

        if CONFIG["pause_for_manual_login"]:
            print()
            print("=" * 60)
            print("请确认浏览器中的小红书账号已正常登录。")
            print("首次运行请手动完成登录；程序会保存登录状态。")
            print("确认后回到终端，按回车继续。")
            print("=" * 60)
            input()

        startup_issue = detect_access_issue(driver)
        if startup_issue in {"captcha", "rate_limit", "captcha_redirect"}:
            logging.error(
                "启动页检测到访问限制：%s。本次任务停止。",
                startup_issue,
            )
            return

        for index, url in enumerate(urls_to_process, start=1):
            logging.info(
                "正在处理第 %s/%s 个链接：%s",
                index,
                len(urls_to_process),
                url,
            )

            row = {
                "标题": "",
                "正文": "",
                "作者": "",
                "点赞数": 0,
                "收藏数": 0,
                "评论数": 0,
                "分享数": 0,
                "发布时间": "",
                "IP属地": "",
                "笔记ID": extract_note_id(url),
                "链接": url,
                "处理状态": "",
                "截图路径": "",
            }

            try:
                success, issue = open_page_with_retry(
                    driver,
                    url,
                    max_retries=max_retries,
                )

                if not success:
                    row["处理状态"] = f"打开失败：{issue}"
                    all_notes_data.append(row)

                    if issue in {
                        "captcha",
                        "rate_limit",
                        "login_required",
                        "captcha_redirect",
                    }:
                        logging.error(
                            "检测到平台验证或访问异常：%s。"
                            "为避免继续请求，本次任务立即停止。",
                            issue,
                        )
                        break

                    logging.warning("跳过当前链接：%s", url)
                    continue

                # 给页面中的图片和异步内容少量加载时间
                time.sleep(random.uniform(2.0, 4.0))

                issue = detect_access_issue(driver)
                if issue:
                    row["处理状态"] = f"页面异常：{issue}"
                    all_notes_data.append(row)

                    logging.error(
                        "检测到平台验证或访问异常：%s。本次任务立即停止。",
                        issue,
                    )
                    break

                page_source = driver.page_source
                note_info = parse_note_info(page_source, url)
                row.update(note_info)

                logging.info(
                    "标题：%s，点赞：%s，收藏：%s，评论：%s",
                    row.get("标题", ""),
                    row.get("点赞数", 0),
                    row.get("收藏数", 0),
                    row.get("评论数", 0),
                )

                if enable_screenshots:
                    note_id = row.get("笔记ID") or f"note_{index}"
                    title_part = safe_filename(row.get("标题") or f"note_{index}")
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

                    screenshot_filename = (
                        f"{index:03d}_{note_id}_{title_part}_{timestamp}.png"
                    )
                    screenshot_path = os.path.join(
                        screenshots_dir,
                        screenshot_filename,
                    )

                    if save_note_screenshot(driver, screenshot_path):
                        row["截图路径"] = os.path.abspath(screenshot_path)
                        logging.info(
                            "截图已保存：%s",
                            row["截图路径"],
                        )
                    else:
                        row["处理状态"] += "；截图失败"

                if enable_user_info:
                    user_info = parse_user_profile(driver, page_source)
                    row.update(user_info)

                all_notes_data.append(row)

            except KeyboardInterrupt:
                logging.warning("用户主动终止任务。")
                row["处理状态"] = "用户终止"
                all_notes_data.append(row)
                break

            except Exception as exc:
                logging.exception(
                    "处理链接时发生异常：%s，错误：%s",
                    url,
                    exc,
                )
                row["处理状态"] = f"处理异常：{exc}"
                all_notes_data.append(row)

            finally:
                if index < len(urls_to_process):
                    interval = random.uniform(
                        min_interval,
                        max_interval,
                    )
                    logging.info(
                        "等待 %.1f 秒后处理下一篇。",
                        interval,
                    )
                    time.sleep(interval)

    finally:
        save_to_excel(
            all_notes_data,
            output_filename_prefix,
        )

        logging.info("任务结束，正在关闭浏览器。")
        try:
            driver.quit()
        except Exception:
            pass


def main() -> None:
    urls = read_urls_from_file(CONFIG["urls_filename"])

    if not urls:
        logging.warning("没有读取到有效 URL，程序终止。")
        return

    process_notes(
        urls,
        CONFIG["output_filename_prefix"],
        enable_screenshots=CONFIG["enable_screenshots"],
        enable_user_info=CONFIG["enable_user_info"],
        screenshots_dir=CONFIG["screenshots_dir"],
        max_notes_per_run=CONFIG["max_notes_per_run"],
        min_interval=CONFIG["min_interval"],
        max_interval=CONFIG["max_interval"],
        max_retries=CONFIG["max_retries"],
    )


if __name__ == "__main__":
    main()
