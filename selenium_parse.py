import datetime
import json
import logging
import os
import re
import time
import random

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium_stealth import stealth
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options

# --- 配置区 ---
# 在这里修改所有设置，无需改动下面的代码
CONFIG = {
    "enable_user_info": False,  # True: 开启主页信息爬取, False: 关闭
    "enable_screenshots": False,  # True: 开启截图, False: 关闭截图
    "urls_filename": "urls.txt",  # 存储URL列表的文件
    "cookies_filename": "cookies.txt",  # 存储Cookie的文件
    "output_filename_prefix": "xiaohongshu_notes",  # Excel文件名前缀
    "screenshots_dir": "./screenshots",  # 截图保存的文件夹
}

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def save_to_excel(data_list, filename_prefix):
    """将数据保存到带时间戳的Excel文件，并增加错误处理。"""
    if not data_list:
        logging.warning("没有有效数据可保存。")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.xlsx"

    # 使用字典列表创建DataFrame，更灵活
    df = pd.DataFrame(data_list)

    try:
        df.to_excel(filename, index=False)
        logging.info(f"成功保存到: {os.path.abspath(filename)}")
        logging.info(f"总记录数: {len(data_list)}")
    except (IOError, PermissionError) as e:
        logging.error(f"保存Excel文件失败: {filename}。错误: {e}")


def extract_url_from_line(text):
    """从单行文本中提取第一个URL，找不到则返回None。"""
    pattern = r"https?://[^\s]+"
    urls = re.findall(pattern, text)
    return urls[0].strip() if urls else None


def read_urls_from_file(filename):
    """从文件中读取并解析URLs，过滤掉无效行。"""
    urls = []
    try:
        with open(filename, "r", encoding="utf-8") as file:
            for line in file:
                if stripped_line := line.strip():  # 过滤空行
                    url = extract_url_from_line(stripped_line)
                    if url:
                        urls.append(url)
                    else:
                        logging.warning(f"在行中未找到有效URL: '{stripped_line}'")
        return urls
    except FileNotFoundError:
        logging.error(f"错误: 找不到URL文件 {filename}!")
        return []
    except UnicodeDecodeError:
        logging.error(f"错误: 文件 {filename} 编码无法解码，请确认文件为 UTF-8！")
        return []


def setup_driver():
    """配置并初始化Chrome WebDriver。"""
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    try:
        driver = webdriver.Chrome(options=chrome_options)
        stealth(
            driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        driver.set_window_size(1280, 800)
        return driver
    except WebDriverException as e:
        logging.error(
            f"初始化WebDriver失败，请检查chromedriver是否已安装并与Chrome版本匹配。错误: {e}"
        )
        return None


def load_cookies(driver, cookies_file):
    """从文件加载Cookies并添加到WebDriver。"""
    try:
        with open(cookies_file, "r") as file:
            cookie_string = file.read().strip()
        driver.get("https://www.xiaohongshu.com")
        time.sleep(2)
        driver.delete_all_cookies()
        for cookie_pair in cookie_string.split(";"):
            if "=" in cookie_pair:
                name, value = cookie_pair.strip().split("=", 1)
                driver.add_cookie(
                    {"name": name, "value": value, "domain": ".xiaohongshu.com"}
                )
        logging.info("Cookies加载成功。")
        return True
    except FileNotFoundError:
        logging.error(
            f"Cookie文件 '{cookies_file}' 未找到。将以未登录状态继续，可能无法获取数据。"
        )
        return False
    except Exception as e:
        logging.error(f"加载或解析Cookie时发生错误: {e}")
        return False


def process_notes(note_urls, cookies_filename, output_filename_prefix, **kwargs):
    """
    处理小红书笔记URL列表，抓取数据并根据配置进行截图和用户信息抓取。
    """
    enable_screenshots = kwargs.get("enable_screenshots", False)
    enable_user_info = kwargs.get("enable_user_info", False)
    screenshots_dir = kwargs.get("screenshots_dir", "./screenshots")

    driver = setup_driver()
    if not driver:
        return

    all_notes_data = []

    try:
        if load_cookies(driver, cookies_filename):
            driver.refresh()
            time.sleep(2)

        if enable_screenshots:
            os.makedirs(screenshots_dir, exist_ok=True)
            logging.info(f"截图功能已开启，将保存至 '{screenshots_dir}' 目录。")
        if enable_user_info:
            logging.info("主页信息爬取功能已开启。")

        for i, url in enumerate(note_urls):
            logging.info(f"正在处理第 {i + 1}/{len(note_urls)} 个链接: {url}")
            if (i + 1) % 51 == 0:  # 每处理50个链接
                sleep_duration = random.uniform(60, 120)
                logging.warning(
                    f"已处理 {i + 1} 个链接，进入批处理休眠 {int(sleep_duration)} 秒..."
                )
                time.sleep(sleep_duration)

            # 初始化笔记和用户信息字段
            note_info = {
                "标题": "N/A",
                "链接": url,
                "点赞数": 0,
                "收藏数": 0,
                "评论数": 0,
            }
            user_info = {"用户名": "N/A", "用户ID": "N/A", "粉丝量": "N/A"}

            try:
                driver.get(url)
                time.sleep(random.uniform(2, 4))

                if enable_screenshots:
                    screenshot_path = os.path.join(
                        screenshots_dir,
                        f"note_{i + 1}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.png",
                    )
                    driver.save_screenshot(screenshot_path)
                    logging.info(f"截图已保存到: {screenshot_path}")

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")

                if soup.find("meta", attrs={"name": "og:title"}) is None:
                    logging.warning(
                        f"无法访问或解析笔记: {url}。可能需要验证或笔记已删除。"
                    )
                    note_info["标题"] = "无法访问或解析"
                    all_notes_data.append({**note_info, **user_info})
                    continue

                # 1. 抓取笔记基础信息
                title_tag = soup.find("meta", attrs={"name": "og:title"})
                note_info["标题"] = title_tag.get("content", "无标题")
                note_info["点赞数"] = soup.find(
                    "meta", attrs={"name": "og:xhs:note_like"}
                ).get("content", 0)
                note_info["收藏数"] = soup.find(
                    "meta", attrs={"name": "og:xhs:note_collect"}
                ).get("content", 0)
                note_info["评论数"] = soup.find(
                    "meta", attrs={"name": "og:xhs:note_comment"}
                ).get("content", 0)
                logging.info(
                    f"标题: {note_info['标题']}, 点赞: {note_info['点赞数']}, 收藏: {note_info['收藏数']}, 评论: {note_info['评论数']}"
                )

                # 2. 如果开启，抓取用户信息
                if enable_user_info:
                    # 在笔记页面找到作者链接
                    author_link_tag = soup.find("a", attrs={"class": "name"})
                    if author_link_tag and author_link_tag.has_attr("href"):
                        profile_url = (
                            "https://www.xiaohongshu.com" + author_link_tag["href"]
                        )
                        logging.info(f"找到作者主页链接: {profile_url}")
                        user_info["profile_url"] = profile_url
                        match = re.search(r"user/profile/([a-z0-9]{24})", profile_url)
                        if match:
                            user_id = match.group(1)
                            user_info["用户唯一id"] = user_id
                        try:
                            # 访问作者主页
                            driver.get(profile_url)
                            time.sleep(3)
                            profile_soup = BeautifulSoup(
                                driver.page_source, "html.parser"
                            )
                            script = profile_soup.body.find_all("script")[1].text
                            script = (
                                str(script)
                                .lstrip("window.__INITIAL_STATE__=")
                                .replace("undefined", "null")
                            )
                            data = json.loads(script)
                            user_page_data = data["user"]["userPageData"]
                            basic_info = user_page_data["basicInfo"]
                            interactions = user_page_data["interactions"]
                            # 提取用户信息
                            user_info["用户名"] = basic_info["nickname"]
                            user_info["用户ID"] = basic_info["redId"]
                            for item in interactions:
                                if item["name"] == "粉丝":
                                    user_info["粉丝量"] = item["count"]
                            logging.info(
                                f"用户名: {user_info['用户名']}, 用户ID: {user_info['用户ID']}, 粉丝量: {user_info['粉丝量']}"
                            )

                        except Exception as e:
                            logging.error(
                                f"抓取主页信息时发生错误: {profile_url}, 错误: {e}"
                            )
                    else:
                        logging.warning(f"在笔记页面 {url} 未找到作者主页链接。")

            except TimeoutException:
                logging.error(f"访问链接超时: {url}")
                note_info["标题"] = "访问超时"
            except Exception as e:
                logging.error(f"处理链接 {url} 时发生未知错误: {e}")
                note_info["标题"] = "处理失败"

            # 合并笔记和用户信息，并添加到总列表
            all_notes_data.append({**note_info, **user_info})

        save_to_excel(all_notes_data, output_filename_prefix)

    finally:
        logging.info("所有任务完成，正在关闭浏览器...")
        if driver:
            driver.quit()


def main():
    """主函数，协调整个流程。"""
    urls_from_file = read_urls_from_file(CONFIG["urls_filename"])
    if not urls_from_file:
        logging.warning("没有读取到有效的URL，程序终止。")
    else:
        process_notes(
            urls_from_file,
            CONFIG["cookies_filename"],
            CONFIG["output_filename_prefix"],
            enable_screenshots=CONFIG["enable_screenshots"],
            enable_user_info=CONFIG["enable_user_info"],  # 传入新配置
            screenshots_dir=CONFIG["screenshots_dir"],
        )


if __name__ == "__main__":
    main()
