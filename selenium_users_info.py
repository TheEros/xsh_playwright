import time
import datetime
import os
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re


def save_to_excel(data_list, filename):
    """保存数据到Excel文件"""
    if not data_list:
        print("没有有效数据可保存")
        return

    # 创建包含时间戳的文件名
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename}_{timestamp}.xlsx"
    df = pd.DataFrame(
        data_list,
        columns=[
            "小红书名称",
            "粉丝量",
            "标题",
            "keywords",
            "description链接",
            "url",
            "点赞数",
            "收藏数",
            "评论数",
        ],
    )
    df.to_excel(filename, index=False)  # Avoid saving index in Excel
    print(f"成功保存到: {os.path.abspath(filename)}")
    print(f"总记录数: {len(data_list)}")


def read_urls_from_file(filename):
    """从文件中读取URLs"""
    try:
        with open(filename, "r", encoding="utf-8") as file:  # Specify encoding here
            urls = [line.strip() for line in file.readlines() if line.strip()]
        return urls
    except FileNotFoundError:
        print(f"错误: 找不到文件 {filename}!")
        return []
    except UnicodeDecodeError:
        print(f"错误: 文件 {filename} 的编码无法解码，请确认文件编码为 UTF-8！")
        return []


def screenshot_note_with_cookies(users_url, start_time):
    # 配置浏览器选项
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")  # 最大化窗口
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    # 初始化浏览器驱动
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1024, 768)

    try:
        # 先访问小红书主页以设置域名
        driver.get("https://www.xiaohongshu.com")
        time.sleep(2)

        cookies = []
        # 加载保存的Cookies
        with open("cookies.txt", "r") as file:  # 替换为你保存 Cookie 的文件路径
            lines = file.readlines()
            for line in str(lines).split(";"):
                name, value = line.strip().split("=", 1)
                cookies.append(
                    {
                        "name": name,
                        "value": value,
                        "domain": ".xiaohongshu.com",
                        "path": "/",
                        "expires": -1,
                    }
                )

        # 添加每个Cookie
        for cookie in cookies:
            driver.add_cookie(cookie)

        # 刷新页面使Cookies生效
        driver.refresh()
        time.sleep(2)

        all_user_data = []
        for user_url in users_url:
            driver.get(user_url)
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            top_note = []
            for top_div in soup.find_all("div", class_="top-wrapper", string="置顶"):
                # 往上找到整个 note-item
                note_item = top_div.find_parent("section", class_="note-item")
                if note_item:
                    # 在这个 note-item 中找 <a> 的 href
                    a_tag = note_item.find("a", href=True)
                    top_note.append(
                        a_tag["href"].split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
                    )
            script = soup.body.find_all("script")[1].text
            script = (
                str(script)
                .lstrip("window.__INITIAL_STATE__=")
                .replace("undefined", "null")
            )
            data = json.loads(script)
            userPageData = data["user"]["userPageData"]
            nickname = userPageData["basicInfo"]["nickname"]
            interactions = userPageData["interactions"]
            # 创建一个生成器，它会“懒惰地”查找结果
            fan_count_generator = (
                item["count"] for item in interactions if item["type"] == "fans"
            )
            # next() 会从生成器中取出第一个元素，然后立即停止
            # 如果没有找到，提供一个默认值（比如 None 或 0）可以避免抛出 StopIteration 异常
            fan_count = next(fan_count_generator, 0)

            notes = data["user"]["notes"][0]
            # 3遍历 notes，排除置顶，取前6个
            count = 0
            for note in notes:
                if note["id"] in top_note:
                    continue
                note_url = "https://www.xiaohongshu.com/explore/{}?xsec_token={}"
                driver.get(note_url.format(note["id"], note["xsecToken"]))
                # time.sleep(2)  # 等待页面加载完成
                page_source = driver.page_source

                # 使用BeautifulSoup解析
                soup = BeautifulSoup(page_source, "html.parser")
                if soup.find("meta", attrs={"name": "og:title"}) is None:
                    print("无法访问", "", "", "", "", url, 0, 0, 0)
                    all_user_data.append(("无法访问", "", "", "", "", url, 0, 0, 0))
                    continue
                title = soup.find("meta", attrs={"name": "og:title"})["content"]
                keywords = soup.find("meta", attrs={"name": "keywords"})["content"]
                description = soup.find("meta", attrs={"name": "description"})[
                    "content"
                ]
                note_comment = soup.find("meta", attrs={"name": "og:xhs:note_comment"})[
                    "content"
                ]
                note_like = soup.find("meta", attrs={"name": "og:xhs:note_like"})[
                    "content"
                ]
                note_collect = soup.find("meta", attrs={"name": "og:xhs:note_collect"})[
                    "content"
                ]
                note_script = soup.body.find_all("script")[1].text
                note_script = (
                    str(note_script)
                    .lstrip("window.__INITIAL_STATE__=")
                    .replace("undefined", "null")
                )
                note_data = json.loads(note_script)
                note_crete_time = note_data["note"]["noteDetailMap"][note["id"]][
                    "note"
                ]["time"]

                count += 1
                if count >= 5 or note_crete_time < start_time:
                    if count == 1:
                        print("此账号从开始发布时间到现在还没有发布")
                    break
                print(
                    nickname,
                    fan_count,
                    title,
                    keywords,
                    description,
                    url,
                    note_like,
                    note_collect,
                    note_comment,
                )
                all_user_data.append(
                    (
                        nickname,
                        fan_count,
                        title,
                        keywords,
                        description,
                        url,
                        note_like,
                        note_collect,
                        note_comment,
                    )
                )
        save_to_excel(all_user_data, "xiaohongshu_notes")
    except Exception as e:
        print(e)
    finally:
        driver.quit()


def yyyymmdd_to_milliseconds(date_string):
    """
    Converts a date string in 'yyyymmdd' format to a millisecond timestamp.

    Args:
      date_string: The date string in 'yyyymmdd' format (e.g., "20231026").

    Returns:
      The integer millisecond timestamp corresponding to the start of that day (UTC).
      Returns None if the input format is invalid.
    """
    try:
        # 1. Parse the string into a datetime object.
        #    '%Y' = 4-digit year, '%m' = 2-digit month, '%d' = 2-digit day.
        dt_object = datetime.datetime.strptime(date_string, "%Y%m%d")

        # 2. Get the timestamp in seconds (as a float).
        timestamp_seconds = dt_object.timestamp()

        # 3. Convert to milliseconds and return as an integer.
        timestamp_milliseconds = int(timestamp_seconds * 1000)

        return timestamp_milliseconds
    except ValueError:
        print(f"Error: Invalid date format for '{date_string}'. Please use 'yyyymmdd'.")
        return None


def extract_urls(text):
    """从文本中提取所有URL"""
    pattern = r"https?://[^\s]+"
    urls = re.findall(pattern, text)[0].strip()
    return urls


if __name__ == "__main__":
    # todo 开始发布的日期
    start_date = "20250814"
    start_time = yyyymmdd_to_milliseconds(start_date)
    # 从文件中读取链接
    file_name = "user_urls.txt"  # 你的URL文件路径
    urls_from_file = read_urls_from_file(file_name)
    notes_list = []
    if not urls_from_file:
        print("没有读取到有效的URL，程序终止。")
    else:
        # 调用 extract_urls 提取每个文本中的URL
        for url in urls_from_file:
            rs_url = extract_urls(url)
            print(rs_url)
            notes_list.append(rs_url)

        # 调用 screenshot_note_with_cookies 处理URL文件
        screenshot_note_with_cookies(notes_list, start_time)
