import time
import datetime
import os
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
    df = pd.DataFrame(data_list, columns=["标题", "链接", "点赞数", "收藏数", "评论数"])
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


def screenshot_note_with_cookies(note_url):
    # 配置浏览器选项
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")  # 最大化窗口

    # 初始化浏览器驱动
    driver = webdriver.Chrome(options=chrome_options)

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

        for i, url in enumerate(note_url):
            # 访问目标笔记页面
            driver.get(url)
            # time.sleep(2)  # 等待页面加载完成
            # 截图保存
            # screenshot_path = "./screenshots/{}_note_screenshot.png".format(i)
            # driver.save_screenshot(screenshot_path)
            # print(f"截图已保存到: {screenshot_path}")
            page_source = driver.page_source

            # 使用BeautifulSoup解析
            soup = BeautifulSoup(page_source, "html.parser")
            if soup.find("meta", attrs={"name": "og:title"}) is None:
                print("无法访问", url, 0, 0, 0)
                all_user_data.append(("无法访问", url, 0, 0, 0))
                continue
            title = soup.find("meta", attrs={"name": "og:title"})["content"]
            note_comment = soup.find("meta", attrs={"name": "og:xhs:note_comment"})[
                "content"
            ]
            note_like = soup.find("meta", attrs={"name": "og:xhs:note_like"})["content"]
            note_collect = soup.find("meta", attrs={"name": "og:xhs:note_collect"})[
                "content"
            ]
            print(title, url, note_like, note_collect, note_comment)
            all_user_data.append((title, url, note_like, note_collect, note_comment))

        save_to_excel(all_user_data, "xiaohongshu_notes")
    finally:
        driver.quit()


def extract_urls(text):
    """从文本中提取所有URL"""
    pattern = r"https?://[^\s]+"
    urls = re.findall(pattern, text)[0].strip()
    return urls


if __name__ == "__main__":
    # 从文件中读取链接
    file_name = "urls.txt"  # 你的URL文件路径
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
        screenshot_note_with_cookies(notes_list)
