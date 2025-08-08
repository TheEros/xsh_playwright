import time
import datetime
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup


def save_to_excel(data_list, filename):
    """保存数据到Excel文件"""
    if not data_list:
        print("没有有效数据可保存")
        return

    # 创建包含时间戳的文件名
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename}_{timestamp}.xlsx"
    df = pd.DataFrame(data_list, columns=["标题", "链接", "点赞数", "收藏数", "评论数"])
    df.to_excel(filename)
    print(f"成功保存到: {os.path.abspath(filename)}")
    print(f"总记录数: {len(data_list)}")


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


# 使用示例
note_urls = [
    "http://xhslink.com/n/7bDQnD9Fliq",
    "https://www.xiaohongshu.com/discovery/item/68930cc000000000230369ed?source=webshare&xhsshare=pc_web&xsec_token=YBMV5BJ_BDroHhfFohxSkqFmGhJChYq9dDFzNwtlSavrY=",
    "http://xhslink.com/m/2m61njrFuNT",
    "http://xhslink.com/m/1O0Af9JOK8V",
    "http://xhslink.com/m/8N4UAgRutU8",
    "http://xhslink.com/m/6WMFYJoK1lV",
]  # 替换为实际笔记URL
screenshot_note_with_cookies(note_urls)
