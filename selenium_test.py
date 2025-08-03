import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup


def screenshot_note_with_cookies(note_url):
    # 配置浏览器选项
    chrome_options = Options()
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

        for i, url in enumerate(note_url):
            # 访问目标笔记页面
            driver.get(url)
            time.sleep(2)  # 等待页面加载完成
            # 截图保存
            screenshot_path = "./screenshots/{}_note_screenshot.png".format(i)
            driver.save_screenshot(screenshot_path)
            print(f"截图已保存到: {screenshot_path}")
            page_source = driver.page_source

            # 使用BeautifulSoup解析
            soup = BeautifulSoup(page_source, "html.parser")
            if soup.find("title").text == "小红书 - 你访问的页面不见了":
                print(f"无法访问: {url}")
                break
            title = soup.find("meta", attrs={"name": "og:title"})["content"]
            note_comment = soup.find("meta", attrs={"name": "og:xhs:note_comment"})[
                "content"
            ]
            note_like = soup.find("meta", attrs={"name": "og:xhs:note_like"})["content"]
            note_collect = soup.find("meta", attrs={"name": "og:xhs:note_collect"})[
                "content"
            ]
            print(title, url, note_like, note_collect, note_comment)

    finally:
        driver.quit()


# 使用示例
note_urls = [
    "http://xhslink.com/m/AYKY5UKt5JM",
]  # 替换为实际笔记URL
screenshot_note_with_cookies(note_urls)
