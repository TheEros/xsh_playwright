from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time


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

    finally:
        driver.quit()


# 使用示例
note_urls = [
    "http://xhslink.com/m/8Y3ukK2cahd",
    "http://xhslink.com/m/5DiLqjqyT8u",
    "http://xhslink.com/m/3cTzDKWiBEt",
    "http://xhslink.com/m/8DawsZPEqAN",
    "http://xhslink.com/m/7B4ze9YPBXS",
    "http://xhslink.com/m/1AM72qGlPat",
    "http://xhslink.com/m/12cQ9koePfn",
    "http://xhslink.com/m/4NI9aJFNxho",
    "http://xhslink.com/m/54TtcvuHDq3",
    "http://xhslink.com/m/3oDa3n4jr2U",
    "http://xhslink.com/m/7f093d9HzUv",
    "http://xhslink.com/m/AZ9P8f3YDCn",
    "http://xhslink.com/m/5ak2mTjNtBE",
    "http://xhslink.com/m/AYoqq4Qe80q",
    "http://xhslink.com/m/7elaW6cQSFf",
    "http://xhslink.com/m/4qa9RwBf9vO",
    "http://xhslink.com/m/7LpmTyea5H7",
    "http://xhslink.com/m/6aAU8pV9RkX",
    "http://xhslink.com/m/9YKgADzThbf",
    "https://www.xiaohongshu.com/discovery/item/688ddf5d000000002303b8dd?app_platform=android&ignoreEngage=true&app_version=8.91.4&share_from_user_hidden=true&xsec_source=app_share&type=video&xsec_token=CBVlx-ZtVyMHDebKo9UD9TZld0qSu1vvG4CcYVJ4FFepY%3D&author_share=1&xhsshare=CopyLink&shareRedId=ODw5QTw6PUE2NzUyOTgwNjZHOTk6OkdB&apptime=1754176575&share_id=3baee04c0bc244ab857d77c13052bb3f&share_channel=copy_link",
    "http://xhslink.com/m/fjL8w53m73",
    "http://xhslink.com/m/xZe3QT1lRc",
    "http://xhslink.com/m/9tI8QnvlC2e",
    "http://xhslink.com/m/AYKY5UKt5JM",
    "http://xhslink.com/m/1UkGcnOPynF",
    "http://xhslink.com/m/AxXTSOG3Ptr",
    "http://xhslink.com/m/7AFl33ccXgq",
    "http://xhslink.com/m/9u9Dcf4zPU4",
    "http://xhslink.com/m/7TFJh54aifM",
    "http://xhslink.com/n/4nFwt7T0PJx",
    "http://xhslink.com/m/4OXjPturJPR",
    "http://xhslink.com/m/dEOovFBrjD",
    "http://xhslink.com/m/AsUhTlarupt",
    "http://xhslink.com/m/7vGru1SZwR7",
    "http://xhslink.com/m/95w9BmuZzAb",
    "http://xhslink.com/m/26DHbnXr647",
    "http://xhslink.com/m/3gsI4tzeVW6",
]  # 替换为实际笔记URL
screenshot_note_with_cookies(note_urls)
