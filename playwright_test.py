from playwright.sync_api import sync_playwright
import datetime
import os

# with sync_playwright() as playwright:
#     browser = playwright.chromium.launch(headless=False)
#     context = browser.new_context()

#     # 读取保存的 Cookie 文件
#     cookies = []
#     with open("cookies.txt", "r") as file:  # 替换为你保存 Cookie 的文件路径
#         lines = file.readlines()
#         for line in str(lines).split(";"):
#             name, value = line.strip().split("=", 1)
#             cookies.append(
#                 {
#                     "name": name,
#                     "value": value,
#                     "domain": ".xiaohongshu.com",
#                     "path": "/",
#                     "expires": -1,
#                 }
#             )

#     # 添加 Cookie 到浏览器上下文
#     context.add_cookies(cookies)

#     # 创建一个新页面，并访问小红书探索页面
#     page = context.new_page()
#     page.goto(
#         "https://www.xiaohongshu.com/explore/688c5f580000000005005308?xsec_token=AB8UaH81cgBcc_Lp0W52DiCtlJLw1HO9ORNB9K3ll546k=&xsec_source=pc_feed"
#     )

#     # 获取页面标题并打印出来
#     title = page.title()
#     print("页面标题:", title)

#     page.screenshot(path="screenshot.png")
#     browser.close()


def process_urls(url_list, cookies_path="cookies.txt", output_dir="screenshots"):
    """
    批量处理多个URL，使用保存的Cookies登录并截图

    :param url_list: 要处理的URL列表
    :param cookies_path: 保存Cookies的文件路径
    :param output_dir: 截图保存目录
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)

        # 创建浏览器上下文并加载Cookies
        context = load_cookies_to_context(browser, cookies_path)

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        for i, url in enumerate(url_list):
            try:
                # 创建新页面并设置超时
                page = context.new_page()
                page.set_default_timeout(60000)  # 设置60秒超时

                print(f"正在处理 URL {i + 1}/{len(url_list)}: {url}")

                # 导航到目标页面
                page.goto(url)

                # 获取页面信息
                title = page.title()
                current_url = page.url
                print(f"页面标题: {title}")
                print(f"实际访问URL: {current_url}")

                # 生成唯一截图文件名
                domain = current_url.split("//")[-1].split("/")[0].replace(".", "_")
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = os.path.join(output_dir, f"{domain}_{timestamp}.png")

                # 页面截图
                page.screenshot(path=screenshot_path)
                print(f"截图已保存: {screenshot_path}")

            except Exception as e:
                print(f"处理URL失败: {url}, 错误: {str(e)}")
            finally:
                # 确保页面关闭
                if "page" in locals():
                    page.close()

        browser.close()


def load_cookies_to_context(browser, cookies_path):
    """
    从文件加载Cookies到浏览器上下文

    :param browser: 浏览器实例
    :param cookies_path: Cookies文件路径
    :return: 已加载Cookies的浏览器上下文
    """
    context = browser.new_context()

    try:
        cookies = []
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

        # 添加 Cookie 到浏览器上下文
        context.add_cookies(cookies)
    except Exception as e:
        print(f"加载Cookies失败: {str(e)}")
        exit(-1)

    return context


if __name__ == "__main__":
    # 示例URL列表（替换为实际需要处理的URL）
    urls_to_process = [
        # "https://www.xiaohongshu.com/explore/6889aa8c000000002502114d?xsec_token=AB3ss2YUzl46ayVUffZTFQ4SetpjXK4YSktFwo1KMKcj8=&xsec_source=pc_feed",
        "http://xhslink.com/m/82pzLGyT6bu",
        "http://xhslink.com/m/82pzLGyT6bu",
        # 添加更多URL...
    ]

    process_urls(urls_to_process)
