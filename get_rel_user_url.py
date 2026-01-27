import requests
import re


def extract_url(text):
    # 正则表达式匹配小红书短链
    pattern = r"https://xhslink\.com/\S+"
    match = re.search(pattern, text)
    if match:
        return match.group(0)  # 返回匹配到的链接
    else:
        return "未找到链接"


def get_redirect_url(short_url):
    try:
        # 添加请求头，模拟浏览器请求
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
        # 发送请求，禁止自动重定向
        response = requests.get(short_url, headers=headers, allow_redirects=False)
        # 检查响应状态码
        if response.status_code in [301, 302, 307]:  # 处理重定向状态码
            redirect_url = response.headers["Location"]  # 获取重定向链接
            return redirect_url
        else:
            return "无法获取重定向链接，状态码: {}".format(response.status_code)
    except Exception as e:
        return "请求失败: {}".format(str(e))

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

if __name__ == "__main__":
    text = "44 小猪吃宵夜发布了一篇小红书笔记，快来看吧！ 😆 KeA1GIGiSMXGWy7 😆 http://xhslink.com/a/sT7omKb6ijX6，复制本条信息，打开【小红书】App查看精彩内容！"
    file_name = "user_urls.txt"
    urls_from_file = read_urls_from_file(file_name)
    if not urls_from_file:
        print("没有读取到有效的URL，程序终止。")
    else:
        # 调用 extract_urls 提取每个文本中的URL
        for url in urls_from_file:
            short_url = extract_url(url)
            if short_url != "未找到链接":
                full_url = get_redirect_url(short_url)
                print(full_url)
            else:
                print(url)