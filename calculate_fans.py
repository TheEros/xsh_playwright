import polars as pl

df = pl.read_excel("./test.xlsx")
df = df.select("小红书名称", "粉丝量", "点赞", "发布链接").rename(
    {"小红书名称": "name", "粉丝量": "fans", "点赞": "likes", "发布链接": "link"}
)

# 统计实际完成情况
total_posts = df.height  # 总发布篇数
wan_zan = df.filter(pl.col("likes") >= 10000).height  # 万赞篇数
qian_zan = df.filter(
    (pl.col("likes") >= 1000) & (pl.col("likes") < 10000)
).height  # 千赞篇数
bai_zan = df.filter(
    (pl.col("likes") >= 100) & (pl.col("likes") < 1000)
).height  # 百赞篇数

# 打印KPI完成情况
kpi_text = f"""
目前《游戏入侵》项目KPI已完成：
万赞8篇，实际完成{wan_zan}篇
千赞20篇，实际完成{qian_zan}篇
百赞80篇，实际完成{bai_zan}篇
发布400篇，实际完成{total_posts}篇
"""

print(kpi_text)
