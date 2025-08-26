import duckdb
import polars as pl

user_info_new_columns = {
    "提交者（自动）": "submitter",
    "提交时间（自动）": "submission_time",
    "小红书名称（必填）": "nickname",
    "粉丝量（必填）": "fans",
    "视频链接（必填）": "note_url",
    "点赞": "like_number",
    "收藏": "collect",
    "评论": "comment",
}

account_new_columns = {
    "小红书ID（必填）": "red_id",
    "云账户绑定姓名（必填）": "cloud_name",
    "云账户绑定电话（必填）": "cloud_phone",
    "云账户绑定银行卡号（必填）": "cloud_bank_number",
    "云账户绑定身份证号码（必填）": "cloud_id_number",
    "小红书账号（有几个账号就需要填几份登记链接）（必填）": "nickname",
}

account_rs = (
    pl.read_csv("./account.csv")
    # 获取最新上传的小红书id的数据
    .with_columns(
        row_number=pl.struct([pl.col("提交时间（自动）")])
        .rank(method="ordinal", descending=True)
        .over("小红书ID（必填）")
    )
    .filter(pl.col("row_number") == 1)
    .select(
        [
            "小红书ID（必填）",
            "云账户绑定姓名（必填）",
            "云账户绑定电话（必填）",
            "云账户绑定银行卡号（必填）",
            "云账户绑定身份证号码（必填）",
            "小红书账号（有几个账号就需要填几份登记链接）（必填）",
        ]
    )
    .rename(account_new_columns)
)

user_info_rs = (
    pl.read_csv("./user_info.csv")
    .filter(pl.col("视频链接（必填）").is_not_null())
    .select(
        [
            "提交者（自动）",
            "提交时间（自动）",
            "小红书名称（必填）",
            "粉丝量（必填）",
            "视频链接（必填）",
            "点赞",
            "收藏",
            "评论",
        ]
    )
    .rename(user_info_new_columns)
)

user_info_sql = """
select submitter, submission_time, user_info.nickname, fans, note_url, like_number, collect, comment,row_number,
case when fans >= 10 and row_number = 1 then 500
     when fans >= 5 and fans < 10 and row_number = 1 then 200
     when fans >= 1 and fans < 5 and row_number = 1 then 100
     when fans >= 0.5 and fans < 1 and row_number = 1 then 50
     when fans >= 0.1 and fans < 0.5 and row_number = 1 then 30
     when fans < 0.1 and row_number = 1 then 20
     else 0 end as base_reward,
case when like_number >= 10000 then 300
     when like_number >=1000 and like_number <10000 then 50
     else 0 end as amount_of_reward,
if(row_number != 1,'稿费只结算一条','') as remark,
cloud_name, cloud_phone, cloud_bank_number, cloud_id_number
from
(select
submitter, submission_time, nickname, fans, note_url, like_number, collect, comment, row_number() over(partition by nickname order by like_number desc) as row_number,
from (select
submitter, submission_time, nickname, fans, note_url, cast(trim(like_number) as int) as like_number, collect, comment
from user_info_rs) as tmp ) as user_info
left join account_rs on user_info.nickname = account_rs.nickname
order by user_info.nickname asc, like_number desc
"""

duckdb.sql(user_info_sql).pl().write_excel(
    "result.xlsx",
)
