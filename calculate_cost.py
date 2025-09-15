import polars as pl
import duckdb

rc = pl.read_excel("test.xlsx")

rs = rc.select(["一类账号", "日期", "点评藏", "加热费用", "账号消费总金额"]).rename(
    {
        "点评藏": "likes",
        "一类账号": "name",
        "加热费用": "cost",
        "日期": "date",
        "账号消费总金额": "total_cost",
    }
)
duckdb.sql("""
select date, name, '点评藏' as type,likes from rs where likes is not null
union all
select date, name,'达人费用' as type, cost from rs where cost is not null and total_cost is not null
union all
select date, name,'加热费用' as type, cost from rs where cost is not null and total_cost is null
order by date, name, type
""").pl().write_excel("result.xlsx")
