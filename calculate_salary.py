import polars as pl
import duckdb
import argparse
import sys

# --- CONFIGURATION ---
# Change all settings here.
CONFIG = {
    # --- DYNAMIC REWARD RULES ---
    # Modify the thresholds and rewards here.
    # IMPORTANT: Tiers must be ordered from HIGHEST threshold to LOWEST.
    "REWARD_RULES": {
        "base_reward_tiers": [
            # Based on fans (in 万)
            {"threshold": 10, "reward": 500},
            {"threshold": 5, "reward": 200},
            {"threshold": 1, "reward": 100},
            {"threshold": 0.5, "reward": 50},
            {"threshold": 0.1, "reward": 30},
            # The final tier for fans < 0.1 (represented by >= 0)
            {"threshold": 0, "reward": 20},
        ],
        "amount_of_reward_tiers": [
            # Based on likes
            {"threshold": 10000, "reward": 300},
            {"threshold": 1000, "reward": 50},
        ],
    },
}

# --- Column Mappings (Less likely to change) ---

USER_INFO_COLUMNS_MAP = {
    "提交者（自动）": "submitter",
    "提交时间（自动）": "submission_time",
    "小红书名称（必填）": "nickname",
    "粉丝量（必填）": "fans",
    "视频链接（必填）": "note_url",
    "点赞": "like_number",
    "收藏": "collect",
    "评论": "comment",
}

ACCOUNT_COLUMNS_MAP = {
    "小红书ID（必填）": "red_id",
    "云账户绑定姓名（必填）": "cloud_name",
    "云账户绑定电话（必填）": "cloud_phone",
    "云账户绑定银行卡号（必填）": "cloud_bank_number",
    "云账户绑定身份证号码（必填）": "cloud_id_number",
    "小红书账号（有几个账号就需要填几份登记链接）（必填）": "nickname",
}
FINAL_OUTPUT_COLUMNS_MAP = {
    "submitter": "提交者",
    "submission_time": "提交时间",
    "nickname": "小红书名称",
    "fans": "粉丝量（万）",
    "note_url": "视频链接",
    "like_number": "点赞数",
    "collect": "收藏数",
    "comment": "评论数",
    "row_number": "稿件排名（按点赞）",
    "base_reward": "基础稿费",
    "amount_of_reward": "爆款奖励",
    "remark": "备注",
    "cloud_name": "云账户姓名",
    "cloud_phone": "云账户电话",
    "cloud_bank_number": "云账户银行卡号",
    "cloud_id_number": "云账户身份证号",
}


def generate_case_statement(
    tiers: list[dict], column_name: str, alias: str, extra_condition: str = None
) -> str:
    """
    Dynamically generates a SQL CASE...WHEN statement from a list of reward tiers.
    """
    when_clauses = []
    for tier in tiers:
        condition = f"WHEN {column_name} >= {tier['threshold']}"
        if extra_condition:
            condition += f" AND {extra_condition}"
        when_clauses.append(f"{condition} THEN {tier['reward']}")

    return f"""CASE
            {chr(10).join(when_clauses)}
            ELSE 0
        END AS {alias}"""


def load_and_prepare_user_info(file_path: str) -> pl.DataFrame:
    print(f"正在加载用户信息文件: {file_path}")
    required_cols = list(USER_INFO_COLUMNS_MAP.keys())
    try:
        df = pl.read_csv(file_path)
    except FileNotFoundError:
        print(f"错误: 文件未找到 '{file_path}'")

        sys.exit(1)

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"错误: 用户信息文件 '{file_path}' 缺少以下必需列: {missing_cols}")
        sys.exit(1)
    return (
        df.select(required_cols)
        .rename(USER_INFO_COLUMNS_MAP)
        .filter(pl.col("note_url").is_not_null())
        .with_columns(
            pl.col("fans").cast(pl.Float64, strict=False).fill_null(0).alias("fans"),
            pl.col("like_number")
            .str.extract(r"(\d+)", 1)
            .cast(pl.Int64, strict=False)
            .fill_null(0)
            .alias("like_number"),
        )
    )


def load_and_prepare_account_data(file_path: str) -> pl.DataFrame:
    print(f"正在加载账户信息文件: {file_path}")
    required_cols = list(ACCOUNT_COLUMNS_MAP.keys())
    try:
        df = pl.read_csv(file_path)
    except FileNotFoundError:
        print(f"错误: 文件未找到 '{file_path}'")
        sys.exit(1)
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"错误: 账户文件 '{file_path}' 缺少以下必需列: {missing_cols}")
        sys.exit(1)
    return (
        df.with_columns(
            row_num=pl.col("提交时间（自动）")
            .rank(method="ordinal", descending=True)
            .over("小红书ID（必填）")
        )
        .filter(pl.col("row_num") == 1)
        .drop("row_num")
        .select(required_cols)
        .rename(ACCOUNT_COLUMNS_MAP)
    )


def calculate_rewards(
    user_info_df: pl.DataFrame, account_df: pl.DataFrame
) -> pl.DataFrame:
    print("正在计算稿费和奖励...")
    con = duckdb.connect(database=":memory:", read_only=False)
    con.register("user_info_view", user_info_df)
    con.register("account_view", account_df)

    base_reward_sql = generate_case_statement(
        tiers=CONFIG["REWARD_RULES"]["base_reward_tiers"],
        column_name="ui.fans",
        alias="base_reward",
        extra_condition="ui.row_number = 1",
    )

    amount_of_reward_sql = generate_case_statement(
        tiers=CONFIG["REWARD_RULES"]["amount_of_reward_tiers"],
        column_name="ui.like_number",
        alias="amount_of_reward",
    )

    query = f"""
    WITH RankedUserInfo AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY nickname ORDER BY like_number DESC) as row_number
        FROM user_info_view
    )
    SELECT
        ui.submitter, ui.submission_time, ui.nickname, ui.fans, ui.note_url,
        ui.like_number, ui.collect, ui.comment, ui.row_number,
        {base_reward_sql},
        {amount_of_reward_sql},
        IF(ui.row_number != 1, '稿费只结算最高点赞的一条', '') AS remark,
        acc.cloud_name, acc.cloud_phone, acc.cloud_bank_number, acc.cloud_id_number
    FROM RankedUserInfo AS ui
    LEFT JOIN account_view AS acc ON ui.nickname = acc.nickname
    ORDER BY ui.nickname ASC, ui.like_number DESC
    """
    print(query)

    result_df = con.execute(query).pl()
    con.close()
    return result_df


def main(user_info_path: str, account_path: str, output_path: str):
    user_info_df = load_and_prepare_user_info(user_info_path)
    account_df = load_and_prepare_account_data(account_path)

    if user_info_df.is_empty():
        print("警告: 用户信息文件中没有有效数据，程序将退出。")
        return

    final_result_df = calculate_rewards(user_info_df, account_df)
    final_result_df = final_result_df.rename(FINAL_OUTPUT_COLUMNS_MAP)

    try:
        print(f"正在保存结果到: {output_path}")
        final_result_df.write_excel(output_path)
        print("文件保存成功！")
    except Exception as e:
        print(f"错误: 保存Excel文件失败。原因: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="小红书稿费计算脚本")
    parser.add_argument(
        "user_info_file",
        nargs="?",
        default="./user_info.csv",
        help="用户信息CSV文件的路径 (默认为: ./user_info.csv)",
    )
    parser.add_argument(
        "--account_file",
        default="./account.csv",
        help="账户信息CSV文件的路径 (默认为: ./account.csv)",
    )
    parser.add_argument(
        "--output_file",
        default="result.xlsx",
        help="输出的Excel文件名 (默认为: result.xlsx)",
    )
    args = parser.parse_args()
    main(
        user_info_path=args.user_info_file,
        account_path=args.account_file,
        output_path=args.output_file,
    )
