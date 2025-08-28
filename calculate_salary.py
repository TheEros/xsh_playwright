import argparse
import sys
from pathlib import Path


import duckdb
import polars as pl

# ====== CONFIG（阈值从高到低；顺序不对会自动排序）======
CONFIG = {
    "REWARD_RULES": {
        # 机构内部价格（示例：请按实际调整）
        "base_reward_tiers_internal": [
            {"threshold": 10, "reward": 500},
            {"threshold": 5, "reward": 200},
            {"threshold": 1, "reward": 100},
            {"threshold": 0.5, "reward": 50},
            {"threshold": 0.1, "reward": 30},
            {"threshold": 0, "reward": 20},
        ],
        # 外部价格（示例：请按实际调整）
        "base_reward_tiers_external": [
            {"threshold": 10, "reward": 500},
            {"threshold": 5, "reward": 200},
            {"threshold": 1, "reward": 100},
            {"threshold": 0.5, "reward": 50},
            {"threshold": 0.1, "reward": 30},
            {"threshold": 0, "reward": 20},
        ],
        # 爆款奖励（示例）
        "amount_of_reward_tiers": [
            {"threshold": 10000, "reward": 300},
            {"threshold": 1000, "reward": 50},
        ],
    }
}


# ====== 列映射 ======
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

# 公司表常见列名（任选其一）
COMPANY_NICK_CANDIDATES = ["nickname", "小红书账号", "小红书名称", "账号昵称", "昵称"]
COMPANY_ISORG_CANDIDATES = [
    "is_institution",
    "是否机构",
    "是否为机构账号",
    "机构",
    "是否机构账号",
]


# ====== 基础工具 ======
def _read_csv_or_exit(path: str) -> pl.DataFrame:
    p = Path(path)
    if not p.exists():
        print(f"错误: 文件未找到 '{path}'")

        sys.exit(1)

    for enc in ("utf8", "utf8-lossy"):
        try:
            return pl.read_csv(path, encoding=enc, infer_schema_length=10000)
        except Exception:
            pass
    print(f"错误: 无法解析 CSV 文件（尝试过 utf8 / utf8-lossy）: '{path}'")
    sys.exit(1)


def _ensure_columns(df: pl.DataFrame, required_raw: list[str], file_hint: str):
    miss = [c for c in required_raw if c not in df.columns]
    if miss:
        print(f"错误: 文件 '{file_hint}' 缺少必需列: {miss}")
        sys.exit(1)


def _num_clean(col: str, to_type) -> pl.Expr:
    # 清洗数值："1.2万"、"10,000"、" 500 " -> 纯数字
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .str.replace_all(r"[^\d.\-]", "")
        .cast(to_type, strict=False)
        .fill_null(0)
    )


def _pick_first_col(df: pl.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _bool01_expr(col: str) -> pl.Expr:
    truthy = {"1", "true", "y", "yes", "是", "机构", "TRUE", "Y", "YES"}
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("0")
        .map_elements(lambda x: 1 if x in truthy else 0)
        .cast(pl.Int8)
    )


def _generate_case_statement(
    tiers: list[dict], column: str, alias: str, extra: str | None = None
) -> str:
    tiers = sorted(tiers, key=lambda x: float(x["threshold"]), reverse=True)
    parts = []
    for t in tiers:
        cond = f"when {column} >= {t['threshold']}"

        if extra:
            cond += f" and {extra}"

        parts.append(f"{cond} then {t['reward']}")
    return f"case {' '.join(parts)} else 0 end as {alias}"


# ====== 数据准备 ======


def load_user_info(path: str) -> pl.DataFrame:
    print(f"正在加载用户信息文件: {path}")
    raw = _read_csv_or_exit(path)
    required = list(USER_INFO_COLUMNS_MAP.keys())
    _ensure_columns(raw, required, path)
    df = (
        raw.select(required)
        .rename(USER_INFO_COLUMNS_MAP)
        .filter(
            pl.col("note_url").is_not_null()
            & (pl.col("note_url").cast(pl.Utf8, strict=False).str.len_chars() > 0)
        )
        .with_columns(
            fans=_num_clean("fans", pl.Float64),
            like_number=_num_clean("like_number", pl.Int64),
            collect=_num_clean("collect", pl.Int64),
            comment=_num_clean("comment", pl.Int64),
        )
    )
    if df.is_empty():
        print("警告: 用户信息文件中没有有效数据。")
    return df


def load_account(path: str) -> pl.DataFrame:
    print(f"正在加载账户信息文件: {path}")
    raw = _read_csv_or_exit(path)
    required = list(ACCOUNT_COLUMNS_MAP.keys())
    _ensure_columns(raw, required, path)
    return (
        raw.with_columns(
            _ts=pl.col("提交时间（自动）")
            .cast(pl.Utf8, strict=False)
            .str.to_datetime(strict=False)
        )
        .with_columns(
            row_num=pl.col("_ts")
            .rank(method="ordinal", descending=True)
            .over("小红书ID（必填）")
        )
        .filter(pl.col("row_num") == 1)
        .drop(["row_num", "_ts"])
        .select(required)
        .rename(ACCOUNT_COLUMNS_MAP)
    )


def load_company(path: str, price_mode: str) -> tuple[pl.DataFrame, int]:
    """
    返回: (公司表DataFrame, default_org_flag)
    default_org_flag: 当无法使用公司表时，both 模式下默认是否机构:
      - 1 = 默认机构（使用内部价）
      - 0 = 默认非机构（使用外部价）

    """
    p = Path(path)
    if not p.exists():
        if price_mode == "both":
            print(
                f"提示: 未找到公司表 '{path}'，both 模式下将默认所有账号为机构（使用内部价格）。"
            )
        return pl.DataFrame({"nickname": [], "is_institution": []}), 1

    raw = _read_csv_or_exit(path)
    nick_col = _pick_first_col(raw, COMPANY_NICK_CANDIDATES)
    org_col = _pick_first_col(raw, COMPANY_ISORG_CANDIDATES)

    if nick_col is None or org_col is None:
        if price_mode == "both":
            print(
                f"提示: 公司表缺少列（昵称列={nick_col}，机构列={org_col}），both 模式下将默认所有账号为机构（使用内部价格）。"
            )
        return pl.DataFrame({"nickname": [], "is_institution": []}), 1

    df = (
        raw.select([nick_col, org_col])
        .rename({nick_col: "nickname", org_col: "is_institution"})
        .with_columns(is_institution=_bool01_expr("is_institution"))
        .with_columns(nickname=pl.col("nickname").cast(pl.Utf8, strict=False))
        .unique(subset=["nickname"], keep="any")
    )
    # 公司表可用时，默认回退设为“非机构=0”
    return df, 0


# ====== 计算 ======
def calculate(
    user_df: pl.DataFrame,
    acc_df: pl.DataFrame,
    comp_df: pl.DataFrame,
    price_mode: str,
    default_org_flag: int,
) -> pl.DataFrame:
    print("正在计算稿费和奖励...")
    con = duckdb.connect(database=":memory:")
    con.register("user_info_view", user_df)

    con.register("account_view", acc_df)
    con.register("company_view", comp_df)

    base_internal_sql = _generate_case_statement(
        CONFIG["REWARD_RULES"]["base_reward_tiers_internal"],
        "ui.fans",
        "base_reward_internal",
        extra="ui.row_number = 1",
    )
    base_external_sql = _generate_case_statement(
        CONFIG["REWARD_RULES"]["base_reward_tiers_external"],
        "ui.fans",
        "base_reward_external",
        extra="ui.row_number = 1",
    )

    boom_sql = _generate_case_statement(
        CONFIG["REWARD_RULES"]["amount_of_reward_tiers"],
        "ui.like_number",
        "amount_of_reward",
    )

    sql = f"""
    with ranked as (
      select *, row_number() over (partition by nickname order by like_number desc) as row_number
      from user_info_view
    )
    select
      ui.submitter, ui.submission_time, ui.nickname, ui.fans, ui.note_url,

      ui.like_number, ui.collect, ui.comment, ui.row_number,
      {base_internal_sql},
      {base_external_sql},
      case
        when '{price_mode}' = 'internal' then base_reward_internal
        when '{price_mode}' = 'external' then base_reward_external
        when coalesce(comp.is_institution, {default_org_flag}) = 1 then base_reward_internal
        else base_reward_external
      end as base_reward,
      {boom_sql},
      case when ui.row_number != 1 then '稿费只结算最高点赞的一条' else '' end as remark,
      acc.cloud_name, acc.cloud_phone, acc.cloud_bank_number, acc.cloud_id_number
    from ranked ui
    left join account_view acc using (nickname)
    left join company_view comp using (nickname)
    order by ui.nickname asc, ui.like_number desc
    """
    print(sql)
    out = con.execute(sql).pl().drop(["base_reward_internal", "base_reward_external"])
    con.close()
    return out


def save_result(df: pl.DataFrame, out_path: str):
    out = Path(out_path)
    try:
        print(f"正在保存结果到: {out}")

        df.write_excel(str(out))

        print("文件保存成功！")
    except Exception as e:
        alt = out.with_suffix(".csv")
        df.write_csv(alt)
        print(f"Excel 写入失败（{e}），已改存为 CSV: {alt}")


# ====== CLI ======
def main(
    user_info_path: str,
    account_path: str,
    company_path: str,
    output_path: str,
    price_mode: str,
):
    user_df = load_user_info(user_info_path)
    if user_df.is_empty():
        print("没有可计算的数据，程序结束。")
        return
    acc_df = load_account(account_path)
    comp_df, default_org_flag = load_company(company_path, price_mode)
    result = calculate(user_df, acc_df, comp_df, price_mode, default_org_flag).rename(
        FINAL_OUTPUT_COLUMNS_MAP
    )
    save_result(result, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="小红书稿费计算脚本（DuckDB + Polars）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "user_info_file", nargs="?", default="./user_info.csv", help="用户信息 CSV 路径"
    )
    parser.add_argument(
        "--account_file", default="./account.csv", help="账户信息 CSV 路径"
    )
    parser.add_argument(
        "--company_file",
        default="./company.csv",
        help="公司信息 CSV 路径（含是否机构）",
    )

    parser.add_argument(
        "--price_mode",
        choices=["internal", "external", "both"],
        default="both",
        help="价格模式：internal=全部机构价，external=全部外部价，both=机构用内部价、非机构用外部价（公司表缺失时默认全部内部价）",
    )
    parser.add_argument(
        "--output_file",
        default="result.xlsx",
        help="输出文件名（优先写 Excel，失败回落为 CSV）",
    )
    args = parser.parse_args()
    main(
        args.user_info_file,
        args.account_file,
        args.company_file,
        args.output_file,
        args.price_mode,
    )
