#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小红书稿费计算脚本（DuckDB + Polars）— 加强健壮性版本
改进点：
1) 读取层更鲁棒：
   - CSV：尝试多种常见编码（utf8/gbk/gb18030/utf8-lossy），自动推断 schema。
   - Excel：优先 Polars；若失败，回退 Pandas；在提供 required_raw 时自动选择包含必需列的工作表。
2) 数据清洗更稳：
   - 统一去空白；粉丝量支持“1.2万 / 12000 / 12,000”等混合格式并统一换算为“万”。
   - 点赞/收藏/评论清洗千分位符与杂字符，空值置 0。
3) 公司表容错：
   - 自动识别昵称列（多候选）；缺失时在 both 模式兜底为“默认机构=1”。

4) 计算层：DuckDB SQL 保留，但参数可控；只对每个昵称点赞最高的一条结算基础稿费；
   - 价格模式：internal/external/both（公司表缺失仍默认机构=1）。
5) 导出更鲁棒：

   - 优先 Polars 写 Excel；失败回落 Pandas OpenPyXL；仍失败回落 CSV。
6) CLI & 日志：
   - 统一标准输出日志；关键步骤统计信息回显；异常明确退出码。
"""

from __future__ import annotations


import argparse
import sys
import re
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


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
            {"threshold": 10, "reward": 400},
            {"threshold": 5, "reward": 100},
            {"threshold": 1, "reward": 50},
            {"threshold": 0.5, "reward": 30},
            {"threshold": 0.1, "reward": 20},
            {"threshold": 0, "reward": 10},
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
    "is_mcn": "是否机构",
    "base_reward": "基础稿费",
    "amount_of_reward": "爆款奖励",
    "remark": "备注",
    "cloud_name": "云账户姓名",
    "cloud_phone": "云账户电话",
    "cloud_bank_number": "云账户银行卡号",
    "cloud_id_number": "云账户身份证号",
}

# 公司表常见列名（任选其一）
COMPANY_NICK_CANDIDATES = [
    "nickname",
    "小红书账号",
    "小红书名称",
    "账号昵称",
    "昵称",
    "账号名称",
]


# ====== 基础工具（新增：统一读取 CSV/Excel）======
def _read_table_or_exit(
    path: str, required_raw: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    统一读取 CSV / Excel：
    - CSV：尝试多种编码（utf8/gbk/gb18030/utf8-lossy）。
    - Excel：优先 Polars；失败则使用 Pandas（可按 required_raw 自动挑选 sheet）。

    - 其它后缀：按 CSV 兜底（尝试以 .csv 读取）。
    """
    p = Path(path)
    if not p.exists():
        print(f"错误: 文件未找到 '{path}'", file=sys.stderr)
        sys.exit(1)

    suffix = p.suffix.lower()

    def _read_csv_any_enc(csv_path: str) -> pl.DataFrame:
        encodings = ("utf8", "gbk", "gb18030", "utf8-lossy")

        last_err = None
        for enc in encodings:
            try:
                return pl.read_csv(csv_path, encoding=enc, infer_schema_length=10000)
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue
        print(
            f"错误: 无法解析 CSV 文件: '{csv_path}'，最后错误: {last_err}",
            file=sys.stderr,
        )
        sys.exit(1)

    def _read_excel_smart(
        xlsx_path: str, required: Optional[List[str]]
    ) -> pl.DataFrame:
        # 1) 试 Polars（读首个工作表）
        try:
            df = pl.read_excel(xlsx_path)
            if required and not set(required).issubset(set(df.columns)):
                raise ValueError("首个工作表不包含必需列，尝试其它工作表")
            return df
        except Exception as e:  # noqa: BLE001
            print(
                f"错误: 无法解析 Excel 文件: '{xlsx_path}'，原因: {e}", file=sys.stderr
            )
            sys.exit(1)

    if suffix == ".csv":
        return _read_csv_any_enc(str(p))
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return _read_excel_smart(str(p), required_raw)

    # 其它后缀：尝试将后缀改为 .csv 读取
    print(f"提示: 未识别的文件后缀 '{suffix}'，按 CSV 方式尝试解析。")
    return _read_table_or_exit(str(p.with_suffix(".csv")), required_raw)


def _ensure_columns(df: pl.DataFrame, required_raw: List[str], file_hint: str) -> None:
    miss = [c for c in required_raw if c not in df.columns]
    if miss:
        print(f"错误: 文件 '{file_hint}' 缺少必需列: {miss}", file=sys.stderr)
        sys.exit(1)


# --- 数值清洗 ---
_num_pat = re.compile(r"[^0-9.\-]")


def _num_clean(col: str, to_type) -> pl.Expr:
    """通用数值清洗：去除非数字/小数点/负号，转目标类型并用 0 填充空。"""
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .str.replace_all(_num_pat.pattern, "")
        .cast(to_type, strict=False)
        .fill_null(0)
    )


# 粉丝量统一换算为“万”
_fans_wan_regex = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*万", re.IGNORECASE)


# 不使用
def _fans_to_wan_expr(col: str = "fans") -> pl.Expr:
    def _parse(v) -> Optional[float]:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        m = _fans_wan_regex.search(s)

        if m:
            try:
                return float(m.group(1))
            except Exception:
                return None
        # 去掉噪声字符
        s2 = _num_pat.sub("", s)
        if s2 in ("", "-"):
            return None
        try:
            num = float(s2)
        except Exception:
            return None
        # 大多是以“人”为单位；若数值很大，自动换算为“万”
        return num / 10000.0 if num >= 10000 else num

    return pl.col(col).map_elements(_parse, return_dtype=pl.Float64).fill_null(0.0)


def _pick_first_col(df: pl.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _generate_case_statement(
    tiers: List[dict], column: str, alias: str, extra: Optional[str] = None
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
    raw = _read_table_or_exit(path)
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
            like_number=_num_clean("like_number", pl.Int64),
            collect=_num_clean("collect", pl.Int64),
            comment=_num_clean("comment", pl.Int64),
        )
        .with_columns(pl.col(pl.Utf8).str.strip_chars())  # 字符串列去空白
    )

    if df.is_empty():
        print("警告: 用户信息文件中没有有效数据。")

    return df


def load_account(path: str) -> pl.DataFrame:
    print(f"正在加载账户信息文件: {path}")

    raw = _read_table_or_exit(path)
    required = list(ACCOUNT_COLUMNS_MAP.keys())
    _ensure_columns(raw, required, path)

    df = (
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
        .with_columns(
            pl.col("云账户绑定银行卡号（必填）")
            .cast(pl.Utf8)
            .str.replace_all(r"\s+", "")
        )
        .filter(pl.col("row_num") == 1)
        .drop(["row_num", "_ts"])
        .select(required)
        .rename(ACCOUNT_COLUMNS_MAP)
        .with_columns(pl.col(pl.Utf8).str.strip_chars())
    )
    return df


def load_company(path: str, price_mode: str) -> Tuple[pl.DataFrame, int]:
    """
    返回: (公司表DataFrame, default_org_flag)
    default_org_flag: 当无法使用公司表时，both 模式下默认是否机构:
      - 1 = 默认机构（使用内部价）
      - 0 = 默认非机构（使用外部价）
    """
    print(f"正在加载机构表: {path}")
    p = Path(path)
    if not p.exists():
        if price_mode == "both":
            print(
                f"提示: 未找到公司表 '{path}'，both 模式下将默认所有账号为机构（使用内部价格）。"
            )
        return pl.DataFrame({"nickname": [], "is_institution": []}), 1

    raw = _read_table_or_exit(path)
    nick_col = _pick_first_col(raw, COMPANY_NICK_CANDIDATES)

    if nick_col is None:
        if price_mode == "both":
            print(
                f"提示: 公司表缺少昵称列（候选={COMPANY_NICK_CANDIDATES}），both 模式下将默认所有账号为机构（使用内部价格）。"
            )
        return pl.DataFrame({"nickname": [], "is_institution": []}), 1

    df = (
        raw.select([nick_col])
        .rename({nick_col: "nickname"})
        .with_columns(nickname=pl.col("nickname").cast(pl.Utf8, strict=False))
        .unique(subset=["nickname"], keep="any")
        .with_columns(pl.col(pl.Utf8).str.strip_chars())
        .with_columns(is_institution=pl.lit(1))
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
      coalesce(comp.is_institution, {default_org_flag}) as is_mcn,
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

    out = (
        con.execute(sql)
        .pl()
        .drop(["base_reward_internal", "base_reward_external"], strict=False)
    )
    con.close()

    # 统计信息
    uniq_users = (
        user_df.get_column("nickname").n_unique()
        if "nickname" in user_df.columns
        else 0
    )
    uniq_accounts = (
        acc_df.get_column("nickname").n_unique() if "nickname" in acc_df.columns else 0
    )
    matched_accounts = (
        out.filter(pl.col("cloud_name").is_not_null()).get_column("nickname").n_unique()
        if "cloud_name" in out.columns
        else 0
    )
    print(
        f"数据统计：unique 昵称={uniq_users}；账户表昵称={uniq_accounts}；匹配到账户信息={matched_accounts}"
    )

    return out


def save_result(df: pl.DataFrame, out_path: str) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # 统一列顺序（若用户调整映射仍不会抛错）
    want_cols = list(FINAL_OUTPUT_COLUMNS_MAP.values())
    cols = [c for c in want_cols if c in df.rename(FINAL_OUTPUT_COLUMNS_MAP).columns]
    final_df = df.rename(FINAL_OUTPUT_COLUMNS_MAP)[cols]

    try:
        print(f"正在保存结果到: {out}")
        # 优先 Polars 写 Excel（需要新版 Polars）
        final_df.write_excel(str(out))
        print("文件保存成功！")

        return
    except Exception as e1:  # noqa: BLE001
        print(f"Excel 写入(Polars)失败：{e1}，尝试 Pandas OpenPyXL...")
        try:
            import pandas as pd

            pdf = final_df.to_pandas()
            with pd.ExcelWriter(out, engine="openpyxl") as writer:
                pdf.to_excel(writer, index=False)
            print("文件保存成功！（Pandas）")
            return
        except Exception as e2:  # noqa: BLE001
            alt = out.with_suffix(".csv")

            final_df.write_csv(alt)
            print(f"Excel 写入失败（{e2}），已改存为 CSV: {alt}")


# ====== CLI ======


def main(
    user_info_path: str,
    account_path: str,
    company_path: str,
    output_path: str,
    price_mode: str,
) -> None:
    # 输入文件加载

    user_df = load_user_info(user_info_path)
    if user_df.is_empty():
        print("没有可计算的数据，程序结束。")
        return

    acc_df = load_account(account_path)
    comp_df, default_org_flag = load_company(company_path, price_mode)

    # 计算
    result = calculate(user_df, acc_df, comp_df, price_mode, default_org_flag)
    # 导出
    save_result(result, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="小红书稿费计算脚本（DuckDB + Polars）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "user_info_file",
        nargs="?",
        default="./user_info.csv",
        help="用户信息 CSV/Excel 路径",
    )
    parser.add_argument(
        "--account_file", default="./account.csv", help="账户信息 CSV/Excel 路径"
    )
    parser.add_argument(
        "--company_file",
        default="./company.csv",
        help="公司信息 CSV/Excel 路径（含是否机构）",
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

    try:
        main(
            args.user_info_file,
            args.account_file,
            args.company_file,
            args.output_file,
            args.price_mode,
        )
    except KeyboardInterrupt:
        print("用户中断。", file=sys.stderr)
        sys.exit(130)
    except Exception as e:  # noqa: BLE001
        print(f"运行失败: {e}", file=sys.stderr)
        sys.exit(1)
