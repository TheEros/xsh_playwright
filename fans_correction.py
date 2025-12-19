# -*- coding: utf-8 -*-
"""

粉丝量统一为“万”（纯数字），保留原始顺序。
- 含 w/W/万：按“万”为单位取数值
- 纯数字：
    * >=1000 视作个数，/10000 转万
    * 含小数点视作“万”
    * <100 且为整数：默认按“万”，并打印提醒
- 含非 w/W 的字母：无法识别，打印提醒

- 原始连续数字过长（>=9位且无单位）：打印提醒
- 转换后 >1000（即 >1亿）：打印提醒
- 结果仅保留数字（四舍五入 4 位小数）
"""

import re
import csv
import polars as pl
from typing import Tuple, Optional, List

# 你的原始数据（顺序即为输出顺序）
raw_values = (
    pl.read_excel("./user_info.xlsx").select("粉丝量（必填）").to_series().to_list()
)


def normalize_to_wan(cell: str) -> Tuple[Optional[float], List[str]]:
    """
    将输入规范化为“万”为单位的数字。
    返回: (wan_value或None, issues列表)
    """
    issues: List[str] = []

    if cell is None:
        return None, ["无法识别：空值"]

    raw = str(cell).strip()
    if not raw:
        return None, ["无法识别：空字符串"]

    s = raw.replace(" ", "")
    lower = s.lower()

    # 非法字母（允许 w/W 和 中文“万”）
    if re.search(r"[a-vx-z]", lower):  # 除了 w 之外的英文字母
        return None, [f"无法识别：含非w字母({raw})"]

    # 处理“1,2W”这种小数逗号 -> 小数点，其它逗号按千分位去掉
    if re.fullmatch(r"\d+,\d+(?:[wW]|万)?", s):
        s = s.replace(",", ".")
        lower = s.lower()
    else:
        s = s.replace(",", "")
        lower = s.lower()

    has_unit = ("w" in lower) or ("万" in lower)

    # 原始连续数字过长（>=9位）且无单位：打印提醒（如 49720357369）
    max_digit_run = 0

    for m in re.finditer(r"\d+", raw):
        max_digit_run = max(max_digit_run, len(m.group(0)))
    if (not has_unit) and max_digit_run >= 9:
        issues.append("原始连续数字过长(>=9位)，请确认是否应为“个数”")

    # 提取数字
    mnum = re.search(r"(\d+(?:\.\d+)?)", lower)
    if not mnum:
        return None, [f"无法识别：无有效数字({raw})"]

    try:
        val = float(mnum.group(1))
    except Exception:
        return None, [f"无法识别：数字解析失败({raw})"]

    # 转换逻辑
    if has_unit:
        wan = val
    else:
        if "." in lower:  # 含小数点，按“万”
            wan = val
        elif val >= 1000:  # 大整数按“个数”/10000
            wan = val / 10000.0
        elif val < 100:  # 小整数，无单位：默认按“万”，但提醒
            wan = val
            issues.append("无单位且为小整数(<100)，默认按“万”，请确认")
        else:  # 介于[100,1000)的整数，按“个数”/10000
            wan = val / 10000.0

    # 异常大值提醒（>100万）
    if wan > 10:
        issues.append("数值极大（>10万），请确认")

    return round(wan, 4), issues


def main():
    rows = []
    warn_count = 0
    unrec_count = 0

    for i, v in enumerate(raw_values, start=1):
        wan, issues = normalize_to_wan(v)
        rows.append(
            {
                "序号": i,  # 保留原始顺序的索引
                "原始值": v,
                "统一为万(数值)": wan,  # 仅数字
                "备注": "; ".join(issues) if issues else "",
            }
        )

        # 打印提醒
        if issues:
            warn_count += 1
            # 无法识别：wan 为 None
            if wan is None:
                unrec_count += 1
            print(f"[WARN] 行{i} 原始值={v} -> {issues}")

    # 写出 CSV（保留原始顺序）
    out_file = "fans_wan_normalized_order_kept.csv"
    with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["序号", "原始值", "统一为万(数值)", "备注"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print("\n==== 完成 ====")
    print(
        f"总行数: {len(rows)}，有提醒的行数: {warn_count}，其中无法识别的行数: {unrec_count}"
    )
    print(f"已导出: {out_file}")


if __name__ == "__main__":
    main()
