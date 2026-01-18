#!/usr/bin/env python
"""
将 .xls 文件转换为 .csv 并对常见编码/空格问题做清理。

用法:
  python convert_xls_to_csv.py 港股通股票代码.xls

依赖:
  pip install pandas xlrd

行为:
  - 读取第一个 sheet，转换为 CSV（UTF-8 带 BOM）
  - 清理 `中文简称` 相关列中的各种 Unicode 空格
  - 保证 `证券代码` 列为 5 位字符串，保留前导 0
"""
from __future__ import annotations
import sys
from pathlib import Path
import re

def normalize_whitespace(s: str) -> str:
    if s is None:
        return s
    s = str(s)
    s = s.replace('\u200b', '').replace('\ufeff', '')
    s = s.replace('\u3000', ' ')
    s = re.sub(r'[\s\u00A0]+', ' ', s)
    return s.strip()


def pad_code_value(x: object) -> object:
    import pandas as pd
    if pd.isna(x):
        return x
    s = str(x).strip()
    if s == '':
        return s
    if re.match(r'^\d+(?:\.0+)?$', s):
        s = s.split('.')[0]
        return s.zfill(5)
    return s


def convert_xls_to_csv(xls_path: Path, csv_path: Path | None = None) -> Path:
    try:
        import pandas as pd
    except Exception as e:
        raise RuntimeError("缺少 pandas。请先运行: pip install pandas xlrd") from e

    if not xls_path.exists():
        raise FileNotFoundError(f"找不到文件: {xls_path}")

    if csv_path is None:
        csv_path = xls_path.with_suffix('.csv')

    # 尝试用 pandas 读取（若 pandas 与 xlrd 不匹配则回退）
    df = None
    try:
        df = pd.read_excel(xls_path, sheet_name=0, engine='xlrd')
    except Exception:
        try:
            import xlrd
        except Exception as e:
            raise RuntimeError("无法读取 .xls：既无法用 pandas 读取，也无法导入 xlrd。请安装依赖: pip install pandas xlrd") from e

        wb = xlrd.open_workbook(str(xls_path))
        sheet = wb.sheet_by_index(0)
        rows = []
        for r in range(sheet.nrows):
            rows.append([sheet.cell_value(r, c) for c in range(sheet.ncols)])
        # 第一行为表头
        header = rows[0]
        data = rows[1:]
        df = pd.DataFrame(data, columns=header)

    # 清理含 '中文简称' 的列
    target_cols = [c for c in df.columns if '中文简称' in c]
    for col in target_cols:
        df[col] = df[col].apply(lambda x: normalize_whitespace(x) if pd.notna(x) else x)

    # 补齐 `证券代码` 为 5 位
    code_cols = [c for c in df.columns if '证券代码' in c]
    for col in code_cols:
        df[col] = df[col].apply(pad_code_value)

    df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    return csv_path


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:]) if argv is None else argv
    if not argv:
        print("用法: python convert_xls_to_csv.py <输入.xls> [输出.csv]")
        return 2

    xls = Path(argv[0])
    csv = Path(argv[1]) if len(argv) > 1 else None

    try:
        out = convert_xls_to_csv(xls, csv)
        print(f"已生成: {out}")
        return 0
    except Exception as e:
        print(f"转换失败: {e}")
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:]) if argv is None else argv
    if not argv:
        print("用法: python convert_xls_to_csv.py <输入.xls> [输出.csv]")
        return 2

    xls = Path(argv[0])
    csv = Path(argv[1]) if len(argv) > 1 else None

    try:
        out = convert_xls_to_csv(xls, csv)
        print(f"已生成: {out}")
        return 0
    except Exception as e:
        print(f"转换失败: {e}")
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
