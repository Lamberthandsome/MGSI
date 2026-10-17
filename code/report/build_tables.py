"""汇总 results/raw 的多次重复实验，计算中位数并与参考值对照，生成报告表。

产出:
    results/tables/table7_ablation_repro.csv|.md
    results/tables/table2_time_range_repro.csv|.md
    results/tables/table3_spatial_repro.csv|.md
    results/tables/table45_point_knn_repro.csv|.md
    results/tables/dataset_change_repro.csv|.md
    results/tables/summary_trends.md   （趋势一致性总表）

用法: python report/build_tables.py   （在 code/ 下 bash run.sh report/build_tables.py）
"""
import os
import sys

import numpy as np
import pandas as pd

PROJECT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RAW = os.path.join(PROJECT, 'results', 'raw')
PAPER = os.path.join(PROJECT, 'results', 'paper_values')
OUT = os.path.join(PROJECT, 'results', 'tables')
os.makedirs(OUT, exist_ok=True)


def load_raw(experiment):
    path = os.path.join(RAW, f'{experiment}.csv')
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    for c in df.columns:
        if c not in ('experiment', 'method', 'manifest'):
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def med_by_method(df, value_col):
    if df is None or value_col not in df.columns:
        return {}
    g = df.groupby('method')[value_col].median()
    return g.to_dict()


def fmt(v):
    return f"{v:.3f}" if isinstance(v, float) and not pd.isna(v) else ('' if v is None else str(v))


def write_csv_md(name, df):
    df.to_csv(os.path.join(OUT, f'{name}.csv'), index=False, encoding='utf-8-sig')
    with open(os.path.join(OUT, f'{name}.md'), 'w', encoding='utf-8') as f:
        f.write(df.to_markdown(index=False) + '\n')
    print(f"[产出] {name}.csv/.md")


def build_ablation():
    raw = load_raw('ablation')
    paper = pd.read_csv(os.path.join(PAPER, 'table7_ablation.csv'))
    rows = []
    med = med_by_method(raw, 'total_ms')
    label_map = {
        'remove_logical_chunking': 'Remove Logical Chunking',
        'remove_global_index': 'Remove Global Index',
        'remove_local_index': 'Remove Local Index',
        'remove_in_memory_index': 'Remove In-Memory Index',
        'remove_dynamic_repartitioning': 'Remove Dynamic Repartitioning',
        'no_index_fullscan': 'No Index (Full Scan, 对照)',
    }
    for method, label in label_map.items():
        if method in med:
            p = paper[paper['variant'] == label]
            rows.append({
                'variant': label,
                'paper_temporal_s': fmt(p.iloc[0]['temporal_query_s']) if len(p) else '',
                'paper_spatial_s': fmt(p.iloc[0]['spatial_query_s']) if len(p) else '',
                'repro_query_ms_median': fmt(med[method]),
                'repro_query_s_median': fmt(med[method] / 1000),
            })
    df = pd.DataFrame(rows)
    if len(df):
        write_csv_md('table7_ablation_repro', df)


def build_time_range():
    raw = load_raw('time_range')
    if raw is None:
        return
    paper = pd.read_csv(os.path.join(PAPER, 'table2_temporal.csv'))
    rows = []
    for method, cols in {
        'mgsi': 'merra2_10yr',
        'flinksql': 'merra2_10yr',
        'spark': 'merra2_10yr',
    }.items():
        sub = raw[raw['method'] == method]
        if not len(sub):
            continue
        seg = {}
        for c in ['load_and_local_index', 'build_global_index', 'global_query', 'load_chunks',
                  'create_table_udf', 'memory_query', 'total_query_s', 'e2e_s']:
            if c in sub.columns:
                seg[f'med_{c}_s'] = fmt(sub[c].median())
        paper_row = paper[paper['method'].isin(['MGSI' if method == 'mgsi' else
                                                'FlinkSQL' if method == 'flinksql' else 'SparkSQL'])]
        rows.append({
            'method': method,
            'manifest': sub['manifest'].iloc[0],
            'paper_merra2_10yr_s': fmt(paper_row.iloc[0]['merra2_10yr']) if len(paper_row) else '',
            'n_reps': len(sub),
            **seg,
        })
    df = pd.DataFrame(rows)
    if len(df):
        write_csv_md('table2_time_range_repro', df)


def build_spatial():
    raw = load_raw('spatial_range')
    if raw is None:
        return
    paper = pd.read_csv(os.path.join(PAPER, 'table3_spatial.csv'))
    pm = paper[paper['dataset'] == 'MERRA-2'].set_index('method')
    rows = []
    for method in ['mgsi', 'flinksql', 'spark']:
        sub = raw[raw['method'] == method]
        if not len(sub):
            continue
        row = {'method': method, 'manifest': sub['manifest'].iloc[0], 'n_reps': len(sub)}
        pname = {'mgsi': 'MGSI', 'flinksql': 'FlinkSQL', 'spark': 'SparkSQL'}[method]
        for i in range(1, 9):
            c = f'R{i}_s'
            row[f'R{i}_med_s'] = fmt(sub[c].median()) if c in sub.columns else ''
            if pname in pm.index:
                row[f'R{i}_paper'] = fmt(pm.loc[pname, f'R{i}'])
        rows.append(row)
    df = pd.DataFrame(rows)
    if len(df):
        write_csv_md('table3_spatial_repro', df)


def build_point_knn():
    out_rows = []
    for exp, query in [('point', 'point'), ('nearest', 'knn')]:
        raw = load_raw(exp)
        if raw is None:
            continue
        # 点/kNN 主计时：查询目标点的值 / 最近邻查询
        col = 'point_query' if exp == 'point' else 'knn_query'
        med = med_by_method(raw, col)
        e2e = med_by_method(raw, 'e2e_s')
        paper = pd.read_csv(os.path.join(PAPER, 'table45_point_knn.csv'))
        prow = paper[paper['query_type'] == query].set_index('method')
        for method, v in med.items():
            out_rows.append({
                'query_type': query,
                'method': method,
                'manifest': raw[raw['method'] == method]['manifest'].iloc[0],
                'paper_merra2_s': fmt(prow.loc['MGSI', 'merra2']) if 'MGSI' in prow.index else '',
                'repro_query_s_median': fmt(v),
                'repro_e2e_s_median': fmt(e2e.get(method)),
            })
    if out_rows:
        write_csv_md('table45_point_knn_repro', pd.DataFrame(out_rows))


def build_dataset_change():
    raw = load_raw('dataset_change')
    if raw is None:
        return
    rows = []
    for method, g in raw.groupby('method'):
        rows.append({
            'method': method,
            'manifest': g['manifest'].iloc[0],
            'n_reps': len(g),
            'query_s_median': fmt(g['query_s'].median()) if 'query_s' in g else '',
            'query_fixed_s_median': fmt(g['query_fixed_s'].median()) if 'query_fixed_s' in g else '',
        })
    if rows:
        write_csv_md('dataset_change_repro', pd.DataFrame(rows))


def build_summary():
    """趋势一致性总表：不看绝对值，看排序方向是否与参考值一致。"""
    lines = ['# 复现趋势一致性总表', '',
             '单机与集群环境不同，绝对耗时不可直接对比；本表只比较**方法间的相对排序**。', '']
    # 表7 消融排序
    raw = load_raw('ablation')
    if raw is not None and 'total_ms' in raw.columns:
        med = raw.groupby('method')['total_ms'].median().sort_values()
        lines.append('## 消融（表7）——各变体耗时中位数（升序=越快）')
        lines.append('')
        lines.append('| 变体 | 实测中位数(ms) | 参考相对位置 |')
        lines.append('|---|---|---|')
        paper_order = {
            'no_index_fullscan': '最慢（对照）',
            'remove_logical_chunking': '参考中伤害最大(+155%)',
            'remove_dynamic_repartitioning': '参考中第二(+100%)',
            'remove_global_index': '第三',
            'remove_in_memory_index': '第四',
            'remove_local_index': '第五',
        }
        for m, v in med.items():
            lines.append(f"| {m} | {v:.1f} | {paper_order.get(m, '')} |")
        lines.append('')
    # 表2/3 方法排序
    for exp, name in [('time_range', '时间范围（表2）'), ('spatial_range', '空间范围（表3）')]:
        raw = load_raw(exp)
        if raw is None:
            continue
        if exp == 'time_range':
            if 'total_query_s' not in raw.columns:
                continue
            med = raw.groupby('method')['total_query_s'].median().sort_values()
        else:
            r8 = raw.groupby('method')['R8_s'].median() if 'R8_s' in raw.columns else None
            if r8 is None:
                continue
            med = r8.sort_values()
        lines.append(f'## {name}——方法总耗时中位数（升序）')
        lines.append('')
        lines.append('| 方法 | 中位数(s) | 参考排序 |')
        lines.append('|---|---|---|')
        paper_rank = {'mgsi': '最快', 'flinksql': '慢于Hierarchy Index', 'spark': '慢于Flink SQL'}
        for m, v in med.items():
            lines.append(f"| {m} | {v:.3f} | {paper_rank.get(m, '')} |")
        lines.append('')
    with open(os.path.join(OUT, 'summary_trends.md'), 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print('[产出] summary_trends.md')


if __name__ == '__main__':
    build_ablation()
    build_time_range()
    build_spatial()
    build_point_knn()
    build_dataset_change()
    build_summary()
    print('[完成] 全部报告表已生成到 results/tables/')
