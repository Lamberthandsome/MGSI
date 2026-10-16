"""Verify actual records and report cold reads separately from resident reuse."""
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parents[1]
EVIDENCE=ROOT/'evidence'
if not EVIDENCE.exists():
    EVIDENCE=ROOT/'evidence_pre_sql'
ACTIVE='preinitialized'


def read(name):
    return json.loads((EVIDENCE/name).read_text(encoding='utf-8'))


def lines(name):
    return [json.loads(line) for line in (EVIDENCE/name).read_text(encoding='utf-8').splitlines()]


def verify_months(actual,expected):
    assert actual.keys()==expected.keys() and len(actual)==120
    for month,value in actual.items():
        assert value['count']==expected[month]['count'],(month,'count')
        assert math.isclose(value['sum'],expected[month]['sum'],rel_tol=1e-9,abs_tol=1e-6),(month,'sum')
        assert math.isclose(value['mean'],value['sum']/value['count'],rel_tol=1e-12),month


def main():
    snapshot=read('snapshot_manifest.json')
    for row in snapshot['files']:
        path=EVIDENCE/row['path']
        assert path.stat().st_size==row['bytes'] and hashlib.sha256(path.read_bytes()).hexdigest()==row['sha256'],row['path']
    all_versions={}
    golden={r['case']:r['monthly_values'] for r in read('golden_results.json')}
    for variant,expected_count in (('',48),('coalesced',12),('spatial_coalesced',48),(ACTIVE,48)):
        prefix=variant+'/' if variant else ''
        for name,expected in read(prefix+'run_pin.json')['files'].items():
            assert hashlib.sha256((EVIDENCE/prefix/name).read_bytes()).hexdigest()==expected,(variant,name)
        variant_rows=lines(prefix+'results.jsonl')
        assert len(variant_rows)==expected_count
        expected_regions=(1,8) if variant=='coalesced' else range(1,9)
        assert {(r['region'],r['repeat']) for r in variant_rows}=={(region,repeat) for region in expected_regions for repeat in range(6)}
        variant_job=read(prefix+'job.json')
        assert variant_job['state']=='FINISHED' and read(prefix+'state.json')['status']=='COMPLETE'
        assert read(prefix+'exceptions.json')['root-exception'] is None
        variant_plan=json.dumps(read(prefix+'plan.json'))
        assert 'CUSTOM' in variant_plan and 'pipelined_bounded' in variant_plan
        assert 'PythonCalc' not in variant_plan and 'HashAggregate' not in variant_plan
        for r in variant_rows:
            assert r['status']=='PASS' and r['job_id']==variant_job['jid']
            assert {w['route'] for w in r['worker_rows']}==set(range(72))
            assert set(r['active_data_hosts'])=={'192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72'}
            verify_months(r['monthly_values'],golden['gleam_1981_1990_spatial_R'+str(r['region'])])
            assert sum(w['physical_read_bytes'] for w in r['worker_rows'])==r['physical_read_bytes']
            if r['repeat']>0:
                assert r['physical_read_bytes']==0 and r['plane_cache_misses']==0,(variant,r['query_id'],'cache still thrashes')
            else:
                assert r['physical_read_bytes']>0 and r['plane_cache_misses']>0
        all_versions[variant or 'native_base']={'queries':len(variant_rows),'job_id':variant_job['jid'],
            'initial_read_seconds':{r['region']:r['until_final_120_month_results_seconds'] for r in variant_rows if r['repeat']==0}}
    rows=lines(ACTIVE+'/results.jsonl')
    assert len(rows)==48
    assert {(r['region'],r['repeat']) for r in rows}=={(region,repeat) for region in range(1,9) for repeat in range(6)}
    job=read(ACTIVE+'/job.json')
    assert job['state']=='FINISHED' and read(ACTIVE+'/state.json')['status']=='COMPLETE'
    plan=json.dumps(read(ACTIVE+'/plan.json'))
    assert 'CUSTOM' in plan and 'pipelined_bounded' in plan
    assert 'PythonCalc' not in plan and 'HashAggregate' not in plan
    assert read(ACTIVE+'/exceptions.json')['root-exception'] is None
    assert read('physical_index_validation.json')['raw_offset_decode_checks']==90
    assert read('physical_index_validation.json')['status']=='PASS'
    for r in rows:
        assert r['status']=='PASS' and r['job_id']==job['jid']
        assert {w['route'] for w in r['worker_rows']}==set(range(72))
        assert set(r['active_data_hosts'])=={'192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72'}
        verify_months(r['monthly_values'],golden['gleam_1981_1990_spatial_R'+str(r['region'])])
        assert sum(w['physical_read_bytes'] for w in r['worker_rows'])==r['physical_read_bytes']
        if r['repeat']>0:
            assert r['physical_read_bytes']==0 and r['plane_cache_misses']==0,(r['query_id'],'cache still thrashes')
    with (PROJECT/'results/paper_values/table3_spatial.csv').open(encoding='utf-8-sig',newline='') as stream:
        paper_rows={row['method']:row for row in csv.DictReader(stream) if row['dataset']=='GLEAM'}
    paper=paper_rows['MGSI']
    old=[37.805,50.044,58.122,64.852,64.220,66.487,82.137,82.608]
    matrix=[]
    for region in range(1,9):
        cold=next(r for r in rows if r['region']==region and r['repeat']==0)
        repeats=[r['until_final_120_month_results_seconds'] for r in rows if r['region']==region and r['repeat']>0]
        matrix.append({'region':region,'thesis_seconds':float(paper['R'+str(region)]),'previous_prototype_seconds':old[region-1],
                       'resident_initial_read_seconds':cold['until_final_120_month_results_seconds'],
                       'resident_repeat_median_seconds':statistics.median(repeats),'resident_repeat_min_seconds':min(repeats),
                       'resident_repeat_max_seconds':max(repeats),'initial_physical_read_bytes':cold['physical_read_bytes']})
        matrix[-1]['initial_vs_thesis_percent']=100*(matrix[-1]['resident_initial_read_seconds']/matrix[-1]['thesis_seconds']-1)
    sql=[]
    if (EVIDENCE/'sql_results.jsonl').exists():
        sql=lines('sql_results.jsonl')
        assert len(sql)==26 and len({(r['method'],r['region'],r['repeat']) for r in sql})==26
        assert read('sql_queue_state.json')['status']=='COMPLETE'
        for name,expected in read('sql_pin.json').items():
            assert hashlib.sha256((EVIDENCE/name).read_bytes()).hexdigest()==expected,name
        for r in sql:
            assert r['status']=='PASS'
            verify_months(r['monthly_values'],golden['gleam_1981_1990_spatial_R'+str(r['region'])])
            if r['method']=='flinksql':
                assert read('sql_job_evidence/'+r['job_id']+'/job.json')['state']=='FINISHED'
                assert 'PythonCalc' not in json.dumps(read('sql_job_evidence/'+r['job_id']+'/plan.json'))
            else:
                assert 'FileScan parquet' in r['executed_plan'] and 'PythonUDF' not in r['executed_plan']
        assert read('sql_input_manifest.json')['status']=='PASS' and not read('sql_input_manifest.json')['spatial_prefilter']
        assert read('source_immutability.json')['status']=='PASS'
    initialization=read(ACTIVE+'/initialization.json')
    assert initialization['header_bytes_read_in_initialization']==1920
    assert initialization['spatial_templates_prepared_per_jvm']==500
    assert len({(r['host'],r['jvm']) for r in initialization['workers'].values()})==24
    init=initialization['until_query_service_ready_seconds']
    environment=read('environment_final_all.json')
    assert environment['taskmanagers']==24 and environment['slots-available']==72 and environment['jobs-running']==0
    total_native=sum(v['queries'] for v in all_versions.values())
    summary={'status':'PASS','snapshot_files_verified':len(snapshot['files']),'native_queries_passed':len(rows),
             'active_variant':ACTIVE,'all_native_versions':all_versions,'all_native_queries_passed':total_native,
             'sql_queries_passed':len(sql),'monthly_results_verified':120*(total_native+len(sql)),
             'physical_offset_decode_checks_passed':90,'native_job_id':job['jid'],
             'native_startup_until_service_ready_seconds_separate':init,
             'native_ownership_preparation_seconds_included_in_startup':initialization['ownership_preparation_seconds'],
             'metadata_header_bytes_in_startup':1920,'spatial_geometry_templates_per_jvm':500,
             'physical_index_preparation_seconds_separate':read('physical_index_validation.json')['preparation_seconds'],
             'matrix':matrix,'cache_read_misses_on_all_40_repeats':0,
             'scope':'GLEAM Eb v4.3a 0.1°, 1981–1990, 120 monthly 15th days, all R1–R8; school reconstruction, not recovered Alibaba code.',
             'limits':['Thesis states 0.25° but also 500×90×144 logical chunks; original data version not recovered.',
                       'Thesis cold/warm boundary unspecified; cold reads and repeated resident queries reported separately.',
                       'SQL Parquet contains only the 120 queried dates, not every date in the ten-year archive; temporal preprocessing differs from unverified original baseline storage.',
                       'JVM heap cache is bounded; not yet Flink managed-memory/checkpoint recovery.',
                       'Locality and memory-aware ownership is stable; live dynamic repartitioning/migration not implemented.',
                       'Flink SQL creates fresh jobs; Spark and MGSI use resident applications; no algorithm performance ranking.']}
    (ROOT/'核验汇总.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    with (ROOT/'GLEAM十年前后对比.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(matrix[0]))
        writer.writeheader();writer.writerows(matrix)
    report=['# GLEAM 十年常驻物理索引重构与实测', '',
            '真实输入已经恢复为完整 1981–1990、每月 15 日、120 个月的 R1–R8；不使用前轮一天样本作为本轮十年结果。八区域各首次读取一次、缓存复用五次，共 48 次，全数核对逐月 sum/count/mean。', '',
            '## 已修正的实现', '',
            '改用 Flink 1.11.3 原生 Java 常驻有限查询作业，移除每次重新启动的 Python UDF 与 blocking GROUP BY 分发。三个索引层保留：主节点协调器的全局 KD 树（左上角代表点+精确分块相交），各 JVM 的 ChunkID 元数据哈希，复用的规则网格内存 KD 空间划分树。全局树仍在主节点查询协调器，未嵌入 Flink JobManager 内部。', '',
            '上传阶段提取 HDF5 压缩块的真实 byte offset/size/filter mask，通过 Hadoop FSDataInputStream positioned read 直接读取并按原 dtype/deflate 解压。90 个实际块的输出与 h5py 数组逐元素一致；查询读取原始 NetCDF，没有改变数据值或转换成 Parquet。每个 JVM 的有界缓存只保留选定日期的物理块平面，复用 KD 树和 HDFS 文件连接，每次重新筛选并求和，不缓存查询聚合结果。', '',
            '根据 HDFS 副本位置与压缩字节/解压后平面内存负载分配固定数据所有权；每 JVM 缓存上限 512 MiB（24 个 JVM 合计配置上限 12 GiB），无节点 CPU、Flink 总进程预算变更。大区域的五次复用均零物理读取、零平面缓存未命中，修复了 256 MiB 诊断版的大区域缓存抖动。', '',
            '最终冻结版本为 `preinitialized/`，八区域全部采用此版本，没有逐区域挑选不同版本的最短时间。相邻逻辑分块按 450×720 网格成组放到同一 JVM，共享交叠的物理压缩块；将相邻 offset 的读取合并（最多 8 MiB span、4 KiB gap），随后只解压所需块。SubTask 初始化时准备 500 种几何 KD 模板及十个文件的连接，每文件只读取 8 字节签名，共 1920 字节；查询变量数据仍在首次查询计时内读取。', '',
            '这是一组对归档数据的有限查询，DataStream API 用于常驻请求/计算，不开展持续数据摄取或增量索引维护实验。', '',
            '## 同一十年输入的前后结果', '',
            '单位秒。旧重构版每窗口重新启动作业；新版作业启动另记 {:.3f} 秒，物理索引准备一次性另记 {:.3f} 秒。首次读取前清除应用平面缓存，未清除操作系统/HDFS 缓存；五次复用均重新执行完整筛选和 120 个月汇总。论文未注明缓存状态，历史列不能与其中任一组直接认定为同条件。'.format(init,summary['physical_index_preparation_seconds_separate']), '',
            '| 区域 | 论文 MGSI（历史） | 旧重构版 | 新版首次读取 | 新版复用中位数（5 次） |',
            '|---|---:|---:|---:|---:|']
    for m in matrix:
        report.append('| R{} | {:.3f} | {:.3f} | {:.3f} | {:.3f} |'.format(m['region'],m['thesis_seconds'],m['previous_prototype_seconds'],m['resident_initial_read_seconds'],m['resident_repeat_median_seconds']))
    report += ['',
        '最终版 {:.3f} 秒初始化包括提交作业、SubTask 元数据/几何树/文件连接准备、72 条返回路径就绪，以及 {:.3f} 秒固定所有权规划。R1 首次查询再需 {:.3f} 秒，两段合计 {:.3f} 秒；一次性物理索引提取另计，全局树在提交计时前创建，未逐段测量。上述边界均显式记录，不能将首次查询称为完整服务冷启动。'.format(init,initialization['ownership_preparation_seconds'],matrix[0]['resident_initial_read_seconds'],init+matrix[0]['resident_initial_read_seconds']), '',
        'R1–R3 首次读取比论文分别高 {:.1f}%、{:.1f}%、{:.1f}%；R4–R8 首次读取处于或低于论文记录。这表明共同框架开销和读取放大已明显修正，不构成八区域完全相同的历史性能复现；缓存复用的毫秒级数字不能拿来代替论文表格。'.format(*[m['initial_vs_thesis_percent'] for m in matrix[:3]]), '',
        '## 实现迭代记录', '',
        '| 版本 | 通过查询数 | R1 首次读取 | R8 首次读取 |',
        '|---|---:|---:|---:|']
    for version,v in all_versions.items():
        report.append('| {} | {} | {:.3f} | {:.3f} |'.format(version,v['queries'],v['initial_read_seconds'][1],v['initial_read_seconds'][8]))
    report += ['', '这些迭代按时间顺序依次运行，操作系统/HDFS 缓存与下载负载没有控制，因此本表是排错和实现演进记录，不是严格消融实验。除取消的 256 MiB 诊断外，共 {} 次原生查询和 {} 次 SQL 查询均核对全部 120 个月，累计 {:,} 条月度结果核对。'.format(total_native,len(sql),120*(total_native+len(sql))), '',
        '## 相同十年输入的 SQL 对照', '']
    if sql:
        source=read('sql_input_manifest.json')
        report += ['Flink SQL / Spark SQL 均读取同一 120 个日期的全球有效点 Parquet，随后执行 R1–R8 WHERE 和按月份 SUM/COUNT，无 R1 空间预过滤；转换成本 {:.3f} 秒另计，共 {:,} 个点、{:,.3f} GiB Parquet。独立直接数组转换流程另外核对了 R1/R8 全 120 个月。'.format(source['preparation_seconds_separate'],source['global_valid_points'],source['parquet_bytes']/1024**3), '',
                   'SQL 转换阶段已经选择每月 15 日，共 120 个日期；尚未构建十年所有日期的完整点表。MGSI 仍读取十个原始全年 NetCDF，物理压缩块包含查询日以外的日期。原论文 SQL 点表是否做日期预筛选、文件/行组布局和压缩方式均未恢复。因此本轮 SQL 只验证十年查询的完整数值与执行路径，尚不能认定扫描输入与旧正式 baseline 完全一致。', '',
                   '| 区域 | 论文 Flink SQL（历史） | 本轮 Flink SQL（新作业） | 论文 Spark SQL（历史） | 本轮 Spark SQL（常驻应用） |',
                   '|---|---:|---:|---:|---:|']
        for region in range(1,9):
            cells=[]
            for method in ('flinksql','sparksql'):
                values=[r['until_final_120_month_results_seconds'] for r in sql if r['region']==region and r['method']==method]
                cells.append(' / '.join('{:.3f}'.format(v) for v in values))
            report.append('| R{} | {:.3f} | {} | {:.3f} | {} |'.format(region,float(paper_rows['FlinkSQL']['R'+str(region)]),cells[0],float(paper_rows['SparkSQL']['R'+str(region)]),cells[1]))
        report += ['', '10 个 Flink SQL 作业和 16 次 Spark SQL 查询数值通过，总计 26 次。Spark 应用启动另记 {:.3f} 秒。查询结果覆盖全部 120 个月，但存储输入、应用初始化和缓存边界仍不同，因此本表用于定位执行成本，不用来宣称算法排名或已复现原论文性能。Spark 大区域与论文差距仍明显，不能仅归因于代码错误；需先统一完整归档点表、布局和计时协议。'.format(next(r['startup_seconds_separate'] for r in sql if r['method']=='sparksql')), '']
    else:
        report += ['输入生成和 SQL 查询正在另行运行；此快照只含已完成的 48 次常驻 MGSI 结果，未生成 SQL 数字。', '']
    report += ['## 仍需对齐的条件', '',
        '当前 GLEAM 为 v4.3a/0.1°。大论文写 0.25°，同时每年 500 个 90×144 分块又对应全球 0.1°，旧输入和代码缺失。不能通过改分辨率或结果数字冒充已恢复旧输入。JVM 有界堆缓存尚未复刻为 Flink 托管内存；动态重分区、迁移和 checkpoint 恢复仍是独立工作，不把当前固定分配当作动态负载均衡证据。', '',
        '其他数据集、查询类型和缺失的 SciSpark/Hierarchy Index 未由本轮自动完成。NCEP 需按真实月产品、坐标、观测时次固定口径；CHIRPS 的长江中上游查询框在旧材料中未明确；MERRA-2/ECMWF 十年输入尚未完全到位。', '',
        '## 留档与接续', '',
        '主作业 '+job['jid']+' 已 FINISHED；主实验固定源文件哈希与完整快照哈希通过。256 MiB 首次诊断因缓存抖动主动取消，成功记录保存在 diagnostic_256mb。首次重跑未等取消/监听端口释放，导致自己的日志和 pin 被覆盖；已保留失败重跑文件，并从从节点恢复实际提交 jar，源文件反向恢复的过程另有标记。没有把这些诊断/失败当作 48 次成功主实验的原始版本证明。', '',
        '最终原始记录：`evidence/preinitialized/results.jsonl`、`preinitialized/job.json`、`preinitialized/plan.json`、`preinitialized/run_pin.json`；根目录保存物理索引验证和 SQL 记录 / `sql_job_evidence/`。输入来源核对了十个原始文件 HDFS length/mtime，以及 310 个 Parquet 文件长度；未对所有数据内容做完整哈希。实验快照逐文件 SHA-256 通过，原始输入不作修改。', '',
        '服务器独立目录：`/opt/mgsi-experiments/20261006-resident-native/`。当前入口 `preinitialized/ResidentMGSI.java` + `preinitialized/run_resident.py`；SQL 输入/队列分别为 `prepare_tenyear_sql.py`、`run_tenyear_sql_queue.py`。已保存的运行不得直接再次追加；先归档当前记录并建立新运行目录。最终 Flink 24 个 TaskManager、72 空闲槽位、零运行作业。', '',
        '本轮只更新复现代码、实验事实和内部接续说明，不以缓存毫秒级结果替换论文的历史表，也不把当前实现称为完整恢复的原阿里云代码。', '']
    (ROOT/'十年复现修复记录.md').write_text('\n'.join(report),encoding='utf-8')
    print(json.dumps({k:v for k,v in summary.items() if k not in ('matrix','limits','all_native_versions')},ensure_ascii=False))


if __name__=='__main__':
    main()
