"""Spark 2.1.0 SQL on all 120 dates with complete monthly collect."""
import json
import math
from pathlib import Path
import time
from pyspark.sql import SparkSession

ROOT=Path(__file__).resolve().parent


def main():
    setup=time.perf_counter()
    spark=SparkSession.builder.appName('MGSI-identical-ten-year-SQL-20261006').getOrCreate()
    sc=spark.sparkContext
    assert spark.version=='2.1.0' and sc.master=='spark://192.0.2.64:7077'
    deadline=time.time()+120
    while sc._jsc.sc().getExecutorMemoryStatus().size()<25 and time.time()<deadline:
        time.sleep(1)
    assert sc._jsc.sc().getExecutorMemoryStatus().size()==25
    startup=time.perf_counter()-setup
    source=json.loads((ROOT/'sql_input_manifest.json').read_text())
    meta=json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    golden={r['case']:r['monthly_values'] for r in json.loads((ROOT/'golden_results.json').read_text())}
    try:
        for region in range(1,9):
            for repeat in (1,2):
                timer=time.perf_counter()
                frame=spark.read.parquet(source['parquet_path'])
                frame.createOrReplaceTempView('points')
                box=meta['boxes'][str(region)]
                sql=spark.sql('SELECT month,SUM(value) AS total,COUNT(*) AS n FROM points WHERE lat BETWEEN {lat_min} AND {lat_max} AND lon BETWEEN {lon_min} AND {lon_max} GROUP BY month'.format(**box))
                plan=sql._jdf.queryExecution().executedPlan().toString()
                rows=sql.collect()
                monthly={r['month']:{'sum':float(r['total']),'count':int(r['n']),'mean':float(r['total'])/int(r['n']) if int(r['n']) else None} for r in rows}
                elapsed=time.perf_counter()-timer
                expected=golden['gleam_1981_1990_spatial_R'+str(region)]
                assert monthly.keys()==expected.keys()
                for month,value in monthly.items():
                    assert value['count']==expected[month]['count'],(region,month,'count')
                    assert math.isclose(value['sum'],expected[month]['sum'],rel_tol=1e-9,abs_tol=1e-6),(region,month,'sum')
                record={'status':'PASS','method':'sparksql','region':region,'repeat':repeat,'application_id':sc.applicationId,
                        'until_final_120_month_results_seconds':elapsed,'startup_seconds_separate':startup,
                        'monthly_values':monthly,'executed_plan':plan,'scope':meta['scope'],
                        'query_boundary':'one resident Spark application; read.parquet through complete collect; no DataFrame persist/cache; possible OS/HDFS/JVM warmth; preparation separate.'}
                with (ROOT/'sql_results.jsonl').open('a') as stream:
                    stream.write(json.dumps(record)+'\n')
                print('TENYEAR_SPARK_SQL_COMPLETE '+json.dumps({k:v for k,v in record.items() if k not in ('monthly_values','executed_plan')}),flush=True)
    finally:
        spark.stop()


if __name__=='__main__':
    main()
