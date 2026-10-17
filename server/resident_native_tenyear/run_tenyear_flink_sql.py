"""Actual JVM Flink SQL aggregates over identical global 120-month input."""
import argparse
import csv
import json
import math
from pathlib import Path
import time
from hdfs import InsecureClient
from pyflink.table import EnvironmentSettings,BatchTableEnvironment
import py4j.java_gateway
import py4j.protocol

ROOT=Path(__file__).resolve().parent


def main():
    args=argparse.ArgumentParser()
    args.add_argument('--region',type=int,required=True)
    args.add_argument('--repeat',type=int,required=True)
    args=args.parse_args()
    source=json.loads((ROOT/'sql_input_manifest.json').read_text())
    meta=json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    golden=next(row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text()) if row['case']=='gleam_1981_1990_spatial_R'+str(args.region))
    py4j.java_gateway.get_return_value=py4j.protocol.get_return_value
    setup=time.perf_counter()
    env=BatchTableEnvironment.create(environment_settings=EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build())
    env.get_config().get_configuration().set_integer('parallelism.default',72)
    env.get_config().get_configuration().set_string('pipeline.jars','file:///opt/mgsi-experiments/cross_baseline_audit/flink-sql-parquet_2.11-1.11.3.jar')
    env.execute_sql("CREATE TABLE points (`month` STRING,lat DOUBLE,lon DOUBLE,`value` DOUBLE) WITH ('connector'='filesystem','path'='{}','format'='parquet')".format(source['parquet_path']))
    output='/mgsi-experiments/resident_native/flink_sql_R{}_{}/{}'.format(args.region,args.repeat,time.time_ns() if hasattr(time,'time_ns') else int(time.time()*1e9))
    env.execute_sql("CREATE TABLE output (`month` STRING,total DOUBLE,n BIGINT) WITH ('connector'='filesystem','path'='hdfs://192.0.2.64:9000{}','format'='csv')".format(output))
    box=meta['boxes'][str(args.region)]
    query='INSERT INTO output SELECT `month`,SUM(`value`),COUNT(*) FROM points WHERE lat BETWEEN {lat_min} AND {lat_max} AND lon BETWEEN {lon_min} AND {lon_max} GROUP BY `month`'.format(**box)
    setup_seconds=time.perf_counter()-setup
    timer=time.perf_counter()
    execution=env.execute_sql(query)
    client=execution.get_job_client()
    job_id=str(client.get_job_id())
    client.get_job_execution_result().result()
    hdfs=InsecureClient('http://192.0.2.64:50070',user='root')
    monthly={}
    for name in hdfs.list(output):
        if name.startswith(('.', '_')):
            continue
        with hdfs.read(output+'/'+name,encoding='utf-8') as stream:
            for fields in csv.reader(stream):
                if not fields:
                    continue
                month,total,count=fields
                assert month not in monthly
                total,count=float(total),int(count)
                monthly[month]={'sum':total,'count':count,'mean':total/count if count else None}
    elapsed=time.perf_counter()-timer
    assert monthly.keys()==golden.keys()
    for month,value in monthly.items():
        assert value['count']==golden[month]['count'],(month,value,golden[month])
        assert math.isclose(value['sum'],golden[month]['sum'],rel_tol=1e-9,abs_tol=1e-6),(month,value,golden[month])
    record={'status':'PASS','method':'flinksql','region':args.region,'repeat':args.repeat,'job_id':job_id,
            'until_final_120_month_results_seconds':elapsed,'table_environment_setup_seconds_separate':setup_seconds,
            'monthly_values':monthly,'scope':meta['scope'],'query_boundary':'fresh Flink SQL job submission through final aggregate return; Parquet preparation separate; OS/HDFS cache uncontrolled.'}
    with (ROOT/'sql_results.jsonl').open('a') as stream:
        stream.write(json.dumps(record)+'\n')
    import urllib.request
    target=ROOT/'sql_job_evidence'/job_id
    target.mkdir(parents=True)
    for name,suffix in [('job',''),('plan','/plan'),('exceptions','/exceptions')]:
        with urllib.request.urlopen('http://192.0.2.64:8081/jobs/'+job_id+suffix,timeout=20) as response:
            (target/(name+'.json')).write_bytes(response.read())
    print('TENYEAR_FLINK_SQL_COMPLETE '+json.dumps({k:v for k,v in record.items() if k!='monthly_values'}),flush=True)


if __name__=='__main__':
    main()
