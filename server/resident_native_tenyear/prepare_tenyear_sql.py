"""Global Parquet points for identical ten-year/120-date SQL comparisons."""
import json
from pathlib import Path
import sys
import time

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,'/opt/mgsi-experiments/cross_baseline_audit')
from audit_worker import ExactRangeFile


def main():
    started=time.perf_counter()
    meta=json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    golden={r['case']:r['monthly_values'] for r in json.loads((ROOT/'golden_results.json').read_text())}
    fs=pa.hdfs.connect(host='192.0.2.64',port=9000,user='root')
    client=InsecureClient('http://192.0.2.64:50070',user='root')
    destination='/mgsi-experiments/resident_native/global_parquet_v1'
    assert not fs.exists(destination)
    fs.mkdir(destination)
    references={1:{},8:{}}
    files=[]
    for file in meta['files']:
        before=client.status(file['path'])
        assert (before['length'],before['modificationTime'])==(file['source_bytes'],file['source_mtime'])
        reader=ExactRangeFile(file['path'])
        with h5py.File(reader,'r') as data:
            variable=data[file['variable']]
            lat,lon=data['lat'][:],data['lon'][:]
            times=file['time_indices']
            labels=[s[:7] for s in file['time_labels']]
            for part,r0 in enumerate(range(0,len(lat),file['physical_shape'][1])):
                r1=min(r0+file['physical_shape'][1],len(lat))
                raw=variable[times,r0:r1,:]
                valid=np.isfinite(raw)&(raw!=file['fill'])
                values=raw.astype(np.float64)*file['scale']+file['offset']
                xs=np.tile(lon,r1-r0)
                ys=np.repeat(lat[r0:r1],len(lon))
                matrix=values.reshape(len(times),-1)
                validity=valid.reshape(len(times),-1)
                for region in (1,8):
                    box=meta['boxes'][str(region)]
                    select=(ys>=box['lat_min'])&(ys<=box['lat_max'])&(xs>=box['lon_min'])&(xs<=box['lon_max'])
                    for mi,month in enumerate(labels):
                        sums=references[region].setdefault(month,{'sum':0.,'count':0})
                        keep=validity[mi]&select
                        sums['sum']+=float(matrix[mi,keep].sum(dtype=np.float64))
                        sums['count']+=int(keep.sum())
                total=int(valid.sum())
                if not total:
                    continue
                flat=valid.ravel()
                month_values=[]
                for mi,label in enumerate(labels):
                    month_values.extend([label]*int(validity[mi].sum()))
                table=pa.Table.from_arrays([
                    pa.array(month_values,type=pa.string()),
                    pa.array(np.tile(ys,len(times))[flat].tolist(),type=pa.float64()),
                    pa.array(np.tile(xs,len(times))[flat].tolist(),type=pa.float64()),
                    pa.array(values.ravel()[flat].tolist(),type=pa.float64())],names=['month','lat','lon','value'])
                path=destination+'/year_{:02d}_part_{:03d}.parquet'.format(file['file_id'],part)
                with fs.open(path,'wb') as stream:
                    pq.write_table(table,stream,version='1.0',compression='snappy',row_group_size=131072)
                files.append({'path':path,'bytes':fs.info(path)['size'],'rows':total})
        reader.close()
        after=client.status(file['path'])
        assert (after['length'],after['modificationTime'])==(file['source_bytes'],file['source_mtime'])
        progress={'source_completed':file['path'],'parquet_files':len(files),'all_global_rows':sum(f['rows'] for f in files),'seconds':time.perf_counter()-started}
        (ROOT/'sql_preparation_progress.json').write_text(json.dumps(progress))
        print('TENYEAR_PARQUET_PROGRESS '+json.dumps(progress),flush=True)
    for region in (1,8):
        expected=golden['gleam_1981_1990_spatial_R'+str(region)]
        assert references[region].keys()==expected.keys()
        for month,value in references[region].items():
            assert value['count']==expected[month]['count'],(region,month,'count')
            assert np.isclose(value['sum'],expected[month]['sum'],rtol=1e-9,atol=1e-6),(region,month,'sum')
    result={'status':'PASS','parquet_path':'hdfs://192.0.2.64:9000'+destination,'files':files,
            'global_valid_points':sum(f['rows'] for f in files),'parquet_bytes':sum(f['bytes'] for f in files),
            'preparation_seconds_separate':time.perf_counter()-started,'direct_array_reference_checks':'R1 and R8, all 120 months',
            'references':references,'scope':meta['scope'],'spatial_prefilter':False}
    (ROOT/'sql_input_manifest.json').write_text(json.dumps(result,indent=2))
    print('TENYEAR_PARQUET_COMPLETE '+json.dumps({k:v for k,v in result.items() if k not in ('files','references')}),flush=True)


if __name__=='__main__':
    main()
