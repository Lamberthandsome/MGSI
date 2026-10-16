# MGSI — Multi-level Geospatial Stream Index

Artifact release for the manuscript *"MGSI: Multi-level Geospatial Stream Index with Dynamic
Load Balancing for Watershed Spatiotemporal Data"*.

This repository contains the implementation of the MGSI index and streaming runtime, the
experiment pipelines used in the paper (single-machine reproduction, five-node cluster
experiments, and the additional reviewer-requested studies), the workload configurations,
and the instructions to obtain the public datasets and re-run every experiment.

## Repository layout

```
MGSI/
├── code/                    # Single-machine reproduction (Python + Java managed cache)
│   ├── mgsi_common/         #   Core library: chunk tree, chunk parameters, global index,
│   │                        #   adaptive repartition policy, data I/O, runtime config
│   ├── java/mgsi/cache/     #   ManagedChunkCache: Flink MemoryManager-backed page cache
│   ├── time_range/          #   Time-range queries (MGSI / Flink SQL / Spark SQL)
│   ├── spatial_range/       #   Spatial range queries R1–R8
│   ├── point/  nearest/     #   Point queries and k-NN (k=8)
│   ├── tiff/                #   GeoTIFF (CHIRPS) queries
│   ├── dataset_change/      #   Dataset adaptation: ERA5, GLEAM, MERRA-2, NCEP/NCAR
│   ├── ablation/            #   Ablation variants (MGSI-1..4, no-repartition, full scan)
│   ├── report/  tests/      #   Table builder, unit tests for policy and global index
│   └── run.sh  run_all.sh   #   Unified entry points
├── server/                  # Five-node cluster experiments (matched-baseline matrix)
│   ├── ResidentMGSI.java    #   Resident (long-lived) MGSI Flink job
│   ├── SedonaMatrix.java    #   Apache Sedona baseline
│   ├── controlled_pipeline.py, prepare_*.py, run_*.py, node_service.py
│   │                        #   Job control, HDFS preparation, Dask/Spark node services
│   └── resident_native_tenyear/  # Ten-year resident variants (preinitialized, coalesced,
│                                 # spatial) and evidence capture
├── reviewer_experiments/    # Studies added during revision
│   ├── scalability_index_cost/  # TaskManager/volume scaling + index build cost
│   ├── fault_injection/         # Failure-injection robustness runs
│   ├── chunk_size/              # Chunk-size variants (time/plane splits)
│   ├── sql_full_archive/        # Full-archive SQL conversions and timing
│   ├── ablation_gleam_v2/       # GLEAM ablation (v2; v3 runs reuse this implementation)
│   ├── ablation_merra2/         # MERRA-2 ablation
│   ├── broadcast/               # Broadcast metadata staging vs cached
│   ├── dynamic_lb/              # Dynamic load balancing: policy, fake-executor
│   │                            # offline validation, steady/AB/p64 workloads
│   └── knn8_tpk/                # k-NN (k=8) and TPK baselines (Sedona/Dask)
├── docs/                    # Reproduction notes, data acquisition guide, deviations
├── requirements.txt         # Python dependencies (single-machine part)
├── LICENSE                  # Apache License 2.0
└── README.md
```

Result files (CSV/JSONL/logs), built JARs, and prepared physical indexes are intentionally
not committed; every script regenerates them from the raw datasets.

## Datasets

All five datasets are public products and are **not** redistributed here. Download sources
and the exact variables used:

| Dataset | Variable | Source |
|---|---|---|
| MERRA-2 | `PRECTOT` (tavg1_2d_flx_Nx) | https://disc.gsfc.nasa.gov/datasets/M2T1NXFLX.5.12.4/ |
| ERA5 | `tp` / evaporation family | https://cds.climate.copernicus.eu/ |
| CHIRPS v2.0 | precipitation (GeoTIFF) | https://www.chc.ucsb.edu/data/chirps |
| GLEAM v3/v4 | `Eb` terrestrial evaporation | https://www.gleam.eu/ |
| NCEP/NCAR R1 | `PRATE_GDS4_SFC_S113` | https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.html |

Detailed download commands, account requirements, and manifest registration are in
[docs/DATA_ACQUISITION.md](docs/DATA_ACQUISITION.md). ERA5 is subject to the Copernicus
licence; CHIRPS/NCEP are open; MERRA-2 requires a free NASA Earthdata login; GLEAM
requires registration by email.

## Environment

### Single-machine part (`code/`)

- Windows or Linux, Python 3.8, Java 8
- `pip install -r requirements.txt` (pyflink 1.13.6, pyspark 3.3.2, netCDF4, rasterio, …)
- Create the venv on an **ASCII-only path** (no Chinese characters/spaces): a non-ASCII
  project path corrupts the Java classpath used by pyflink/pyspark workers.

### Cluster part (`server/`, `reviewer_experiments/`)

Configuration as reported in the manuscript (Section 4.1):

- 5 nodes: 1 master + 4 workers, each physical node 24 cores / 48 GB RAM, 1 Gbps Ethernet
- Hadoop 2.7.2 (HDFS), Flink 1.11.3
- Flink session: 1 JobManager + 24 TaskManagers, 4 cores / 8 GB per TaskManager
- Apache Sedona, Spark SQL (standalone), and Dask/Zarr baselines run on the same cluster
  (see `server/node_service.py` for worker topology)

**Note on addresses:** cluster code refers to nodes by the documentation addresses
`192.0.2.64–66/71/72` (master `.64`). Replace them with your own host addresses before
running. HDFS is accessed as `root` via WebHDFS on port 50070; the Flink REST API is on
port 8081.

### Measurement protocol

All reported numbers use **1 warmup + 5 timed repetitions** (mean ± sample standard
deviation). Before each timed query the application-level cache is emptied; the prepared
chunk tree/metadata may be reused; OS/HDFS caches are not manually cleared. Initial
initialization and index construction are excluded from query latency and reported
separately (index build cost in `reviewer_experiments/scalability_index_cost/`).

### Managed cache profile

`code/java/mgsi/cache/` implements the native managed page cache on top of the real Flink
MemoryManager byte reservations (LFU, read leases): 8 GiB TaskManager process, 2 GiB
managed budget, 512 MiB cache pool, 32 KiB pages, 128 MiB per subtask at four slots,
16 MiB read-buffer cap. See
[code/java/mgsi/cache/README.md](code/java/mgsi/cache/README.md).

## Running

### Single machine

Register downloaded files in `data/MANIFEST_*.txt` (one absolute path per line), then:

```bash
bash code/run.sh code/time_range/local_mgsi.py merra2_single   # one experiment
bash code/run_all.sh 5                                          # full suite, 5 reps
bash code/run.sh code/report/build_tables.py                    # rebuild tables
```

### Cluster

1. Upload the datasets to HDFS (`/user/dataset/<name>/`).
2. `server/prepare_current.py` inventories sources and builds `matrix_inputs.json`;
   `server/prepare_matrix.py` derives Zarr/Sedona representations;
   `server/prepare_physical_index.py` prepares the MGSI physical index.
3. `server/run_resident.py` submits `ResidentMGSI.java` (resident MGSI);
   `server/run_matrix_dask.py` / `SedonaMatrix.java` run the baselines;
   `server/controlled_pipeline.py` sequences a full comparison cell.
4. Reviewer studies: each directory under `reviewer_experiments/` contains its own
   `prepare_*` / `run_*` / `analysis_*` scripts and a chain shell script that reproduces
   the corresponding table/figure.

Unit checks: `code/tests/test_adaptive_policy.py`, `test_global_index.py`; the dynamic
load-balancing policy ships an offline `fake_executor.py` harness with recorded workloads
(`workload_*.json`) so the repartition logic can be validated without a cluster.

## Known scope limits

- SciSpark is an independent public project and is not re-implemented here; Hierarchy
  Index results in the manuscript predate this codebase.
- The ablation v3 runs reuse the `ablation_gleam_v2` implementation (same code path).
- Working notes in `docs/` are written in Chinese; the code, comments, and log formats
  are in English.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Citation

> *MGSI: Multi-level Geospatial Stream Index with Dynamic Load Balancing for Watershed
> Spatiotemporal Data* (manuscript under revision). Code and configuration archive:
> https://github.com/Lamberthandsome/MGSI, DOI: 10.5281/zenodo.XXXXXXX (to be minted at
> release v1.0.0).
