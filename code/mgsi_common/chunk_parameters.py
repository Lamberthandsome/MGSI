"""Logical chunk accounting, encoded HDFS spans, and bounded decode plans."""
from dataclasses import dataclass
import calendar

MIB=1024**2
HDFS_REFERENCE_BLOCK_BYTES=128*MIB
CACHE_POOL_BYTES=512*MIB
MANAGED_MEMORY_BYTES=2048*MIB
CACHE_PAGE_BYTES=32768
DECODE_BUFFER_BYTES=16*MIB

def positive(value,name):
    if type(value) is not int or value<=0:raise ValueError(name+' must be a positive integer')
    return value

@dataclass(frozen=True)
class ChunkShape:
    time: int
    latitude: int
    longitude: int
    bytes_per_value: int=4

    def __post_init__(self):
        for name,value in self.__dict__.items():positive(value,name)

    @property
    def cells(self):return self.time*self.latitude*self.longitude

    @property
    def decoded_bytes(self):return self.cells*self.bytes_per_value

    def clipped(self,time,latitude,longitude):
        return ChunkShape(min(self.time,positive(time,'time')),min(self.latitude,positive(latitude,'latitude')),
                          min(self.longitude,positive(longitude,'longitude')),self.bytes_per_value)

def dataset_shape(dataset,year=1981):
    if dataset=='MERRA-2':return ChunkShape(24,91,144)
    if dataset=='CHIRPS':return ChunkShape(1,1024,1024)
    if dataset=='ERA5':return ChunkShape(8784 if calendar.isleap(year) else 8760,91,144)
    if dataset=='NCEP/NCAR':return ChunkShape(12,94,192)
    if dataset=='GLEAM':return ChunkShape(366 if calendar.isleap(year) else 365,90,144)
    raise ValueError('Unknown dataset')

def slot_cache_budget(slots,cache_pool=CACHE_POOL_BYTES,page=CACHE_PAGE_BYTES):
    positive(slots,'slots');positive(cache_pool,'cache_pool');positive(page,'page')
    if page & (page-1):raise ValueError('Page size must be a power of two')
    budget=cache_pool//(slots*page)*page
    if not budget:raise ValueError('No complete page is available per slot')
    return budget

def page_charge(payload_bytes,index_bytes=0,page=CACHE_PAGE_BYTES):
    for value in [payload_bytes,index_bytes]:
        if type(value) is not int or value<0:raise ValueError('Invalid buffer length')
    positive(page,'page')
    total=payload_bytes+index_bytes
    return ((total+page-1)//page)*page if total else 0

def physical_spans(offset,length,block_bytes=HDFS_REFERENCE_BLOCK_BYTES):
    """Split an ENCODED byte range, never a decoded logical-cell volume."""
    for value in [offset,length]:
        if type(value) is not int or value<0:raise ValueError('Invalid physical range')
    positive(block_bytes,'block_bytes')
    spans=[]
    end=offset+length
    while offset<end:
        block=offset//block_bytes
        stop=min(end,(block+1)*block_bytes)
        spans.append((block,offset,stop-offset))
        offset=stop
    return spans

def slab_plan(shape,maximum_bytes=DECODE_BUFFER_BYTES):
    """Bounded decoded rectangles; preserves the parent logical chunk identity."""
    positive(maximum_bytes,'maximum_bytes')
    cells=maximum_bytes//shape.bytes_per_value
    if not cells:raise ValueError('Buffer cannot hold one value')
    width=min(shape.longitude,cells)
    height=min(shape.latitude,max(1,cells//width))
    times=min(shape.time,max(1,cells//(width*height)))
    for t in range(0,shape.time,times):
        for y in range(0,shape.latitude,height):
            for x in range(0,shape.longitude,width):
                yield ((t,y,x),(min(t+times,shape.time),min(y+height,shape.latitude),min(x+width,shape.longitude)))
