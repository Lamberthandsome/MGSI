package mgsi.cache;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;

/** Slot quotas for decoded fragments and packed index descriptors.
 * Reservations belong to the real Flink MemoryManager. Native segments are
 * allocated only on admission and are freed before releasing the reservation.
 * The class does not submit jobs, decode formats, or change cluster services.
 */
public final class ManagedChunkCache implements AutoCloseable {
    public static final long DEFAULT_CACHE_BYTES = 512L << 20;
    public static final long DEFAULT_MANAGED_BYTES = 2L << 30;
    public static final int DECODE_BUFFER_BYTES = 16 << 20;
    private final MemoryManager memory;
    private final int slots, pageBytes;
    private final long quota;
    private final Map<Integer, Slot> owners = new HashMap<>();
    private boolean closed;

    public ManagedChunkCache(MemoryManager memory, int slots, long cacheBytes) {
        if (memory == null || slots <= 0 || cacheBytes <= 0 || cacheBytes > memory.getMemorySize())
            throw new IllegalArgumentException("Invalid managed cache configuration");
        this.memory = memory;
        this.slots = slots;
        this.pageBytes = memory.getPageSize();
        this.quota = cacheBytes / slots / pageBytes * pageBytes;
        if (quota <= 0) throw new IllegalArgumentException("Each slot needs a complete page");
    }

    public long quotaBytes() { return quota; }
    public int pageBytes() { return pageBytes; }
    public long totalQuotaBytes() { return quota * slots; }

    public synchronized Slot openSlot(int slot) {
        if (closed || slot < 0 || slot >= slots || owners.containsKey(slot))
            throw new IllegalStateException("Slot cannot acquire another cache owner");
        Slot owner = new Slot(slot);
        owners.put(slot, owner);
        return owner;
    }

    private synchronized void remove(int slot, Slot owner) {
        if (owners.get(slot) == owner) owners.remove(slot);
    }

    @Override public void close() {
        List<Slot> snapshot;
        synchronized (this) {
            closed = true;
            snapshot = new ArrayList<>(owners.values());
        }
        for (Slot owner : snapshot) owner.close();
    }

    public static final class Key implements Comparable<Key> {
        public final long chunk, generation;
        public final String variable;
        public final int firstTime, times, firstRow, rows, firstColumn, columns;
        public Key(long chunk, String variable, long generation, int firstTime, int times,
                   int firstRow, int rows, int firstColumn, int columns) {
            if (chunk < 0 || generation < 0 || variable == null || variable.isEmpty()
                || variable.getBytes(StandardCharsets.UTF_8).length > 255
                || firstTime < 0 || firstRow < 0 || firstColumn < 0 || times <= 0 || rows <= 0 || columns <= 0)
                throw new IllegalArgumentException("Invalid immutable fragment key");
            this.chunk=chunk; this.variable=variable; this.generation=generation;
            this.firstTime=firstTime; this.times=times; this.firstRow=firstRow; this.rows=rows;
            this.firstColumn=firstColumn; this.columns=columns;
        }
        @Override public int compareTo(Key other) {
            int c=Long.compare(chunk,other.chunk);
            if(c==0)c=variable.compareTo(other.variable);
            if(c==0)c=Long.compare(generation,other.generation);
            int[] a={firstTime,times,firstRow,rows,firstColumn,columns};
            int[] b={other.firstTime,other.times,other.firstRow,other.rows,other.firstColumn,other.columns};
            for(int i=0;c==0&&i<a.length;i++)c=Integer.compare(a[i],b[i]);
            return c;
        }
        @Override public boolean equals(Object other) {return other instanceof Key && compareTo((Key)other)==0;}
        @Override public int hashCode() {
            int result=Long.hashCode(chunk)*31+variable.hashCode();
            result=result*31+Long.hashCode(generation);
            for(int value:new int[]{firstTime,times,firstRow,rows,firstColumn,columns})result=result*31+value;
            return result;
        }
    }

    private static final class Entry {
        final Key key;
        final List<MemorySegment> pages;
        final long payload, charge;
        final int indexBytes;
        long frequency=1, lastUse;
        int leases;
        boolean retired;
        Entry(Key key,List<MemorySegment> pages,long payload,int indexBytes,long charge,long lastUse) {
            this.key=key;this.pages=pages;this.payload=payload;this.indexBytes=indexBytes;
            this.charge=charge;this.lastUse=lastUse;
        }
    }

    public final class Slot implements AutoCloseable {
        private final int identifier;
        private final Map<Key,Entry> entries=new HashMap<>();
        private final Set<Entry> retired=new HashSet<>();
        private boolean reserved, closing, released;
        private long used,sequence;

        private Slot(int identifier) {
            this.identifier=identifier;
            try {memory.reserveMemory(this,quota);reserved=true;}
            catch(MemoryReservationException unavailable) {reserved=false;}
        }
        public synchronized long budgetBytes() {return reserved?quota:0;}
        public synchronized long usedBytes() {return used;}
        public synchronized int entryCount() {return entries.size();}
        public synchronized boolean enabled() {return reserved&&!closing;}

        public synchronized Lease get(Key key) {
            if(closing)return null;
            Entry entry=entries.get(key);
            if(entry==null)return null;
            if(entry.frequency<Long.MAX_VALUE)entry.frequency++;
            entry.lastUse=++sequence;entry.leases++;
            return new Lease(this,entry);
        }

        public boolean put(Key key,byte[] payload,byte[] index) throws IOException {
            return putStream(key,payload.length,index,new ByteArrayInputStream(payload));
        }

        public synchronized boolean putStream(Key key,long payloadBytes,byte[] index,InputStream input) throws IOException {
            if(key==null||input==null||index==null||payloadBytes<0||index.length>pageBytes)
                throw new IllegalArgumentException("Invalid payload or packed descriptor");
            long total=Math.addExact(payloadBytes,index.length);
            if(total==0||!enabled()||entries.containsKey(key))return false;
            long charge=Math.multiplyExact((total-1)/pageBytes+1,(long)pageBytes);
            if(charge>quota)return false;
            long reclaimable=0;
            for(Entry entry:entries.values())if(entry.leases==0)reclaimable+=entry.charge;
            if(used+charge-reclaimable>quota)return false;
            while(used+charge>quota) {
                Entry victim=entries.values().stream().filter(e->e.leases==0)
                    .min(Comparator.comparingLong((Entry e)->e.frequency).thenComparingLong(e->e.lastUse)
                         .thenComparing(e->e.key)).get();
                entries.remove(victim.key);free(victim);
            }
            List<MemorySegment> pages=new ArrayList<>();
            try {
                int count=Math.toIntExact(charge/pageBytes);
                for(int i=0;i<count;i++) {
                    MemorySegment segment=MemorySegmentFactory.allocateOffHeapUnsafeMemory(pageBytes,this,()->{});
                    if(!segment.isOffHeap())throw new IllegalStateException("Native segment required");
                    pages.add(segment);
                }
                byte[] buffer=new byte[(int)Math.min(DECODE_BUFFER_BYTES,Math.max(1,payloadBytes))];
                long offset=0;
                while(offset<payloadBytes) {
                    int amount=(int)Math.min(buffer.length,payloadBytes-offset),read=0;
                    while(read<amount) {
                        int n=input.read(buffer,read,amount-read);
                        if(n<0)throw new EOFException("Decoded payload ended before declared extent");
                        if(n==0) {
                            int value=input.read();if(value<0)throw new EOFException("Short decoded payload");
                            buffer[read++]=(byte)value;
                        } else read+=n;
                    }
                    write(pages,offset,buffer,0,amount);offset+=amount;
                }
                write(pages,payloadBytes,index,0,index.length);
                Entry entry=new Entry(key,pages,payloadBytes,index.length,charge,++sequence);
                entries.put(key,entry);used+=charge;
                return true;
            } catch(IOException|RuntimeException|OutOfMemoryError failure) {
                for(MemorySegment page:pages)page.free();
                if(failure instanceof IOException)throw (IOException)failure;
                if(failure instanceof OutOfMemoryError)return false;
                throw (RuntimeException)failure;
            }
        }

        public synchronized void invalidateChunks(Collection<Long> chunks) {
            for(Entry entry:new ArrayList<>(entries.values()))
                if(chunks.contains(entry.key.chunk))retire(entry);
        }

        private void retire(Entry entry) {
            entries.remove(entry.key);entry.retired=true;
            if(entry.leases==0)free(entry);else retired.add(entry);
        }
        private void free(Entry entry) {
            for(MemorySegment page:entry.pages)page.free();
            used-=entry.charge;retired.remove(entry);
        }
        private synchronized void release(Entry entry) {
            if(entry.leases<=0)throw new IllegalStateException("Lease already released");
            entry.leases--;
            if(entry.retired&&entry.leases==0)free(entry);
            finishClose();
        }
        private void finishClose() {
            if(closing&&!released&&used==0) {
                if(reserved)memory.releaseMemory(this,quota);
                reserved=false;released=true;remove(identifier,this);
            }
        }
        @Override public synchronized void close() {
            if(!closing) {
                closing=true;
                for(Entry entry:new ArrayList<>(entries.values()))retire(entry);
            }
            finishClose();
        }
    }

    private void write(List<MemorySegment> pages,long offset,byte[] source,int start,int length) {
        while(length>0) {
            int number=Math.toIntExact(offset/pageBytes),within=(int)(offset%pageBytes);
            int n=Math.min(length,pageBytes-within);
            pages.get(number).put(within,source,start,n);
            offset+=n;start+=n;length-=n;
        }
    }

    public final class Lease implements AutoCloseable {
        private final Slot owner;
        private final Entry entry;
        private boolean ended;
        private Lease(Slot owner,Entry entry) {this.owner=owner;this.entry=entry;}
        public long payloadBytes() {return entry.payload;}
        public boolean nativeBacking() {return entry.pages.stream().allMatch(MemorySegment::isOffHeap);}
        public InputStream payload() {return view(0,entry.payload);}
        public InputStream index() {return view(entry.payload,entry.indexBytes);}
        private InputStream view(long start,long length) {
            return new InputStream() {
                private long at;
                @Override public int read() throws IOException {
                    byte[] one=new byte[1];int n=read(one,0,1);return n<0?-1:one[0]&255;
                }
                @Override public int read(byte[] output,int offset,int length) throws IOException {
                    if(output==null)throw new NullPointerException("output");
                    if(offset<0||length<0||offset>output.length-length)throw new IndexOutOfBoundsException();
                    synchronized(owner) {
                        if(ended)throw new IOException("Read lease has ended");
                        if(length==0)return 0;
                        if(at>=lengthLimit)return -1;
                        int wanted=(int)Math.min(Math.min(length,DECODE_BUFFER_BYTES),lengthLimit-at);
                        int left=wanted,destination=offset;
                        long position=start+at;
                        while(left>0) {
                            int number=Math.toIntExact(position/pageBytes),within=(int)(position%pageBytes);
                            int n=Math.min(left,pageBytes-within);
                            entry.pages.get(number).get(within,output,destination,n);
                            position+=n;destination+=n;left-=n;
                        }
                        at+=wanted;return wanted;
                    }
                }
                private final long lengthLimit=length;
            };
        }
        @Override public void close() {
            synchronized(owner) {if(!ended){ended=true;owner.release(entry);}}
        }
    }
}
