# Managed chunk cache

`ManagedChunkCache` is the native-memory cache component for the explicit slot-quota
policy. An embedding Flink operator supplies its real TaskManager `MemoryManager`,
configured slot count, and the JVM cache-pool byte budget. Open one owner per index
slot, pass bounded decoder streams to `putStream`, and close all read leases before
releasing an owner. Cache bypass leaves source-query execution available.

`managed-cache-profile.properties` records the four-slot reference policy. Its 512 MiB
cache pool is inside 2 GiB of managed memory, not an addition to the 8 GiB process
budget. Cached native payload plus packed descriptors is page charged; heap keys,
containers, and KD-tree objects are additional. A logical chunk may have several
bounded cache fragments. The class does not submit jobs or decode scientific formats.
