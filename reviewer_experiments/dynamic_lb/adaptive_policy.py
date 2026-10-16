"""Deterministic chunk policy and a file-backed handoff correctness backend.

The policy changes logical work partitions under fixed execution-slot budgets.
FileBackend implements the storage/checkpoint interfaces on local fixtures;
it does not submit Flink jobs, operate HDFS, or measure distributed performance.
"""
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from collections import defaultdict
from functools import wraps
import hashlib
import json
import math
import os
import re
from threading import RLock
import uuid


@dataclass(frozen=True)
class Policy:
    sample_seconds: int = 5
    window_seconds: int = 60
    consecutive_samples: int = 3
    cooldown_seconds: int = 60
    drain_timeout_seconds: int = 30
    low_load: float = 0.2
    high_load: float = 0.8
    minimum_cells: int = 16
    max_partitions_per_slot: int = 4
    hot_minimum_accesses: int = 4
    hot_mean_multiplier: float = 2.0
    cold_mean_multiplier: float = 0.5
    migration_cost_margin: float = 1.5

    def __post_init__(self):
        integers = (self.sample_seconds, self.window_seconds, self.consecutive_samples,
                    self.cooldown_seconds, self.drain_timeout_seconds, self.minimum_cells,
                    self.max_partitions_per_slot, self.hot_minimum_accesses)
        if any(type(x) is not int or x <= 0 for x in integers):
            raise ValueError("Policy counts and intervals must be positive integers")
        if self.window_seconds % self.sample_seconds or self.cooldown_seconds < self.window_seconds:
            raise ValueError("Use integral window buckets and cooldown at least one window")
        if not 0 <= self.low_load < self.high_load <= 1:
            raise ValueError("Invalid load thresholds")
        if not (self.hot_mean_multiplier > 1 and 0 <= self.cold_mean_multiplier < 1
                and self.migration_cost_margin >= 1):
            raise ValueError("Invalid frequency or cost controls")


@dataclass(frozen=True)
class Box:
    low: tuple
    high: tuple

    def __post_init__(self):
        if not self.low or len(self.low) != len(self.high):
            raise ValueError("Invalid dimensionality")
        if any(type(x) is not int for x in self.low + self.high):
            raise ValueError("Logical grid bounds must be integer cell indexes")
        if any(a >= b for a, b in zip(self.low, self.high)):
            raise ValueError("Empty or reversed logical partition")

    @property
    def cells(self):
        return math.prod(b - a for a, b in zip(self.low, self.high))

    def contains(self, point):
        return len(point) == len(self.low) and all(a <= x < b for a, b, x in zip(self.low, self.high, point))

    def split(self, minimum_cells, preferred_axis=None):
        axes = sorted(range(len(self.low)), key=lambda d: (-(self.high[d] - self.low[d]), d))
        if preferred_axis in axes:
            axes.remove(preferred_axis)
            axes.insert(0, preferred_axis)
        for d in axes:
            if self.high[d] - self.low[d] < 2:
                continue
            mid = (self.low[d] + self.high[d]) // 2
            hi, lo = list(self.high), list(self.low)
            hi[d], lo[d] = mid, mid
            left, right = Box(self.low, tuple(hi)), Box(tuple(lo), self.high)
            if min(left.cells, right.cells) >= minimum_cells:
                return left, right
        return None

    def joined(self, other):
        if len(self.low) != len(other.low):
            return None
        differing = [d for d in range(len(self.low))
                     if (self.low[d], self.high[d]) != (other.low[d], other.high[d])]
        if len(differing) != 1:
            return None
        d = differing[0]
        if self.high[d] != other.low[d] and other.high[d] != self.low[d]:
            return None
        return Box(tuple(min(a,b) for a,b in zip(self.low,other.low)),
                   tuple(max(a,b) for a,b in zip(self.high,other.high)))


@dataclass(frozen=True)
class Partition:
    identifier: int
    owner: str
    box: Box
    path: str
    checksum: str
    nbytes: int
    born: float = 0.0
    generation: int = 0
    variable: str = "field"
    replicas: tuple = ()

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        data = dict(data)
        data["box"] = Box(tuple(data["box"]["low"]), tuple(data["box"]["high"]))
        data["replicas"] = tuple(data["replicas"])
        return cls(**data)


@dataclass(frozen=True)
class NodeSample:
    active: int
    capacity: int
    observed_at: float
    queue_wait_seconds: float = 0.0
    transfer_bytes_per_second: float | None = None
    checkpoint_seconds: float | None = None

    def valid(self, now, policy):
        numbers = (self.observed_at, self.queue_wait_seconds)
        return (type(self.active) is int and type(self.capacity) is int
                and 0 <= self.active <= self.capacity and self.capacity > 0
                and all(math.isfinite(x) for x in numbers) and self.queue_wait_seconds >= 0
                and 0 <= now-self.observed_at <= 2*policy.sample_seconds)

    @property
    def load(self):
        return self.active / self.capacity


class WindowCounters:
    """Exact access counts on aligned complete buckets, plus the current bucket."""
    def __init__(self, policy):
        self.policy = policy
        self.buckets = {}
        self.latest_time = 0.0

    def expire(self, now):
        if not math.isfinite(now) or now < self.latest_time:
            raise ValueError("Counter processing time must be finite and nondecreasing")
        self.latest_time = now
        first = int(now // self.policy.sample_seconds) - self.policy.window_seconds // self.policy.sample_seconds
        for identifier in list(self.buckets):
            self.buckets[identifier] = {k:v for k,v in self.buckets[identifier].items() if k >= first}
            if not self.buckets[identifier]:
                del self.buckets[identifier]

    def add(self, identifier, now):
        if not math.isfinite(now) or now < 0:
            raise ValueError("Invalid access time")
        self.expire(now)
        bucket = int(now // self.policy.sample_seconds)
        lane = self.buckets.setdefault(identifier, {})
        if lane.get(bucket, 0) >= 2**64-1:
            raise OverflowError("Unsigned counter capacity exceeded")
        lane[bucket] = lane.get(bucket, 0) + 1

    def count(self, identifier, now):
        current = int(now // self.policy.sample_seconds)
        first = current - self.policy.window_seconds // self.policy.sample_seconds
        return sum(v for k,v in self.buckets.get(identifier, {}).items() if first <= k < current)

    def serialized(self):
        return {str(c): {str(k):v for k,v in lane.items()} for c,lane in self.buckets.items()}

    def restore(self, value, latest_time=None):
        self.buckets = {int(c): {int(k):int(v) for k,v in lane.items()} for c,lane in value.items()}
        last_bucket = max((k for lane in self.buckets.values() for k in lane), default=0)
        self.latest_time = last_bucket*self.policy.sample_seconds if latest_time is None else latest_time


class LFUCache:
    """A decoded-payload cache; budget counts payload bytes, not Python overhead."""
    def __init__(self, byte_budget):
        self.byte_budget = byte_budget
        self.entries = {}
        self.sequence = 0

    @property
    def used_bytes(self):
        return sum(v[1] for v in self.entries.values())

    def get(self, key):
        self.sequence += 1
        entry = self.entries.get(key)
        if entry is None:
            return None
        self.entries[key] = (entry[0],entry[1],entry[2]+1,self.sequence)
        return entry[0]

    def put(self, key, rows, nbytes):
        if nbytes > self.byte_budget:
            return
        self.entries.pop(key,None)
        while self.entries and self.used_bytes+nbytes > self.byte_budget:
            victim = min(self.entries, key=lambda k:(self.entries[k][2],self.entries[k][3],k))
            del self.entries[victim]
        self.sequence += 1
        self.entries[key] = (rows,nbytes,1,self.sequence)

    def invalidate(self, identifiers):
        for key in list(self.entries):
            if key[0] in identifiers:
                del self.entries[key]


class AdmissionBlocked(RuntimeError): pass
class StaleLayout(RuntimeError): pass
class InjectedFailure(RuntimeError): pass
class CommittedHandoffPending(RuntimeError): pass


@dataclass(frozen=True)
class Placement:
    parents: tuple
    owner: str
    box: Box
    preserve_id: bool = False


@dataclass(frozen=True)
class Plan:
    kind: str
    placements: tuple
    affected: tuple
    nodes: tuple
    epoch: int
    estimated_gain: float = 0.0
    estimated_cost: float = 0.0


class FileBackend:
    """Local immutable payloads and atomic metadata/checkpoint journal."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True,exist_ok=True)
        self.pointer = self.root / "layout.json"

    def checked(self, path):
        path = Path(path).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("Payload outside this backend")
        return path

    def atomic_json(self, path, value):
        path = self.checked(path)
        temp = path.with_suffix(path.suffix+".tmp")
        with temp.open("wb") as stream:
            stream.write(json.dumps(value,sort_keys=True,allow_nan=False).encode())
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp,path)

    def create(self, identifier, owner, box, rows, born=0.0, generation=0, variable="field", transaction="initial"):
        if not re.fullmatch(r"[A-Za-z0-9_-]+",owner):
            raise ValueError("Invalid node identifier")
        rows = sorted(rows,key=lambda r:tuple(r[0]))
        if len(rows) != box.cells or len({tuple(r[0]) for r in rows}) != box.cells:
            raise ValueError("Payload does not exactly cover its logical cells")
        if any(not box.contains(r[0]) for r in rows):
            raise ValueError("Cell outside logical partition")
        directory = self.root / owner
        directory.mkdir(exist_ok=True)
        path = directory / f"{transaction}-c{identifier}-g{generation}.json"
        data = json.dumps(rows,sort_keys=True,allow_nan=False).encode()
        with path.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        return Partition(identifier,owner,box,str(path),hashlib.sha256(data).hexdigest(),len(data),born,generation,variable,(owner,))

    def read(self, partition):
        data = self.checked(partition.path).read_bytes()
        if hashlib.sha256(data).hexdigest() != partition.checksum:
            raise ValueError("Payload digest mismatch")
        return json.loads(data)

    def checkpoint(self, epoch, old, new):
        self.atomic_json(self.root / f"checkpoint-{epoch}.json",{"old":old,"new":new,"ready":True})

    def publish(self, snapshot):
        self.atomic_json(self.pointer,snapshot)


def serialized_call(method):
    """Serialize coordinator and query-admission calls across local threads."""
    @wraps(method)
    def call(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return call


class Controller:
    def __init__(self, backend, capacities, partitions=None, policy=None, cache_budget=65536):
        self._lock = RLock()
        self.backend = backend
        self.policy = policy or Policy()
        self.capacities = dict(capacities)
        if not self.capacities or any(type(v) is not int or v<1 for v in self.capacities.values()):
            raise ValueError("Invalid execution-slot capacities")
        self.counters = WindowCounters(self.policy)
        initial = list(partitions or ())
        self.chunks = {p.identifier:p for p in initial}
        if len(self.chunks) != len(initial):
            raise ValueError("Partition identifiers must be unique")
        self.epoch = 0
        self.next_id = max(self.chunks,default=-1)+1
        self.cooldown = {n:0.0 for n in capacities}
        self.last_sample = None
        self.streaks = {n:[0,0] for n in capacities}
        self.leases = {}
        self.next_query = 0
        self.pending = None
        self.blocked = set()
        self.trace = []
        self.action_counts = defaultdict(int)
        self.caches = {(n,i):LFUCache(cache_budget) for n,c in capacities.items() for i in range(c)}
        if backend.pointer.exists():
            snapshot = json.loads(backend.pointer.read_text())
            self._install(snapshot)
            checkpoint = backend.root / f"checkpoint-{self.epoch+1}.json"
            if checkpoint.exists():
                prior = json.loads(checkpoint.read_text())["old"]
                if prior["epoch"] == self.epoch:
                    self.counters.restore(prior["counters"], prior.get("counter_time"))
                    self.next_id = max(self.next_id,prior["next_id"])
        else:
            self.validate(self.chunks)
            backend.publish(self._snapshot())
        self.views = {n:self.epoch for n in capacities}

    def validate(self, chunks):
        for chunk in chunks.values():
            if chunk.owner not in self.capacities:
                raise ValueError("Unknown owner")
        for node,capacity in self.capacities.items():
            if sum(chunk.owner==node for chunk in chunks.values()) > self.policy.max_partitions_per_slot*capacity:
                raise ValueError("Owned work partitions exceed the configured bound")
        values = list(chunks.values())
        for i,a in enumerate(values):
            for b in values[i+1:]:
                if a.variable == b.variable and all(max(x,y)<min(u,v) for x,u,y,v in zip(a.box.low,a.box.high,b.box.low,b.box.high)):
                    raise ValueError("Overlapping logical partitions")

    def _snapshot(self, chunks=None, epoch=None, counters=None, cooldown=None):
        return {"epoch":self.epoch if epoch is None else epoch,"next_id":self.next_id,
                "capacities":self.capacities,"policy":asdict(self.policy),
                "chunks":[p.to_dict() for p in (self.chunks if chunks is None else chunks).values()],
                "counters":self.counters.serialized() if counters is None else counters,
                "counter_time":self.counters.latest_time,
                "cooldown":self.cooldown if cooldown is None else cooldown}

    def _install(self, snapshot):
        if snapshot["capacities"] != self.capacities or snapshot["policy"] != asdict(self.policy):
            raise ValueError("Checkpoint configuration mismatch")
        self.chunks = {p["identifier"]:Partition.from_dict(p) for p in snapshot["chunks"]}
        self.validate(self.chunks)
        self.epoch,self.next_id = snapshot["epoch"],snapshot["next_id"]
        self.counters.restore(snapshot["counters"], snapshot.get("counter_time"))
        self.cooldown = dict(snapshot["cooldown"])

    def initial_owner(self, replica_nodes, assigned):
        eligible = set(replica_nodes) & self.capacities.keys()
        if not eligible:
            raise ValueError("No registered storage node")
        return min(eligible,key=lambda n:(assigned.get(n,0)/self.capacities[n],n))

    @serialized_call
    def begin_query(self, identifiers=None, expected_epoch=None):
        if expected_epoch is not None and expected_epoch != self.epoch:
            raise StaleLayout("Refresh the query's index generation")
        identifiers = tuple(sorted(set(self.chunks if identifiers is None else identifiers)))
        if set(identifiers) & self.blocked:
            raise AdmissionBlocked("Defer this query until the handoff barrier clears")
        if any(c not in self.chunks for c in identifiers):
            raise StaleLayout("Candidate IDs no longer belong to this layout")
        self.next_query += 1
        self.leases[self.next_query] = {"partitions":{c:self.chunks[c] for c in identifiers},"counted":set()}
        return self.next_query

    @serialized_call
    def read_query(self, query, now, predicate=lambda point:True):
        lease = self.leases[query]
        output = []
        for identifier,chunk in lease["partitions"].items():
            cache = self.caches[(chunk.owner,identifier % self.capacities[chunk.owner])]
            key = (identifier,chunk.generation)
            rows = cache.get(key)
            if rows is None:
                rows = self.backend.read(chunk)
                cache.put(key,rows,chunk.nbytes)
            if identifier not in lease["counted"]:
                self.counters.add(identifier,now)
                lease["counted"].add(identifier)
            output.extend(r for r in rows if predicate(tuple(r[0])))
        return sorted(output,key=lambda r:tuple(r[0]))

    @serialized_call
    def end_query(self, query):
        del self.leases[query]

    def frequency_class(self, chunk, now, mean):
        count = self.counters.count(chunk.identifier,now)
        if count >= max(self.policy.hot_minimum_accesses,self.policy.hot_mean_multiplier*mean):
            return "hot"
        if now-chunk.born >= self.policy.window_seconds and count <= self.policy.cold_mean_multiplier*mean:
            return "cold"
        return "normal"

    @serialized_call
    def decide(self, samples, now, preferred_axis=None):
        if not math.isfinite(now) or now<0 or now % self.policy.sample_seconds:
            raise ValueError("Decision time must be a nonnegative aligned sampling boundary")
        if self.last_sample is not None and now <= self.last_sample:
            raise ValueError("Sampling time must increase")
        if self.last_sample is not None and now-self.last_sample != self.policy.sample_seconds:
            self.streaks = {n:[0,0] for n in self.capacities}
        self.last_sample = now
        self.counters.expire(now)
        valid = {}
        for node in self.capacities:
            s = samples.get(node)
            if s is None or s.capacity != self.capacities[node] or not s.valid(now,self.policy) or now<self.cooldown[node]:
                self.streaks[node] = [0,0]
                continue
            valid[node] = s
            self.streaks[node] = [self.streaks[node][0]+1 if s.load>self.policy.high_load else 0,
                                  self.streaks[node][1]+1 if s.load<self.policy.low_load else 0]
        if self.pending:
            return None
        mean = sum(self.counters.count(c,now) for c in self.chunks)/max(1,len(self.chunks))
        high = sorted((n for n in valid if self.streaks[n][0]>=self.policy.consecutive_samples),key=lambda n:(-valid[n].load,n))
        low = sorted((n for n in valid if self.streaks[n][1]>=self.policy.consecutive_samples),key=lambda n:(valid[n].load,n))
        for node in high:
            owned = [p for p in self.chunks.values() if p.owner==node]
            target = min(2*len(owned),self.policy.max_partitions_per_slot*self.capacities[node])
            candidates = sorted(owned,key=lambda p:(-self.counters.count(p.identifier,now),-p.box.cells,p.identifier))
            placements = []
            affected = []
            for p in candidates:
                if len(affected)>=target-len(owned):
                    break
                halves = p.box.split(self.policy.minimum_cells,preferred_axis)
                if halves:
                    affected.append(p.identifier)
                    placements.extend(Placement((p.identifier,),node,b) for b in halves)
            if placements:
                return Plan("split",tuple(placements),tuple(affected),(node,),self.epoch)
            if len(owned)<=1:
                continue
            for p in candidates:
                if self.frequency_class(p,now,mean)!="hot":
                    continue
                s = valid[node]
                if s.transfer_bytes_per_second is None or s.checkpoint_seconds is None:
                    continue
                if not math.isfinite(s.transfer_bytes_per_second) or s.transfer_bytes_per_second<=0 or not math.isfinite(s.checkpoint_seconds) or s.checkpoint_seconds<0:
                    continue
                destinations = sorted((n for n in low if n!=node),key=lambda n:(n not in p.replicas,valid[n].load,n))
                for dest in destinations:
                    dest_count = sum(q.owner==dest for q in self.chunks.values())
                    if dest_count >= self.policy.max_partitions_per_slot*self.capacities[dest]:
                        continue
                    # Use encoded descriptor/counter bytes in the cost model;
                    # logical bucket sizes alone omit serialization overhead.
                    handoff_state = {"partition":p.to_dict(),
                                     "buckets":self.counters.buckets.get(p.identifier,{}),
                                     "counter_time":self.counters.latest_time}
                    state_bytes = len(json.dumps(handoff_state,sort_keys=True,
                                                separators=(",",":"),allow_nan=False).encode())
                    cost = (p.nbytes+state_bytes)/s.transfer_bytes_per_second+s.checkpoint_seconds
                    gain = self.counters.count(p.identifier,now)*max(0,s.queue_wait_seconds-valid[dest].queue_wait_seconds)
                    if gain > self.policy.migration_cost_margin*cost:
                        return Plan("migrate",(Placement((p.identifier,),dest,p.box,True),),(p.identifier,),(node,dest),self.epoch,gain,cost)
        for node in low:
            owned = sorted((p for p in self.chunks.values() if p.owner==node),key=lambda p:p.identifier)
            groups = [((p.identifier,),p.box,p.variable) for p in owned]
            target = max(1,len(owned)//2)
            while len(groups)>target:
                pair = None
                for i,a in enumerate(groups):
                    if any(self.frequency_class(self.chunks[c],now,mean)!="cold" for c in a[0]):
                        continue
                    for j in range(i+1,len(groups)):
                        b = groups[j]
                        box = a[1].joined(b[1]) if a[2]==b[2] else None
                        if box and all(self.frequency_class(self.chunks[c],now,mean)=="cold" for c in b[0]):
                            pair = (i,j,(tuple(sorted(a[0]+b[0])),box,a[2]))
                            break
                    if pair:
                        break
                if not pair:
                    break
                i,j,group = pair
                groups = [g for k,g in enumerate(groups) if k not in (i,j)]+[group]
                groups.sort(key=lambda g:g[0])
            combined = [g for g in groups if len(g[0])>1]
            if combined:
                return Plan("merge",tuple(Placement(g[0],node,g[1]) for g in combined),
                            tuple(sorted(c for g in combined for c in g[0])),(node,),self.epoch)
        return None

    @serialized_call
    def submit(self, plan, now):
        if self.pending or plan.epoch!=self.epoch or not set(plan.affected)<=self.chunks.keys():
            raise StaleLayout("Plan cannot reserve this layout")
        self.pending = {"plan":plan,"started":now,"published":False,"transaction":"tx-"+uuid.uuid4().hex}
        self.blocked = set(plan.affected)
        self.trace.append({"time":now,"event":"barrier","kind":plan.kind,"epoch":self.epoch})

    @serialized_call
    def advance(self, now, fail=None):
        if not self.pending:
            return "idle"
        if self.pending["published"]:
            raise CommittedHandoffPending("Recover committed metadata before resuming the handoff")
        plan = self.pending["plan"]
        active = any(set(q["partitions"]) & self.blocked for q in self.leases.values())
        if active:
            if now-self.pending["started"] < self.policy.drain_timeout_seconds:
                return "waiting_for_queries"
            self._abort(now)
            return "drain_timeout"
        txid = self.pending["transaction"]
        try:
            old = self._snapshot()
            before_rows = sorted((r for c in plan.affected for r in self.backend.read(self.chunks[c])),key=lambda r:tuple(r[0]))
            replacement = []
            for placement in plan.placements:
                rows = [r for c in placement.parents for r in self.backend.read(self.chunks[c]) if placement.box.contains(r[0])]
                parent = self.chunks[placement.parents[0]]
                identifier = parent.identifier if placement.preserve_id else self.next_id
                if not placement.preserve_id:
                    self.next_id += 1
                replacement.append(self.backend.create(identifier,placement.owner,placement.box,rows,
                                   parent.born if placement.preserve_id else now,self.epoch+1,parent.variable,txid))
                if fail=="stage":
                    raise InjectedFailure("staging failure")
            if fail=="verify":
                Path(replacement[0].path).write_bytes(b"corrupt fixture")
            after_rows = sorted((r for p in replacement for r in self.backend.read(p)),key=lambda r:tuple(r[0]))
            if before_rows != after_rows:
                raise ValueError("Layout change lost or duplicated records")
            catalog = {c:p for c,p in self.chunks.items() if c not in plan.affected}
            catalog.update({p.identifier:p for p in replacement})
            self.validate(catalog)
            buckets = self.counters.serialized()
            for c in plan.affected:
                if plan.kind!="migrate":
                    buckets.pop(str(c),None)
            cooldown = dict(self.cooldown)
            for n in plan.nodes:
                cooldown[n] = now+self.policy.cooldown_seconds
            new = self._snapshot(catalog,self.epoch+1,buckets,cooldown)
            if fail=="checkpoint":
                raise InjectedFailure("checkpoint failure")
            self.backend.checkpoint(self.epoch+1,old,new)
            self.trace.append({"time":now,"event":"checkpoint_ready","kind":plan.kind})
            if fail=="publish_before":
                raise InjectedFailure("publication failure")
            # Replacement IDs must stay fenced after the commit point until
            # every cache/index view acknowledges the new layout generation.
            self.blocked.update(p.identifier for p in replacement)
            self.backend.publish(new)
            self.pending["published"] = True
            if fail=="publish_after":
                raise InjectedFailure("failure after commit point")
            self._install(new)
            if fail=="invalidate":
                raise InjectedFailure("cache acknowledgment failure")
            for cache in self.caches.values():
                cache.invalidate(plan.affected)
            for n in self.views:
                self.views[n] = self.epoch
                if fail=="broadcast":
                    raise InjectedFailure("index acknowledgment failure")
            self.trace.append({"time":now,"event":"committed","kind":plan.kind,"epoch":self.epoch})
            self.action_counts[plan.kind] += 1
            self.streaks = {n:[0,0] for n in self.capacities}
            self.pending,self.blocked = None,set()
            return "committed"
        except Exception as exc:
            if self.pending["published"]:
                raise CommittedHandoffPending("Recover the published layout before reopening admissions") from exc
            self._abort(now)
            return "aborted"

    def _abort(self, now):
        plan = self.pending["plan"]
        for n in plan.nodes:
            self.cooldown[n] = max(self.cooldown[n],now+self.policy.sample_seconds)
        self.streaks = {n:[0,0] for n in self.capacities}
        self.trace.append({"time":now,"event":"aborted","kind":plan.kind,"epoch":self.epoch})
        self.pending,self.blocked = None,set()
