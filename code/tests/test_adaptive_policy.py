"""Local decision, lossless layout, handoff and recovery checks on tiny fixtures."""
from pathlib import Path
import importlib.util
import json
import math
import random
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event
import unittest

sys.dont_write_bytecode = True
sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[3]
WORK = ROOT / "MGSI-Reproduction/revision_work/P58_repartition_completed_20261013"
WORK.mkdir(parents=True,exist_ok=True)
module_path = Path(__file__).resolve().parents[1] / "mgsi_common/adaptive_policy.py"
spec = importlib.util.spec_from_file_location("adaptive_policy",module_path)
a = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = a
spec.loader.exec_module(a)
COUNTS = {"oracle_query_checks":0,"record_comparisons":0,"failure_cases":0,"sampling_checks":0}
TRACES = []
SEQUENCE = 0


def fixture(capacity=2,small=False):
    global SEQUENCE
    SEQUENCE += 1
    folder = WORK / "fixtures" / f"case-{SEQUENCE:03d}"
    # Every rerun has its own root; no recursive cleanup or mutation of old runs.
    while folder.exists():
        SEQUENCE += 1
        folder = WORK / "fixtures" / f"case-{SEQUENCE:03d}"
    store = a.FileBackend(folder)
    height,width = (8,8) if small else (16,16)
    rows = [[list((x,y)),None if (x*width+y)%11==0 else float(x*width+y-50)]
            for x in range(height) for y in range(width)]
    boxes = ([a.Box((x,0),(x+2,8)) for x in range(0,8,2)] if small else
             [a.Box((x,y),(x+4,y+8)) for x in range(0,16,4) for y in (0,8)])
    partitions = [store.create(i,"A" if i<len(boxes)//2 else "B",box,
                  [r for r in rows if box.contains(r[0])]) for i,box in enumerate(boxes)]
    controller = a.Controller(store,{"A":capacity,"B":capacity},partitions)
    return controller,rows


def samples(controller,t,active_a=2,active_b=1,wait_a=1.0,wait_b=0.0,rate=1048576.0,checkpoint=.01):
    return {"A":a.NodeSample(active_a,controller.capacities['A'],t,wait_a,rate,checkpoint),
            "B":a.NodeSample(active_b,controller.capacities['B'],t,wait_b,rate,checkpoint)}


def decision(controller,start=70,active_a=2,active_b=1,**kwargs):
    plan = None
    for t in range(start,start+15,5):
        plan = controller.decide(samples(controller,t,active_a,active_b,**kwargs),t)
        COUNTS["sampling_checks"] += 1
    return plan,t


def query(controller,now,predicate=lambda point:True,identifiers=None):
    q = controller.begin_query(identifiers)
    result = controller.read_query(q,now,predicate)
    controller.end_query(q)
    return result


def oracle_checks(test,controller,source,now,count=35):
    rng = random.Random(1457)
    for k in range(count):
        # Independent scan uses literal coordinate comparisons, not Box helpers.
        x0,x1 = sorted(rng.sample(range(-1,18),2))
        y0,y1 = sorted(rng.sample(range(-1,18),2))
        select = lambda p:x0<=p[0]<x1 and y0<=p[1]<y1
        expected = [r for r in source if x0<=r[0][0]<x1 and y0<=r[0][1]<y1]
        actual = query(controller,now,select)
        test.assertEqual(actual,expected)
        values = [r[1] for r in actual if r[1] is not None]
        reference = [r[1] for r in expected if r[1] is not None]
        test.assertEqual((sum(values),len(values)),(sum(reference),len(reference)))
        COUNTS['oracle_query_checks'] += 1
        COUNTS['record_comparisons'] += len(expected)


class DecisionTests(unittest.TestCase):
    def test_threshold_edges(self):
        c,_ = fixture(capacity=5)
        for t in range(70,95,5):
            self.assertIsNone(c.decide(samples(c,t,4,1),t))
        p,t = decision(c,start=95,active_a=5,active_b=1)
        self.assertEqual(p.kind,'split')
        self.assertEqual(len(p.placements),8)

    def test_consecutive_and_missing_samples(self):
        c,_ = fixture()
        for t,active in [(70,2),(75,2),(80,1),(85,2),(90,2)]:
            self.assertIsNone(c.decide(samples(c,t,active,1),t))
        self.assertEqual(c.decide(samples(c,95),95).kind,'split')
        other,_ = fixture()
        for t in [70,75,85,90]:
            self.assertIsNone(other.decide(samples(other,t),t))
        self.assertEqual(other.decide(samples(other,95),95).kind,'split')

    def test_stale_metrics_and_alignment(self):
        c,_ = fixture()
        for t in [70,75,80]:
            s = samples(c,t)
            s['A'] = a.NodeSample(2,2,t-11)
            self.assertIsNone(c.decide(s,t))
        with self.assertRaises(ValueError): c.decide(samples(c,83),83)
        with self.assertRaises(ValueError): c.decide(samples(c,80),80)

    def test_replica_route_tie_and_capacity(self):
        c,_ = fixture()
        self.assertEqual(c.initial_owner(['A','B'],{'A':1,'B':0}),'B')
        self.assertEqual(c.initial_owner(['A','B'],{'A':0,'B':0}),'A')
        with self.assertRaises(ValueError): c.initial_owner(['missing'],{})

    def test_sparse_exact_window_and_boundaries(self):
        w = a.WindowCounters(a.Policy())
        events = [0,4.99,5,59.99,60]
        for t in events: w.add(1,t)
        self.assertEqual(w.count(1,60),sum(0<=t<60 for t in events))
        w.expire(65)
        self.assertEqual(w.count(1,65),sum(5<=t<65 for t in events))
        for t in range(65,10001): w.add(1,t)
        self.assertLessEqual(len(w.buckets[1]),13)
        w.expire(10070)
        self.assertEqual(w.buckets,{})
        with self.assertRaises(ValueError): w.add(1,10069)

    def test_duplicate_chunk_completion_not_double_counted(self):
        c,_ = fixture()
        q = c.begin_query([0])
        one = c.read_query(q,10)
        two = c.read_query(q,10)
        c.end_query(q)
        self.assertEqual(one,two)
        self.assertEqual(c.counters.count(0,15),1)

    def test_split_merge_odd_shapes_and_boundaries(self):
        box = a.Box((0,0),(5,8))
        left,right = box.split(16,0)
        self.assertEqual((left.cells,right.cells),(16,24))
        self.assertEqual(left.joined(right),box)
        self.assertIsNone(a.Box((0,0),(4,4)).split(16))
        self.assertIsNone(left.joined(a.Box((8,0),(10,8))))
        with self.assertRaises(ValueError): a.Box((0,0),(0,8))
        self.assertFalse(left.contains((2,0)))
        self.assertTrue(right.contains((2,0)))

    def test_split_lossless_and_cooldown(self):
        c,rows = fixture()
        plan,t = decision(c)
        c.submit(plan,t)
        self.assertEqual(c.advance(t),'committed')
        self.assertEqual(sum(p.owner=='A' for p in c.chunks.values()),8)
        oracle_checks(self,c,rows,81)
        for t in range(85,140,5):
            self.assertIsNone(c.decide(samples(c,t),t))
        TRACES.append({'case':'split_cooldown','trace':c.trace,'actions':dict(c.action_counts)})

    def test_merge_lossless_and_minimum_partition(self):
        c,rows = fixture()
        plan,t = decision(c,active_a=0,active_b=1)
        self.assertEqual(plan.kind,'merge')
        c.submit(plan,t)
        self.assertEqual(c.advance(t),'committed')
        self.assertEqual(sum(p.owner=='A' for p in c.chunks.values()),2)
        oracle_checks(self,c,rows,81)
        other,source = fixture(small=True)
        for t in [0,5,10]: self.assertIsNone(other.decide(samples(other,t,0,1),t))
        plan,t = decision(other,start=70,active_a=0,active_b=1)
        other.submit(plan,t);other.advance(t)
        self.assertEqual(sum(p.owner=='A' for p in other.chunks.values()),1)
        for t in range(85,155,5): self.assertIsNone(other.decide(samples(other,t,0,1),t))

    def test_cache_budget_and_lfu_order(self):
        cache = a.LFUCache(20)
        cache.put((1,0),['one'],10);cache.put((2,0),['two'],10)
        cache.get((1,0));cache.put((3,0),['three'],10)
        self.assertIsNone(cache.get((2,0)))
        self.assertLessEqual(cache.used_bytes,20)
        cache.put((4,0),['too big'],21)
        self.assertNotIn((4,0),cache.entries)
        cache.invalidate([1]);self.assertNotIn((1,0),cache.entries)

    def test_frequency_window_independent_of_cache_residency(self):
        c,_ = fixture(small=True)
        for _ in range(20): query(c,65,identifiers=[0])
        mean = sum(c.counters.count(i,70) for i in c.chunks)/len(c.chunks)
        self.assertEqual(c.frequency_class(c.chunks[0],70,mean),'hot')
        self.assertEqual(c.frequency_class(c.chunks[1],70,mean),'cold')
        before = c.counters.count(0,70)
        for cache in c.caches.values(): cache.invalidate([0])
        self.assertEqual(c.counters.count(0,70),before)


def migration_fixture():
    c,rows = fixture(small=True)
    for _ in range(20): query(c,65,identifiers=[0])
    plan,t = decision(c,active_a=2,active_b=0)
    if plan is None or plan.kind!='migrate':
        raise AssertionError('Expected cost-admissible migration under synthetic inputs')
    return c,rows,plan,t


class HandoffTests(unittest.TestCase):
    def test_migration_lossless_preserves_frequency_invalidates_cache(self):
        c,rows,plan,t = migration_fixture()
        old = c.chunks[0]
        old_count = c.counters.count(0,t)
        c.submit(plan,t)
        self.assertEqual(c.advance(t),'committed')
        self.assertEqual(c.chunks[0].owner,'B')
        self.assertEqual(c.counters.count(0,t),old_count)
        self.assertTrue(Path(old.path).exists())
        self.assertEqual(c.backend.read(old),c.backend.read(c.chunks[0]))
        self.assertTrue(all(not any(key[0]==0 for key in cache.entries) for cache in c.caches.values()))
        self.assertTrue(all(epoch==c.epoch for epoch in c.views.values()))
        oracle_checks(self,c,rows,81)
        TRACES.append({'case':'migration','trace':c.trace,'actions':dict(c.action_counts),
                       'fixture_bytes':c.chunks[0].nbytes,'estimated_cost_input':plan.estimated_cost})

    def test_migration_cost_guard_requires_observations(self):
        c,_ = fixture(small=True)
        for _ in range(20): query(c,65,identifiers=[0])
        p,t = decision(c,active_a=2,active_b=0,wait_a=0.00001)
        self.assertTrue(p is None or p.kind!='migrate')
        other,_ = fixture(small=True)
        for _ in range(20): query(other,65,identifiers=[0])
        p,t = decision(other,active_a=2,active_b=0,rate=None)
        self.assertTrue(p is None or p.kind!='migrate')

    def test_inflight_query_drain_and_stale_generation(self):
        c,rows,plan,t = migration_fixture()
        old_epoch = c.epoch
        q = c.begin_query([0])
        c.submit(plan,t)
        self.assertEqual(c.advance(t),'waiting_for_queries')
        with self.assertRaises(a.AdmissionBlocked): c.begin_query([0])
        expected = [r for r in rows if 0<=r[0][0]<2 and 0<=r[0][1]<8]
        self.assertEqual(c.read_query(q,t),expected)
        c.end_query(q)
        self.assertEqual(c.advance(t+.5),'committed')
        with self.assertRaises(a.StaleLayout): c.begin_query([0],expected_epoch=old_epoch)
        oracle_checks(self,c,rows,81)

    def test_drain_timeout_retains_old_layout(self):
        c,rows,plan,t = migration_fixture()
        old_pointer = c.backend.pointer.read_bytes()
        q = c.begin_query([0])
        c.submit(plan,t)
        self.assertEqual(c.advance(t+30),'drain_timeout')
        self.assertEqual(c.backend.pointer.read_bytes(),old_pointer)
        self.assertEqual(c.epoch,0)
        c.end_query(q)
        oracle_checks(self,c,rows,t+31)

    def test_failures_before_commit_recover_old_data(self):
        for phase in ['stage','verify','checkpoint','publish_before']:
            with self.subTest(phase=phase):
                c,rows,plan,t = migration_fixture()
                before = c.backend.pointer.read_bytes()
                c.submit(plan,t)
                self.assertEqual(c.advance(t,fail=phase),'aborted')
                self.assertEqual(c.backend.pointer.read_bytes(),before)
                recovered = a.Controller(c.backend,c.capacities)
                self.assertEqual(recovered.epoch,0)
                oracle_checks(self,recovered,rows,90)
                COUNTS['failure_cases'] += 1

    def test_failures_after_commit_recover_new_data(self):
        for phase in ['publish_after','invalidate','broadcast']:
            with self.subTest(phase=phase):
                c,rows,plan,t = migration_fixture()
                c.submit(plan,t)
                with self.assertRaises(a.CommittedHandoffPending): c.advance(t,fail=phase)
                with self.assertRaises(a.AdmissionBlocked): c.begin_query([0])
                with self.assertRaises(a.CommittedHandoffPending): c.advance(t+1)
                recovered = a.Controller(c.backend,c.capacities)
                self.assertEqual(recovered.epoch,1)
                self.assertEqual(recovered.chunks[0].owner,'B')
                self.assertTrue(all(v==1 for v in recovered.views.values()))
                oracle_checks(self,recovered,rows,90)
                COUNTS['failure_cases'] += 1

    def test_new_split_ids_remain_fenced_until_acknowledgment(self):
        for phase in ['invalidate','broadcast']:
            with self.subTest(phase=phase):
                c,rows = fixture()
                plan,t = decision(c)
                affected = set(plan.affected)
                c.submit(plan,t)
                with self.assertRaises(a.CommittedHandoffPending): c.advance(t,fail=phase)
                replacements = set(c.chunks)-set(range(8))
                self.assertTrue(replacements)
                self.assertTrue(replacements <= c.blocked)
                with self.assertRaises(a.AdmissionBlocked): c.begin_query()
                with self.assertRaises(a.AdmissionBlocked): c.begin_query(replacements)
                # An unchanged partition can still serve a query while the
                # affected partitions wait for their generation acknowledgments.
                untouched = set(range(8))-affected
                q = c.begin_query(untouched)
                c.read_query(q,81)
                c.end_query(q)
                recovered = a.Controller(c.backend,c.capacities)
                oracle_checks(self,recovered,rows,90)

    def test_query_and_controller_threads_drain_existing_lease(self):
        c,rows,plan,t = migration_fixture()
        ready,released = Event(),Event()
        def worker():
            q = c.begin_query([0])
            ready.set()
            if not released.wait(timeout=5):
                raise AssertionError('Controller did not release the test worker')
            result = c.read_query(q,80)
            c.end_query(q)
            return result
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            self.assertTrue(ready.wait(timeout=5))
            try:
                c.submit(plan,t)
                self.assertEqual(c.advance(t),'waiting_for_queries')
                with self.assertRaises(a.AdmissionBlocked): c.begin_query([0])
            finally:
                released.set()
            actual = future.result(timeout=5)
        expected = [r for r in rows if 0<=r[0][0]<2 and 0<=r[0][1]<8]
        self.assertEqual(actual,expected)
        self.assertEqual(c.advance(81),'committed')
        oracle_checks(self,c,rows,82)

    def test_counter_processing_clock_survives_committed_recovery(self):
        c,rows,plan,t = migration_fixture()
        c.submit(plan,t)
        self.assertEqual(c.advance(t),'committed')
        recovered = a.Controller(c.backend,c.capacities)
        self.assertEqual(recovered.counters.latest_time,80)
        with self.assertRaises(ValueError): query(recovered,79,identifiers=[0])

    def test_retry_after_failed_stage_has_unique_artifacts(self):
        c,rows,plan,t = migration_fixture()
        c.submit(plan,t)
        self.assertEqual(c.advance(t,fail='stage'),'aborted')
        c.submit(plan,t+5)
        self.assertEqual(c.advance(t+5),'committed')
        oracle_checks(self,c,rows,90)

    def test_policy_and_checkpoint_input_validation(self):
        with self.assertRaises(ValueError): a.Policy(sample_seconds=7)
        with self.assertRaises(ValueError): a.Policy(cooldown_seconds=30)
        with self.assertRaises(ValueError): a.Policy(low_load=.9)
        c,_ = fixture()
        with self.assertRaises(ValueError): a.Controller(c.backend,{'A':3,'B':3})
        with self.assertRaises(ValueError): c.backend.checked(c.backend.root.parent/'outside.json')
        with self.assertRaises(ValueError):
            a.Controller(a.FileBackend(c.backend.root/'bad-bounds'),c.capacities,
                         list(c.chunks.values()),policy=a.Policy(max_partitions_per_slot=1))
        with self.assertRaises(ValueError):
            a.Controller(a.FileBackend(c.backend.root/'duplicate-ids'),c.capacities,
                         list(c.chunks.values())+[c.chunks[0]])

    def test_random_decisions_do_not_change_query_values(self):
        c,rows = fixture()
        rng = random.Random(621)
        for t in range(70,1070,5):
            # Fixed, labelled synthetic inputs; no cluster load measurements.
            for _ in range(rng.randrange(4)):
                query(c,t,identifiers=[rng.choice(list(c.chunks))])
            s = samples(c,t,active_a=rng.choice([0,0,2,2,2]),active_b=rng.choice([0,0,2,2]))
            plan = c.decide(s,t)
            COUNTS['sampling_checks'] += 1
            if plan:
                c.submit(plan,t)
                self.assertEqual(c.advance(t),'committed')
                for node in c.capacities:
                    self.assertLessEqual(sum(p.owner==node for p in c.chunks.values()),4*c.capacities[node])
                actual = query(c,t)
                self.assertEqual(actual,rows)
                COUNTS['oracle_query_checks'] += 1
                COUNTS['record_comparisons'] += len(rows)
        oracle_checks(self,c,rows,1075,count=100)
        TRACES.append({'case':'changing_synthetic_load','trace':c.trace,'actions':dict(c.action_counts)})


if __name__=='__main__':
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {'status':'PASS' if result.wasSuccessful() else 'FAIL','tests':result.testsRun,
              'failures':len(result.failures),'errors':len(result.errors),'checks':COUNTS,
              'scope':'Local synthetic grids and file-backed decision/handoff state machine; no Flink/HDFS performance or network migration measurements',
              'policy':a.asdict(a.Policy()),'traces':TRACES,'fixture_roots':SEQUENCE}
    (WORK/'correctness.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k not in ['traces','policy']},ensure_ascii=False))
    sys.exit(0 if result.wasSuccessful() else 1)
