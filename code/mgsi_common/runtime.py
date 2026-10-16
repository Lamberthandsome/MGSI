"""Flink 批处理表环境工厂 + 结果记录器。

flink_env(): 与原脚本相同的 EnvironmentSettings/BatchTableEnvironment/内存配置。
record(): 把一次实验的分段计时追加到 results/raw/<experiment>.csv（原计时 + 修正计时双列）。
"""
import csv
import os
import time

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def flink_env():
    from pyflink.table import EnvironmentSettings, BatchTableEnvironment
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")
    return t_env


_RAW_DIR = os.path.join(_PROJECT_ROOT, 'results', 'raw')


class Recorder:
    """分段计时记录器：mark(name) 记一个时间点，finish() 写 CSV。"""

    def __init__(self, experiment, method, extra=None):
        self.experiment = experiment
        self.method = method
        self.extra = extra or {}
        self.marks = {}
        self._last = time.time()

    def mark(self, name):
        now = time.time()
        self.marks[name] = now - self._last
        self._last = now

    def mark_total(self, name, seconds):
        self.marks[name] = seconds

    def finish(self):
        os.makedirs(_RAW_DIR, exist_ok=True)
        path = os.path.join(_RAW_DIR, f"{self.experiment}.csv")
        file_exists = os.path.exists(path)
        fieldnames = ['experiment', 'method'] + list(self.marks.keys()) + list(self.extra.keys())
        with open(path, 'a', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                w.writeheader()
            row = {'experiment': self.experiment, 'method': self.method}
            row.update({k: f"{v:.4f}" for k, v in self.marks.items()})
            row.update(self.extra)
            w.writerow(row)
        print(f"[记录] 已追加到 {path}")
