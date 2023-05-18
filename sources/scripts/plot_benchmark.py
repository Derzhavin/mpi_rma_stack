import pathlib
import re
import math

import numpy as np
from matplotlib import pyplot as plt

LOGS_PATH = "../data/decentralized/random_operation_0/"
PLOT_PATH = "../data/decentralized/"
TOTAL_OPS = 15000

logs_path = pathlib.Path(LOGS_PATH)

procs_folders = []
for procs_folder in logs_path.glob('*'):
    if all([c.isdigit() for c in procs_folder.stem]):
        procs_folders += [procs_folder]

max_proc_num = len(procs_folders)

ops = []

for proc_folder in procs_folders:
    log_file = list(proc_folder.glob("Rank_0_benchmark_*.log"))[0]
    with open(log_file, 'r') as f:
        data_log = f.read().strip()
        pattern = r'procs \d+, rank \d+, elapsed \(sec\) \d+, total \(sec\) (\d+)'
        total_time = re.findall(pattern, data_log)[0]
        total_time = int(total_time)
        ops_per_second = math.floor(TOTAL_OPS / total_time)
        ops += [ops_per_second]


procs = np.arange(1, max_proc_num + 1, 1)
f, ax = plt.subplots(1)
ax.set_xlim(xmin=1,xmax=72)

ax.plot(procs, ops)
ax.grid()
x_ticks = np.arange(2, max_proc_num + 1, 4)
plt.xticks(x_ticks)
plt.xlabel("Количество процессов")
plt.ylabel("Время, мс")
plot_path = pathlib.Path(PLOT_PATH) / 'producer_consumer.png'
plt.savefig(plot_path)