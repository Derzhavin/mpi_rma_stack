import pathlib
import re
import math
import numpy as np
from matplotlib import pyplot as plt

CENTRALIZED_LOGS_PATH = "../data/centralized/random_operation_0/"
DECENTRALIZED_LOGS_PATH = "../data/decentralized/random_operation_0/"
PLOT_PATH = "../data"
TOTAL_OPS = 15000


linestyle_tuple = [
    ('densely dotted', (0, (1, 1))),

    ('dashed', (0, (5, 5))),
    ('densely dashed', (0, (5, 1))),

    ('dashdotted', (0, (3, 5, 1, 5))),
    ('densely dashdotted', (0, (3, 1, 1, 1)))
]

logs_paths = [pathlib.Path(CENTRALIZED_LOGS_PATH),
             pathlib.Path(DECENTRALIZED_LOGS_PATH)]

ops = [[],[]]

for i in range(len(logs_paths)):
    procs_folders = []
    for procs_folder in logs_paths[i].glob('*'):
        if all([c.isdigit() for c in procs_folder.stem]):
            procs_folders += [procs_folder]
    procs_folders.sort()
    max_proc_num = len(procs_folders)

    for proc_folder in procs_folders:
        log_file = list(proc_folder.glob("Rank_0_benchmark_*.log"))[0]
        with open(log_file, 'r') as f:
            data_log = f.read().strip()
            pattern = r'procs \d+, rank \d+, elapsed \(sec\) \d+\.\d+, total \(sec\) (\d+\.\d+)'
            total_time = re.findall(pattern, data_log)[0]
            total_time = float(total_time)
            ops_per_second = math.floor(TOTAL_OPS / total_time)
            ops[i] += [ops_per_second]


procs = np.arange(1, max_proc_num + 1, 1)
f, ax = plt.subplots(1)
ax.set_xlim(xmin=1,xmax=6.1)
ax.plot(procs, ops[0], marker='o', linestyle=linestyle_tuple[2][1], color='red', label='централизованный стек')
ax.plot(procs, ops[1], marker='s', linestyle=linestyle_tuple[2][1], color='blue', label='децентрализованный стек')
ax.grid()
x_ticks = np.arange(1, max_proc_num + 1, 1)
plt.xticks(x_ticks)
plt.xlabel("Количество процессов")
plt.ylabel("Количество операций в секунду")
plt.legend(loc='upper left')
plot_path = pathlib.Path(PLOT_PATH) / 'producer_consumer_centralized_vs_decentralized.png'
plt.savefig(plot_path)