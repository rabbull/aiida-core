import json
from enum import Enum

from matplotlib import pyplot as plt
import numpy as np


class DatabaseBackend(Enum):
    SQLite = 'sqlite'
    PostgreSQL = 'psql'


class Benchmark(Enum):
    BulkUpdate = 'bulk_update'
    JsonContains = 'json_contains'
    JsonHasKey = 'has_key'


class Experiment(Enum):
    DeepJson = ('deep_json', 'depth')
    WideJson = ('wide_json', 'breadth')
    LargeTable = ('large_table', 'num_entries')


class Result:
    def __init__(self, name: str, x: list[int], avg: list[float], std: list[float]):
        self.name = name
        self.x = x
        self.avg = avg
        self.std = std


def plot(title: str, results: list[Result]):
    plt.figure(figsize=(10, 6))
    for result in results:
        x = np.array(result.x)
        avg = np.array(result.avg)
        std = np.array(result.std)

        line, = plt.plot(x, avg, marker='o', linestyle='-', label=result.name)
        plt.fill_between(x, avg - std, avg + std,
                         color=line.get_color(), alpha=0.2)

    plt.xlabel("X")
    plt.ylabel("Average")
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{title}.jpg')


def main():
    results = {}
    for benchmark in Benchmark:
        for db in DatabaseBackend:
            filename = f'{benchmark.value}_{db.value}.json'
            with open(filename, 'r') as f:
                res = json.load(f)
            benchmarks = res['benchmarks']
            for run in benchmarks:
                for e in Experiment:
                    if e.value[0] in run['name']:
                        name = e.value[0]
                        x = run['params'][e.value[1]]
                        break
                avg = run['stats']['mean'] * 1000
                std = run['stats']['stddev'] * 1000

                k = (benchmark.value, name)
                v = (x, avg, std)
                if not k in results:
                    results[k] = {
                        'sqlite': [],
                        'psql': []
                    }
                results[k][db.value].append(v)
    for key in results:
        name = '+'.join(key)
        result = results[key]
        r = []
        for db in result:
            result[db].sort(key=lambda e: e[0])
            x, avg, std = zip(*result[db])
            r.append(Result(db, list(x), list(avg), list(std)))
        plot(name, r)
        
        


if __name__ == '__main__':
    main()
