import os

for bench in [
    'bulk_update',
    'json_contains',
    'has_key'
]:
    for db in [
        'psql',
        'sqlite'
    ]:
        cmd = f'pytest --db-backend={db} --benchmark-only --benchmark-json=bench_result/{bench}_{db}.json tests/benchmark/test_{bench}.py'
        print(cmd)
        os.system(cmd)
