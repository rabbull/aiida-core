import pytest

from aiida import orm
from aiida.manage import get_manager
from aiida.orm import EntityTypes
from utils import gen_json

GROUP_NAME = 'bulk-update'


@pytest.mark.benchmark(group=GROUP_NAME)
@pytest.mark.parametrize('depth', [1, 2, 4, 8])
@pytest.mark.parametrize('breadth', [1, 2, 4])
@pytest.mark.usefixtures('aiida_profile_clean')
def test_deep_json(benchmark, depth, breadth):
    nodes = []
    for _ in range(10):
        nodes.append(orm.Dict(gen_json(depth, breadth, force_dict=True)).store())

    updates = []
    for node in nodes:
        update = {'id': node.id, 'attributes': gen_json(depth, breadth, force_dict=True)}
        updates.append(update)

    backend = get_manager().get_backend()
    benchmark(lambda: backend.bulk_update(entity_type=EntityTypes.NODE, rows=updates, extend_json=True))


@pytest.mark.benchmark(group=GROUP_NAME)
@pytest.mark.parametrize('depth', [2])
@pytest.mark.parametrize('breadth', [1, 10, 100])
@pytest.mark.usefixtures('aiida_profile_clean')
def test_wide_json(benchmark, depth, breadth):
    nodes = []
    for _ in range(10):
        nodes.append(orm.Dict(gen_json(depth, breadth, force_dict=True)).store())

    updates = []
    for node in nodes:
        update = {'id': node.id, 'attributes': gen_json(depth, breadth, force_dict=True)}
        updates.append(update)

    backend = get_manager().get_backend()
    benchmark(lambda: backend.bulk_update(entity_type=EntityTypes.NODE, rows=updates, extend_json=True))


@pytest.mark.benchmark(group=GROUP_NAME)
@pytest.mark.parametrize('num_entries', [2**i for i in range(1, 10)])
@pytest.mark.usefixtures('aiida_profile_clean')
def test_large_table(benchmark, num_entries):
    nodes = []
    for _ in range(num_entries):
        nodes.append(orm.Dict(gen_json(2, 10, force_dict=True)).store())

    updates = []
    for node in nodes:
        update = {'id': node.id, 'attributes': gen_json(2, 10, force_dict=True)}
        updates.append(update)

    backend = get_manager().get_backend()
    benchmark(lambda: backend.bulk_update(entity_type=EntityTypes.NODE, rows=updates, extend_json=True))
