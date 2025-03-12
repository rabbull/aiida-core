import pytest

from aiida import orm
from aiida.manage import get_manager
from aiida.orm import EntityTypes
from utils import gen_json

GROUP_NAME = 'bulk-update'


@pytest.mark.benchmark(group=GROUP_NAME)
@pytest.mark.parametrize('depth', list(range(1, 16)))
@pytest.mark.parametrize('breadth', [2])
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
@pytest.mark.parametrize('breadth', list(range(1, 101, 5)))
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
@pytest.mark.parametrize('num_entries', [2**i for i in range(1, 11)])
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


@pytest.mark.usefixtures('aiida_profile_clean')
def test_parallel():
    node = orm.Dict({
        'mylist': []
    }).store()

    update1 = [{
        'id': node.pk,
        'attributes': {
            'mylist': [i*2 for i in range(1000)]
        }
    }]
    update2 = [{
        'id': node.pk,
        'attributes': {
            'mylist': [i*2+1 for i in range(1000)]
        }
    }]

    def foo1():
        get_manager().get_backend().bulk_update(EntityTypes.NODE, update1, extend_json=True)
    
    def foo2():
        get_manager().get_backend().bulk_update(EntityTypes.NODE, update2, extend_json=True)
    
    from threading import Thread

    t1 = Thread(target=foo1)
    t2 = Thread(target=foo2)

    t1.start()
    t2.start()
    t1.run()
    t2.run()
    t1.join()
    t2.join()

    
    print(sorted(orm.load_node(node.pk).attributes['mylist']))