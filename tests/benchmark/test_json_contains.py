import pytest

from aiida import orm
from aiida.orm.querybuilder import QueryBuilder
from utils import extract_component, gen_json

GROUP_NAME = 'json-contains'


COMPLEX_JSON_DEPTH_RANGE = [2**i for i in range(4)]
COMPLEX_JSON_BREADTH_RANGE = [2**i for i in range(4)]
LARGE_TABLE_SIZE_RANGE = [2**i for i in range(1, 11)]


@pytest.mark.benchmark(group=GROUP_NAME)
@pytest.mark.parametrize('depth', list(range(1, 16)))
@pytest.mark.parametrize('breadth', [2])
@pytest.mark.usefixtures('aiida_profile_clean')
def test_deep_json(benchmark, depth, breadth):
    lhs = gen_json(depth, breadth)
    rhs = extract_component(lhs, p=1.0 / depth)
    assert 0 == len(QueryBuilder().append(orm.Dict).all())

    orm.Dict(
        {
            'id': f'{depth}-{breadth}',
            'data': lhs,
        }
    ).store()
    qb = QueryBuilder().append(
        orm.Dict,
        filters={
            'attributes.data': {'contains': rhs},
        },
        project=['attributes.id'],
    )
    qb.all()
    result = benchmark(qb.all)
    assert len(result) == 1


@pytest.mark.benchmark(group=GROUP_NAME)
@pytest.mark.parametrize('depth', [2])
@pytest.mark.parametrize('breadth', list(range(1, 101, 5)))
@pytest.mark.usefixtures('aiida_profile_clean')
def test_wide_json(benchmark, depth, breadth):
    lhs = gen_json(depth, breadth)
    rhs = extract_component(lhs, p=1.0 / depth)
    assert 0 == len(QueryBuilder().append(orm.Dict).all())

    orm.Dict(
        {
            'id': f'{depth}-{breadth}',
            'data': lhs,
        }
    ).store()
    qb = QueryBuilder().append(
        orm.Dict,
        filters={
            'attributes.data': {'contains': rhs},
        },
        project=['attributes.id'],
    )
    qb.all()
    result = benchmark(qb.all)
    assert len(result) == 1


@pytest.mark.benchmark(group=GROUP_NAME)
@pytest.mark.parametrize('num_entries', LARGE_TABLE_SIZE_RANGE)
@pytest.mark.usefixtures('aiida_profile_clean')
def test_large_table(benchmark, num_entries):
    data = gen_json(2, 10)
    rhs = extract_component(data)
    assert 0 == len(QueryBuilder().append(orm.Dict).all())

    for i in range(num_entries):
        orm.Dict(
            {
                'id': f'N={num_entries}, i={i}',
                'data': data,
            }
        ).store()
    qb = QueryBuilder().append(
        orm.Dict,
        filters={
            'attributes.data': {'contains': rhs},
        },
        project=['attributes.id'],
    )
    qb.all()
    result = benchmark(qb.all)
    assert len(result) == num_entries
