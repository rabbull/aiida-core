import random
import string


def gen_json(depth: int, breadth: int, force_dict: bool = False):
    def gen_str(n: int, with_digits: bool = True):
        population = string.ascii_letters
        if with_digits:
            population += string.digits
        return ''.join(random.choices(population, k=n))

    if depth == 0:  # random primitive value
        # real numbers are not included as their equivalence is tricky
        return random.choice(
            [
                random.randint(-114, 514),  # integers
                gen_str(6),  # strings
                random.choice([True, False]),  # booleans
                None,  # nulls
            ]
        )

    else:
        gen_dict = random.choice([True, False]) if not force_dict else True
        data = [gen_json(depth - 1, breadth) for _ in range(breadth)]
        if gen_dict:
            keys = set()
            while len(keys) < breadth:
                keys.add(gen_str(6, False))
            data = dict(zip(list(keys), data))
        return data


def extract_component(data, p: float = -1):
    if random.random() < p:
        return data

    if isinstance(data, dict) and data:
        key = random.choice(list(data.keys()))
        return {key: extract_component(data[key])}
    elif isinstance(data, list) and data:
        element = random.choice(data)
        return [extract_component(element)]
    else:
        return data


def extract_key(data):
    return random.choice(list(data.keys()))
