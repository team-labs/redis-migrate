"""
Methods to format pulled Redis data into put-able Redis data
"""

def redis_string(value: bytes) -> list:
    """
    Formats a pulled string value for put to redis
    """
    return [value]


def redis_list(value: list) -> list:
    """
    Formats a pulled list value for put to redis
    """
    return value


def redis_bitmap(value: bytes) -> list:
    """
    Formats a pulled bitmap value for put to redis
    """
    return value


def redis_set(value: set) -> list:
    """
    Formats a pulled set value for put to redis
    """
    set_values = []

    for item in value:
        set_values.append(item)

    return set_values


def redis_hash(value: dict) -> list:
    """
    Formats a pulled list value for put to redis
    """
    return [value]


def redis_sortedset(value: list) -> list:
    """
    Formats a pulled sortedset value for put to redis
    """
    sortedset_values = []

    for tup in value:
        sortedset_values.append(tup[1])
        sortedset_values.append(tup[0])

    return sortedset_values
