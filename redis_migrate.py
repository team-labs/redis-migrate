"""
redis_migrate.py
===
Migrates all of the key/values for a given Redis server to
another. Currently only supports:
- String
- List
- Set
- Hash
- SortedSet
"""
import redis
import argparse
import formatters

TYPE_TO_GET_COMMAND = {
    b"string": ["get"],
    b"list": ["lrange", 0, 99999],
    b"bitmap": ["getbit"],
    b"set": ["smembers"],
    b"hash": ["hgetall"],
    b"zset": ["zrange", 0, 99999, False, True]
}


TYPE_TO_PUT_COMMAND = {
    b"string": ["set", formatters.redis_string],
    b"list": ["lpush", formatters.redis_list],
    b"bitmap": ["setbit", formatters.redis_bitmap],
    b"set": ["sadd", formatters.redis_set],
    b"hash": ["hmset", formatters.redis_hash],
    b"zset": ["zadd", formatters.redis_sortedset]
}


def get_keys(conn: redis.StrictRedis) -> list:
    """
    Returns the keys for a given redis connection
    """
    return conn.keys()


def get_key_types(conn: redis.StrictRedis, keys: list) -> list:
    """
    Returns a list of types for a list of keys in redis
    """
    pipe = conn.pipeline()
    for key in keys:
        pipe.type(key)
    return pipe.execute()


def get_values(conn: redis.StrictRedis, keys: list, key_types: list) -> list:
    """
    Gets the values for a set of keys and their types
    """
    pipe = conn.pipeline()

    for i, key_type in enumerate(key_types):
        key = keys[i]
        command_with_values = TYPE_TO_GET_COMMAND[key_type]
        command = command_with_values[0]
        redis_method = getattr(pipe, command)
        arguments = [key] + [arg for arg in command_with_values[1:]]
        redis_method(*arguments)

    return pipe.execute()


def migrate_data(conn: redis.StrictRedis, data: dict) -> list:
    """
    Uploads the given data to the given redis database connection
    """
    pipe = conn.pipeline()

    for key, value in data.items():
        command_and_formatter = TYPE_TO_PUT_COMMAND[value["type"]]
        command = command_and_formatter[0]
        formatter = command_and_formatter[1]
        redis_method = getattr(pipe, command)
        formatted_values = formatter(value["value"])
        arguments = [key] + formatted_values
        redis_method(*arguments)

    return pipe.execute()


def map_data(keys: list, values: list, types: list) -> dict:
    """
    Returns a dict that maps keys to their values and types
    """
    data: dict = {}

    for i, value in enumerate(values):
        key = keys[i]
        key_type = types[i]

        data[key] = {
            "value": value,
            "type": key_type
        }

    return data


def check_success(responses: list) -> bool:
    """
    Iterates through the list of responses and returns True if all
    were successful
    """
    success = True

    for response in responses:
        success = success and response

    return success


def main():
    """
    Parses any arguments and kicks off functionality
    """
    parser = argparse.ArgumentParser(description='Migrate data from one Redis server to another')
    parser.add_argument('from-hostname',
                        help='the hostname of the redis instance to transfer data from')
    parser.add_argument('to-hostname',
                        help='the hostname of the redis instance to transfer data from')
    parser.add_argument('--from-port',
                        help='the port of the redis instance to transfer data from',
                        type=int, default=6379)
    parser.add_argument('--to-port',
                        help='the port of the redis instance to transfer data from',
                        type=int, default=6379)
    parser.add_argument('--from-password',
                        help='the password of the redis instance to transfer data from')
    parser.add_argument('--to-password',
                        help='the password of the redis instance to transfer data from')
    parser.add_argument('--from-database',
                        help='the database of the redis instance to transfer data from',
                        type=int, default=0)
    parser.add_argument('--to-database',
                        help='the database of the redis instance to transfer data from',
                        type=int, default=0)

    args = parser.parse_args()

    from_host = getattr(args, 'from-hostname')
    to_host = getattr(args, 'to-hostname')
    from_port = args.from_port
    to_port = args.to_port
    from_db = args.from_database
    to_db = args.to_database
    from_redis = redis.StrictRedis(host=from_host, port=from_port, db=from_db)
    keys = get_keys(from_redis)
    key_types = get_key_types(from_redis, keys)
    values = get_values(from_redis, keys, key_types) 

    data = map_data(keys, values, key_types)
    to_redis = redis.StrictRedis(host=to_host, port=to_port, db=to_db)
    success_values = migrate_data(to_redis, data)
    success = check_success(success_values)

    if success:
        print('Successfully copied data!')
    else:
        print('One or more keys could not be copied. Please check your database to troubleshoot')

if __name__ == "__main__":
    main()
