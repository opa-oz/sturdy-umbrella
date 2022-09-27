import sys
import argparse
import json

from typing import List, Tuple, Any
from pathlib import Path

import psycopg2

base_path = Path('/opt')


def get(query: str, pg_config: str, payload: Tuple or None, detuple=False) -> List[Any]:
    """
    Executes request to PostgreSQL
    :param query: SQL query
    :param pg_config: PG connection string
    :param payload: Optional arguments for request
    :param detuple: Unwrap tuples in result (very handy when ask for 1 field in request)
    :return: Query's results
    """

    conn = psycopg2.connect(pg_config)

    with conn.cursor() as cursor:
        cursor.execute(query, vars=payload or None)

        result = cursor.fetchall()

    if detuple:
        result = [r[0] for r in result]

    return result


def main(args):
    """
    Parses arguments and performs query
    :param args:
    :return:
    """
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--query", required=True,
                            help="SQL query")

    arg_parser.add_argument("--pg_config", required=True,
                            help="PostgreSQL config")

    arg_parser.add_argument("--output", required=True,
                            help="JSON array of values")

    arg_parser.add_argument("--payload", required=True,
                            help="JSON array of values")

    arg_parser.add_argument("--detuple", type=bool, default=False,
                            help="Does it need to be detupled?")

    args, _ = arg_parser.parse_known_args(args[1:])
    args = vars(args)

    query_path = args['query']
    pg_config_path = args['pg_config']
    output_path = args['output']

    detuple = args['detuple']
    payload = args['payload']

    if payload and len(payload) > 0:
        with open(payload, 'r', encoding='utf8') as payload_file:
            payload = json.load(payload_file)

            if isinstance(payload, list):
                tuple(list(tuple(p) for p in payload))
    else:
        payload = None

    with open(query_path, 'r', encoding='utf8') as query_file:
        query = query_file.read().strip()

    with open(pg_config_path, 'r', encoding='utf8') as pg_config_file:
        pg_config = pg_config_file.read().strip()

    result = get(query, pg_config, payload, detuple)

    with open(output_path, 'w', encoding='utf8') as output_file:
        json.dump(result, output_file, ensure_ascii=False)


if __name__ == '__main__':
    main(sys.argv)
