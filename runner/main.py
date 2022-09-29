import json
import random
import sys
import argparse
from collections import defaultdict
from typing import Dict, List

import yaml
from pathlib import Path
import networkx as nx
from matplotlib import pyplot as plt


class Dependency:
    dep_type: str
    cube_id: str

    def __init__(self, dep_type: str, cube_id: str):
        self.dep_type = dep_type
        self.cube_id = cube_id

    def is_static(self):
        return self.dep_type == 'static'


class BaseCube:
    cube_id: str

    def __init__(self, cube_id: str):
        self.cube_id = cube_id


class StaticCube(BaseCube):
    cube_id: str
    path: Path
    is_leaf: bool

    def __init__(self, cube_id: str, path: Path):
        super().__init__(cube_id)
        self.path = path
        self.is_leaf = True


class Cube(BaseCube):
    cube_id: str
    base: str
    input: Dict

    is_leaf: bool

    def __init__(self, cube_id: str, cube):
        super().__init__(cube_id)
        if cube.get('base') is None:
            raise Exception(f'There is no base for cube "{cube_id}"')

        self.base = cube['base']
        self.input = cube.get('input', {})
        self.is_leaf = len(self.get_blockers()) == 0

    def get_blockers(self):
        """
        Calculate blockers to start cube
        :return:
        """
        blockers: List[Dependency] = []
        for item in self.input.values():
            if not isinstance(item, str):
                continue

            if item.startswith('$S$'):
                blockers.append(Dependency('static', item.replace('$S$', '')))

            if item.startswith('$O$'):
                blockers.append(Dependency('output', item.replace('$O$', '')))

        return blockers


def build_pipeline(config, pipeline_dir: str):
    static = config.get('static')
    cubes: Dict[str, Dict] = config.get('cubes')
    name = config.get('name')
    base_dir = Path.cwd() / pipeline_dir

    if cubes is None:
        raise Exception('There is no cubes in config')

    if name is None:
        raise Exception('There is no name of pipeline')

    static_storage = {}
    cubes_storage: Dict[str, Cube] = {}
    graph = nx.DiGraph()

    if static is not None and isinstance(static, dict) and len(static.keys()) != 0:
        for key, file in static.items():
            filepath = base_dir / 'static_data' / file

            if not filepath.exists():
                raise Exception(f'Missing "{file}" inside "static" folder (full path: {filepath})')

            static_storage[key] = StaticCube(key, filepath)
            graph.add_node(key)

    for cube_id, cube in cubes.items():
        cubes_storage[cube_id] = Cube(cube_id, cube)
        graph.add_node(cube_id)

    for cube_id in cubes_storage:
        cube: Cube = cubes_storage[cube_id]
        deps: List[Dependency] = cube.get_blockers()

        for dep in deps:
            graph.add_edge(dep.cube_id, cube.cube_id)

    if not nx.is_directed(graph):
        raise Exception('Graph is not directed')
    if not nx.is_directed_acyclic_graph(graph):
        raise Exception('Graph is not directed acyclic graph')

    return graph, cubes_storage, static_storage


def run_pipeline(pipeline: nx.DiGraph, cubes_storage: Dict, static_storage: Dict):
    leafs = [cube.cube_id for cube in cubes_storage.values() if cube.is_leaf]
    static_leafs = [cube_id for cube_id in list(static_storage)]

    levels = defaultdict(int)
    max_level = 0

    for leaf in leafs + static_leafs:
        for key, level in nx.single_source_shortest_path_length(pipeline, leaf).items():
            levels[key] = max(levels[key], level)
            max_level = max(level, max_level)

    levels_pipeline = [[] for _ in range(max_level + 1)]

    for key, level in levels.items():
        levels_pipeline[level].append(key)

    print(levels_pipeline)


def print_graph(graph):
    """
    Print graph to a file
    :param graph:
    :return:
    """

    nx.draw_networkx(graph, arrows=True)
    plt.savefig("preview.png", format="PNG")
    # tell matplotlib you're done with the plot:
    # https://stackoverflow.com/questions/741877/how-do-i-tell-matplotlib-that-i-am-done-with-a-plot
    plt.clf()


def main(args):
    """
    Main args processing
    :param args:
    :return:
    """
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--pipeline", required=True,
                            help="Pipeline file")

    args, _ = arg_parser.parse_known_args(args[1:])
    args = vars(args)

    pipeline_path = args['pipeline']

    with open(Path(pipeline_path) / 'pipeline.yaml', 'r', encoding='utf8') as pipeline_file:
        config = yaml.load(pipeline_file, yaml.Loader)

    pipeline, cubes_storage, static_storage = build_pipeline(config, pipeline_path)

    print_graph(pipeline)

    run_pipeline(pipeline, cubes_storage, static_storage)


if __name__ == '__main__':
    main(sys.argv)
