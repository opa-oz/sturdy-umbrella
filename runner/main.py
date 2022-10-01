import shutil
import subprocess
import sys
import argparse
import uuid
from collections import defaultdict
from typing import Dict, List
import multiprocessing as mp

import yaml
from pathlib import Path
import networkx as nx
from matplotlib import pyplot as plt
from networkx.drawing.nx_pydot import graphviz_layout

OPS_PREFIX = 'cr.yandex/crpnpsi88vqr2fvp2ngb/operations'


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
    label: str

    def __init__(self, cube_id: str, path: Path):
        super().__init__(cube_id)
        self.path = path
        self.is_leaf = True
        self.label = path.suffix if path.suffix else cube_id


class Cube(BaseCube):
    cube_id: str
    base: str
    input: Dict
    label: str

    is_leaf: bool

    def __init__(self, cube_id: str, cube):
        super().__init__(cube_id)
        if cube.get('base') is None:
            raise Exception(f'There is no base for cube "{cube_id}"')

        self.label = cube.get('label', cube_id)
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


def build_pipeline(config, pipeline_dir: Path):
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
    graph = nx.DiGraph(directed=True)

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


def run_container(command: str):
    # print('Run:', command)
    subprocess.run(
        command,
        shell=True,
        check=True,
    )


def run_pipeline(pipeline: nx.DiGraph, cubes_storage: Dict, static_storage: Dict, pipeline_dir: Path):
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

    # print('Levels: ', levels_pipeline)

    runs_dir = pipeline_dir / 'runs'
    runs_dir.mkdir(exist_ok=True)

    run_id = str(uuid.uuid4())

    run_dir = runs_dir / str(run_id)
    run_dir.mkdir(exist_ok=True)

    level_dirs: List[Path] = []

    for level in range(len(levels_pipeline)):
        level_dir = run_dir / f'level_{level}'
        level_dir.mkdir(exist_ok=True)

        level_dirs.append(level_dir)

    outputs: Dict[str, Path] = {}

    with mp.Pool(mp.cpu_count()) as pool:

        for level in range(len(levels_pipeline)):
            steps: List[BaseCube] = []
            level_dir = level_dirs[level]

            for step in levels_pipeline[level]:
                if static_storage.get(step) is not None:
                    steps.append(static_storage.get(step))
                elif cubes_storage.get(step) is not None:
                    steps.append(cubes_storage.get(step))

            commands = []

            for step in steps:
                if isinstance(step, StaticCube):
                    static_filename = f'static_{uuid.uuid4()}'
                    static_filepath = level_dir / static_filename

                    shutil.copy(step.path, static_filepath)

                    outputs[f'$S${step.cube_id}'] = static_filepath

                if isinstance(step, Cube):
                    inputs = step.input
                    deps = []

                    for _, val in inputs.items():
                        if isinstance(val, str) and ('$S$' in val or '$O$' in val):
                            input_filename = f'input_{uuid.uuid4()}'
                            file_dep = level_dir / input_filename
                            shutil.copy(outputs[val], file_dep)

                            deps.append(f'/opt/{input_filename}')
                        elif isinstance(val, str) and len(val) == 0:
                            deps.append('""')
                        else:
                            deps.append(val)

                    output_filename = f'output_{step.cube_id}_{uuid.uuid4()}'
                    output_path = level_dir / output_filename
                    command = ' '.join(
                        [
                            'docker', 'run',
                            '--mount', f'type=bind,source="{level_dir.absolute()}",target=/opt',
                            '-i', '--rm',
                            '-t', f'{OPS_PREFIX}/{step.base}:macos-m1'
                        ] + [str(d) for d in deps] + [f'/opt/{output_filename}']
                    )

                    commands.append(command)

                    # subprocess.run(
                    #     command,
                    #     shell=True,
                    #     check=True,
                    # )

                    outputs[f'$O${step.cube_id}'] = output_path

                    print(level, step.cube_id, deps)

            pool.map(run_container, commands)
            commands = []

    print(f'\nRun #{run_id} is done!')


def print_graph(graph, cubes_storage, static_storage, base_path: Path):
    """
    Print graph to a file
    :param static_storage:
    :param cubes_storage:
    :param graph:
    :return:
    """
    labels = {}

    for node in graph.nodes:
        if cubes_storage.get(node) is not None:
            labels[node] = cubes_storage.get(node).label
        elif static_storage.get(node) is not None:
            labels[node] = static_storage.get(node).label
        else:
            labels[node] = '???'

    pos = graphviz_layout(graph, prog="dot")
    nx.draw(graph, pos, with_labels=True, node_size=2000, labels=labels, font_size=10)

    plt.savefig(base_path / "preview.png", format="PNG")
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

    pipeline_path = Path(args['pipeline'])

    with open(pipeline_path / 'pipeline.yaml', 'r', encoding='utf8') as pipeline_file:
        config = yaml.load(pipeline_file, yaml.Loader)

    pipeline, cubes_storage, static_storage = build_pipeline(config, pipeline_path)

    print_graph(pipeline, cubes_storage, static_storage, pipeline_path)

    run_pipeline(pipeline, cubes_storage, static_storage, pipeline_path)


if __name__ == '__main__':
    main(sys.argv)
