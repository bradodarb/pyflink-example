from abc import ABCMeta, abstractmethod
from typing import List, Dict, Any, Set

import framework.dag.loader as loader


class FlowStep(metaclass=ABCMeta):
    match_key = 'no match'

    @property
    def kind(self):
        return self._kind

    @property
    def name(self):
        return self._name

    @property
    def config(self):
        return self._config

    @property
    def description(self):
        return self._description

    def __init__(self, kind: str, name: str, config: Dict, description: str = None):
        self._kind = kind
        self._name = name
        self._config = config
        self._description = description
        self._init()

    @classmethod
    def match(cls, source: Dict) -> bool:
        return cls.match_key in source

    @abstractmethod
    def _init(self):
        pass


class Dag:

    @staticmethod
    def get_flow_step_types() -> Set[Any]:
        subclasses = set()
        work = [FlowStep]
        while work:
            parent = work.pop()
            for child in parent.__subclasses__():
                if child not in subclasses:
                    subclasses.add(child)
                    work.append(child)
        return subclasses

    @staticmethod
    def get_flow_steps(source: List[Dict]) -> List[FlowStep]:
        result = []
        allowed_step_types = Dag.get_flow_step_types()
        for item in source:
            for step_type in allowed_step_types:
                if step_type.match_key and step_type.match(item):
                    item['kind'] = step_type.match_key
                    item['name'] = item.pop(step_type.match_key)
                    result.append(step_type(**item))
        return result

    @staticmethod
    def load(path: str, context: Dict = None) -> 'Dag':
        dag_config = loader.resolve(".", path)
        dag = Dag(**dag_config)
        return dag

    def __init__(self, version: str, kind: str, name: str, description: str, flow: List[Dict] = None):
        self._version = version
        self._kind = kind
        self._name = name
        self._description = description
        self._flow = Dag.get_flow_steps(flow)

    def get_flow_step_sequence(self, *kinds, comparator=None) -> List[FlowStep]:

        source_sequence = [item for item in self._flow if item.kind in kinds]
        if not comparator:
            return source_sequence

        return list(sorted(source_sequence, key=comparator))
