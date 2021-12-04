from collections import defaultdict
from typing import List
from data.manager import DataManager


class DeadlockDetector:
    def __init__(self):
        self.blocking_graph = defaultdict(set)

    def update_blocking_graph(self, sites: List[DataManager]):
        for site in sites:
            if site.is_up:
                graph = site.generate_blocking_graph()
                for tid, adjacent_tid_set in graph.items():
                    self.blocking_graph[tid].update(adjacent_tid_set)

    # def __get_degree(self):
    #     in_degree = defaultdict(int)
    #     out_degree = defaultdict(int)
    #     for tid, adjacent_tid_set in self.blocking_graph.items():
    #         out_degree[tid] = len(adjacent_tid_set)
    #         for adjacent_tid in adjacent_tid_set:
    #             in_degree[adjacent_tid] += 1
    #     return in_degree, out_degree

    def has_cycle(self, current, parent, visited):
        """ Use DFS to judge if there is a cycle in the blocking graph. """
        visited[current] = True
        for adjacent_tid in self.blocking_graph[current]:
            if adjacent_tid == parent:
                return True
            if not visited[adjacent_tid]:
                if self.has_cycle(adjacent_tid, parent, visited):
                    return True
        return False

    def judge(self):
        visited = defaultdict(bool)
        for tid in self.blocking_graph.keys():
            if not visited[tid]:
                if self.has_cycle(tid, tid, visited):
                    return True
        return False


if __name__ == '__main__':
    """ Test graph with a circle.
    0 → 1 → 3
    ↑ ↙
    2 
     
    """

    def has_cycle(graph):
        visited = defaultdict(bool)

        def dfs(current, parent, graph, visited):
            visited[current] = True
            for neighbor in graph[current]:
                if neighbor == parent:
                    return True
                if not visited[neighbor]:
                    if dfs(neighbor, parent, graph, visited):
                        return True
            return False

        for node in graph.keys():
            if not visited[node]:
                if dfs(node, node, graph, visited):
                    return True
        return False

    blocking_graph = defaultdict(set)
    blocking_graph['t0'] = {'t1'}
    blocking_graph['t1'] = {'t2', 't3'}
    blocking_graph['t2'] = {'t0'}
    blocking_graph['t3'] = set()

    print(has_cycle(blocking_graph))
