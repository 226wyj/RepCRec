from collections import defaultdict
from typing import List

from data.manager import DataManager


def generate_blocking_graph(sites: List[DataManager]):
    blocking_graph = defaultdict(set)
    for site in [x for x in sites if x.is_up]:
        graph = site.generate_blocking_graph()
        print('\nBlocking graph of site {}'.format(site.sid))
        print(graph)
        print()
        for tid, adjacent_tid_set in graph.items():
            blocking_graph[tid].update(adjacent_tid_set)
    return blocking_graph


def has_cycle(start, end, visited, blocking_graph):
    """ Use DFS to judge if there is a cycle in the blocking graph. """
    visited[start] = True
    for adjacent_tid in blocking_graph[start]:
        if adjacent_tid == end:
            return True
        if not visited[adjacent_tid]:
            if has_cycle(adjacent_tid, end, visited, blocking_graph):
                return True
    return False


def detect(transactions, blocking_graph):
    victim_timestamp = float('-inf')
    victim_tid = None
    # To avoid `RuntimeError: dictionary changed size during iteration`,
    # we should use list() function or .copy() method.
    for tid in list(blocking_graph.keys()):
        visited = defaultdict(bool)
        if has_cycle(tid, tid, visited, blocking_graph):
            if transactions[tid].timestamp > victim_timestamp:
                victim_timestamp = transactions[tid].timestamp
                victim_tid = tid
    return victim_tid


# if __name__ == '__main__':
#     """ Test graph with a circle.
#     0 → 1 → 3
#     ↑ ↙
#     2
#
#     """
#
#     def has_cycle(graph, cycle):
#         visited = defaultdict(bool)
#         def dfs(start, end, graph, visited):
#             if start not in graph or end not in graph:
#                 return False
#             if start == end:
#                 return True
#             visited[start] = True
#             cycle.append(start)
#             for neighbor in graph[start]:
#                 if not visited[neighbor]:
#                     cycle.append(neighbor)
#                     if dfs(neighbor, end, graph, visited):
#                         return True
#                 cycle.pop(-1)
#             return False
#
#         for node in graph.keys():
#             if not visited[node]:
#                 if dfs(node, node, graph, visited):
#                     return True
#         return False
#
#     blocking_graph = defaultdict(set)
#     blocking_graph['t0'] = {'t1'}
#     blocking_graph['t1'] = {'t2', 't3'}
#     blocking_graph['t2'] = {'t0'}
#     blocking_graph['t3'] = set()
#     cycle = []
#     print(has_cycle(blocking_graph, cycle))
#     print(cycle)
