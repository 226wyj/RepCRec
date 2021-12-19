from collections import defaultdict
from typing import List

from data.manager import DataManager


def generate_blocking_graph(sites: List[DataManager]) -> defaultdict:
    """
    Collect blocking information from all up sites, and generate
    a complete blocking graph for all the existing transactions.
    """
    blocking_graph = defaultdict(set)
    for site in [x for x in sites if x.is_up]:
        graph = site.generate_blocking_graph()
        for tid, adjacent_tid_set in graph.items():
            blocking_graph[tid].update(adjacent_tid_set)
    return blocking_graph


def has_cycle(start, end, visited, blocking_graph) -> bool:
    """Use DFS to judge if there is a cycle in the blocking graph.

    Principle:
        For all the arcs that starts from a node, if this node's parent
        existed as the end of an arc, then there is a cycle in the graph.
    """
    visited[start] = True
    for adjacent_tid in blocking_graph[start]:
        if adjacent_tid == end:
            return True
        if not visited[adjacent_tid]:
            if has_cycle(adjacent_tid, end, visited, blocking_graph):
                return True
    return False


def detect(transactions, blocking_graph):
    """
    Find out if there is a cycle in the blocking graph. If so, then there exists
    a deadlock, and this function will return the youngest transaction id. Otherwise,
    return nothing.
    """
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
