
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import collections

from ansible.plugins.strategy.linear import StrategyModule as LinearStrategyModule

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()


class StrategyModule(LinearStrategyModule):
    def __init__(self, tqm):
        super(StrategyModule, self).__init__(tqm)

    def _default_cluster_id(self, host):
        return ','.join(str(g) for g in sorted(host.groups))

    def _get_serialized_batches(self, play, cluster_id_extract=None):
        """
        Returns a list of hosts, subdivided into batches to ensure.
        """

        if not cluster_id_extract:
            cluster_id_extract = self._default_cluster_id

        # make sure we have a unique list of hosts
        all_hosts = list(self._inventory.get_hosts(play.hosts))
        all_hosts.sort(key=lambda host: host.name)
        display.debug(all_hosts)

        cluster_dict = collections.defaultdict(list)
        for host in all_hosts:
            cluster_dict[cluster_id_extract(host)].append(host)
        print(cluster_dict)

        # sort the clusters to ensure consistent and predictable ordering
        cluster_ids = list(cluster_dict.keys())
        cluster_ids.sort()
        cluster_list = [cluster_dict[gr] for gr in cluster_ids]

        if len(cluster_list) == 0:
            # shortcut: no hosts at all, no reason to continue
            return []

        serialized_batches = []

        while True:
            batch = []
            for cluster_hosts in cluster_list:
                if len(cluster_hosts) > 0:
                    batch.append(cluster_hosts.pop(0))
            if len(batch) == 0:
                break
            serialized_batches.append(batch)

        return serialized_batches

    def run(self, iterator, play_context):
        play = iterator._play
        batches = self._get_serialized_batches(play)
        print(batches)
        return super(StrategyModule, self).run(iterator, play_context)


