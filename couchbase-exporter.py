from prometheus_client import start_http_server, Metric, REGISTRY
from argparse import ArgumentParser
from os import environ
from json import loads
from requests import get
from time import sleep
from re import compile

class CouchbaseCollector(object):
    def __init__(self, cluster, username, password):
        self._cluster = cluster
        self._username = username
        self._password = password
        self._first_cap_re = compile("(.)([A-Z][a-z]+)")
        self._all_cap_re = all_cap_re = compile("([a-z0-9])([A-Z])")
    
    def format_metric(self, stat, prefix=None, component=None):
        formatted_stat_first = self._first_cap_re.sub(r'\1_\2', stat)
        formatted_stat = self._all_cap_re.sub(r'\1_\2', formatted_stat_first).lower() 

        prefix = "%s_" % prefix if prefix is not None else ""
        component = "%s_" % component if component is not None else ""

        return "%s%s%s" % (prefix, component, formatted_stat)
    
    def get_request(self, url):
        return loads(get(
            url,
            auth=(self._username, self._password),
            verify=False
        ).content.decode("UTF-8"))

class BucketCollector(CouchbaseCollector):
    def get_buckets(self):
        return [bucket["name"] for bucket in self.get_request("%s/pools/default/buckets" % self._cluster)]
    
    def collect(self):
        buckets = self.get_buckets()
        stats_collections = {}

        for bucket in buckets:
            response = self.get_request("%s/pools/default/buckets/%s/stats?zoom=minute" % (self._cluster, bucket))["op"]["samples"]

            stats_collection = {}

            for stat, values in response.items():
                stats_collection[stat] = sum(values) / len(values)
            
            stats_collections[bucket] = stats_collection

        for stat, value in stats_collections[buckets[0]].items():
            metric_name = self.format_metric(stat, "cb", "bucket")

            metric = Metric(metric_name, "", "gauge")

            for name, stats_collection in stats_collections.items():
                metric.add_sample(
                    metric_name,
                    value=stats_collection[stat],
                    labels={"bucket": name}
                )
            
            yield metric

class NodeCollector(CouchbaseCollector):
    def collect(self):
        stats = self.get_request("%s/pools/default" % self._cluster)

        data_nodes = []
        other_nodes = []

        for node in stats["nodes"]:
            if "kv" in node["services"]:
                data_nodes.append(node)
            else:
                other_nodes.append(node)

        for storage_type, totals in stats["storageTotals"].items():
            for stat, value in totals.items():
                metric_name = self.format_metric(stat, "cb", storage_type)

                metric = Metric(metric_name, "", "gauge")
                metric.add_sample(metric_name, value=value, labels={})
                yield metric

        for stat, value in data_nodes[0]["systemStats"].items():
            metric_name = self.format_metric(stat, "cb", "node")

            metric = Metric(metric_name, "", "gauge")

            for node in data_nodes:
                metric.add_sample(
                    metric_name,
                    value=node["systemStats"][stat],
                    labels={"node": node["hostname"]}
                )
            
            for node in other_nodes:
                metric.add_sample(
                    metric_name,
                    value=node["systemStats"][stat],
                    labels={"node": node["hostname"]}
                )
            
            yield metric
    
        for stat, value in data_nodes[0]["interestingStats"].items():
            metric_name = self.format_metric(stat, "cb", "node")

            metric = Metric(metric_name, "", "gauge")

            for node in data_nodes:
                metric.add_sample(
                    metric_name,
                    value=node["interestingStats"][stat],
                    labels={"node": node["hostname"]}
                )
            
            yield metric

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("--cluster", default="http://localhost:8091", help="Full URL of the Couchbase cluster Web UI")
    parser.add_argument("--username", help="Username to access the Couchbase cluster")
    parser.add_argument("--password", help="Password to access the Couchbase cluster")
    parser.add_argument("--port", default=8080, help="Port to run the metric server on")

    args = parser.parse_args()

    try:
        cluster = environ["CB_CLUSTER"]
    except KeyError:
        cluster = args.cluster
    
    try:
        username = environ["CB_USERNAME"]
    except KeyError:
        username = args.username

    try:
        password = environ["CB_PASSWORD"]
    except KeyError:
        password = args.password

    start_http_server(args.port)

    REGISTRY.register(BucketCollector(cluster, username, password))
    REGISTRY.register(NodeCollector(cluster, username, password))

    while True: sleep(1)