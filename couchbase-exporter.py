#!/usr/bin/env python3
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
        formatted_stat = self._all_cap_re.sub(r'\1_\2', formatted_stat_first).lower().replace("+", "_") 
        
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
                stats_collection[self.format_metric(stat, "cb", "bucket")] = sum(values) / len(values)
            
            stats_collections[bucket] = stats_collection

        for stat, value in stats_collections[buckets[0]].items():
            metric = Metric(stat, "", "gauge")

            for name, stats_collection in stats_collections.items():
                metric.add_sample(
                    stat,
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

        # Derived node metrics
        derived = {}
        derived["cb_node_status_healthy"] = Metric("cb_node_status_healthy", "How many nodes are healthy", "gauge")
        derived["cb_node_status_unhealthy"] = Metric("cb_node_status_unhealthy", "How many nodes are unhealthy", "gauge")
        derived["cb_node_status_warmup"] = Metric("cb_node_status_warmup", "How many nodes are warming up", "gauge")
        derived["cb_node_membership_active"] = Metric("cb_node_membership_active", "How many nodes are active and taking traffic", "gauge")
        derived["cb_node_membership_inactive_added"] = Metric("cb_node_membership_inactive_added", "How many nodes are NOT active and pending rebalance", "gauge")
        derived["cb_node_membership_inactive_failed"] = Metric("cb_node_membership_inactive_failed", "How many nodes are NOT active and failed over", "gauge")
        derived["cb_node_uptime"] = Metric("cb_node_uptime", "Uptime of the node", "counter")
        derived["cb_node_info"] = Metric("cb_node_info", "Information about the node", "gauge")
        derived["cb_service_enabled"] = Metric("cb_service_enabled", "Whether or not a service is enabled", "gauge")
    
        for node in stats["nodes"]:
            derived["cb_node_status_healthy"].add_sample(
                "cb_node_status_healthy",
                value=(node["status"] == "healthy"),
                labels={"node": node["hostname"]}
            )

            derived["cb_node_status_unhealthy"].add_sample(
                "cb_node_status_unhealthy",
                value=(node["status"] == "unhealthy"),
                labels={"node": node["hostname"]}
            )

            derived["cb_node_status_warmup"].add_sample(
                "cb_node_status_warmup",
                value=(node["status"] == "warmup"),
                labels={"node": node["hostname"]}
            )

            derived["cb_node_membership_active"].add_sample(
                "cb_node_membership_active",
                value=(node["clusterMembership"] == "active"),
                labels={"node": node["hostname"]}
            )
            
            derived["cb_node_membership_inactive_added"].add_sample(
                "cb_node_membership_inactive_added",
                value=(node["clusterMembership"] == "inactiveAdded"),
                labels={"node": node["hostname"]}
            )

            derived["cb_node_membership_inactive_failed"].add_sample(
                "cb_node_membership_inactive_failed",
                value=(node["clusterMembership"] == "inactiveFailed"),
                labels={"node": node["hostname"]}
            )

            derived["cb_node_uptime"].add_sample(
                "cb_node_uptime",
                value=float(node["uptime"]),
                labels={"node": node["hostname"]}
            )

            derived["cb_node_info"].add_sample(
                "cb_node_info",
                value=0.0,
                labels={
                    "node": node["hostname"],
                    "cluster_compatability": str(node["clusterCompatibility"]),
                    "version": node["version"],
                    "os": node["os"]
                }
            )

            derived["cb_service_enabled"].add_sample(
                "cb_service_enabled",
                value=("fts" in node["services"]),
                labels={
                    "node": node["hostname"],
                    "service": "fts"
                }
            )

            derived["cb_service_enabled"].add_sample(
                "cb_service_enabled",
                value=("index" in node["services"]),
                labels={
                    "node": node["hostname"],
                    "service": "index"
                }
            )

            derived["cb_service_enabled"].add_sample(
                "cb_service_enabled",
                value=("kv" in node["services"]),
                labels={
                    "node": node["hostname"],
                    "service": "kv"
                }
            )

            derived["cb_service_enabled"].add_sample(
                "cb_service_enabled",
                value=("n1ql" in node["services"]),
                labels={
                    "node": node["hostname"],
                    "service": "n1ql"
                }
            )

        for name, metric in derived.items():
            yield metric
        
        # Derived cluster metrics
        metric = Metric("cb_rebalance_running", "Whether or not a cluster rebalance is happening", "gauge")
        metric.add_sample(
            "cb_rebalance_running",
            value=(0.0 if stats["rebalanceStatus"] == "none" else 1.0),
            labels={}
        )
        yield metric

        if "rebalance_start" in stats["counters"]:
            metric = Metric("cb_rebalance_start_total", "How many rebalances have started", "counter")
            metric.add_sample(
                "cb_rebalance_start_total",
                value=stats["counters"]["rebalance_start"],
                labels={}
            )
            yield metric
        
        if "rebalance_success" in stats["counters"]:
            metric = Metric("cb_rebalance_success_total", "How many rebalances have succeeded", "counter")
            metric.add_sample(
                "cb_rebalance_success_total",
                value=stats["counters"]["rebalance_success"],
                labels={}
            )
            yield metric

        if "failover_node" in stats["counters"]:
            metric = Metric("cb_failover_node_total", "How many node failovers have occurred", "counter")
            metric.add_sample(
                "cb_failover_node_total",
                value=stats["counters"]["failover_node"],
                labels={}
            )
            yield metric

        metric = Metric("cb_balanced", "Whether or not the cluster is balanced", "gauge")
        metric.add_sample(
            "cb_balanced",
            value=float(stats["balanced"]),
            labels={}
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