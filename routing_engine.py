

"""
Routing Engine Script for E-Commerce Logistics Optimizer
--------------------------------------------------------
This module computes the shortest delivery path between logistics nodes (warehouses, hubs, and customer regions) 
using Dijkstra's algorithm. It ingests node and edge data from S3, constructs a weighted graph, computes optimal
routes, and stores the routing table back to S3.

Technologies Used:
- Python (graph algorithms)
- Boto3 (S3 access)
- NetworkX (graph construction and pathfinding)
- Pandas (intermediate processing)

Author: Kiran Ranganalli
Last Updated: July 2025
"""

import boto3
import pandas as pd
import networkx as nx
import io
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("routing_engine")

# Constants
S3_BUCKET = "ecom-logistics-data"
NODE_FILE = "graph/nodes.csv"
EDGE_FILE = "graph/edges.csv"
OUTPUT_FILE = "graph/routing_table.json"

# Initialize S3
s3_client = boto3.client("s3")

# Load CSV from S3
def load_csv_from_s3(file_key):
    logger.info(f"Loading file from S3: {file_key}")
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
    return pd.read_csv(io.BytesIO(response['Body'].read()))

# Save JSON to S3
def save_json_to_s3(data, file_key):
    logger.info(f"Saving JSON to S3: {file_key}")
    s3_client.put_object(Body=json.dumps(data), Bucket=S3_BUCKET, Key=file_key)

# Build Graph
def build_graph(nodes_df, edges_df):
    logger.info("Building logistics network graph")
    G = nx.DiGraph()

    for _, row in nodes_df.iterrows():
        G.add_node(row["node_id"], location=row["location"], type=row["type"])

    for _, row in edges_df.iterrows():
        G.add_edge(row["source"], row["target"], weight=row["distance_km"])

    return G

# Compute Shortest Paths
def compute_shortest_paths(G):
    logger.info("Computing shortest paths using Dijkstra's algorithm")
    routing_table = {}

    for source in G.nodes:
        lengths, paths = nx.single_source_dijkstra(G, source)
        routing_table[source] = {
            target: {"path": paths[target], "distance_km": lengths[target]}
            for target in paths
        }

    return routing_table

# Orchestrate Routing Engine
def run_routing_engine():
    logger.info("Routing Engine started")

    nodes_df = load_csv_from_s3(NODE_FILE)
    edges_df = load_csv_from_s3(EDGE_FILE)

    G = build_graph(nodes_df, edges_df)
    routing_table = compute_shortest_paths(G)

    save_json_to_s3(routing_table, OUTPUT_FILE)

    logger.info("Routing Engine completed successfully")

if __name__ == "__main__":
    run_routing_engine()
