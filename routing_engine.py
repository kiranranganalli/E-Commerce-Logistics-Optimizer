
import networkx as nx
import pandas as pd

def generate_fulfillment_graph():
    G = nx.Graph()
    centers = ['A', 'B', 'C', 'D', 'E']
    edges = [('A','B',5), ('B','C',3), ('C','D',4), ('D','E',2), ('A','E',8), ('B','E',6)]
    for u,v,d in edges:
        G.add_edge(u, v, weight=d)
    return G

def compute_shortest_path(G, origin, destination):
    try:
        return nx.shortest_path(G, origin, destination, weight='weight')
    except nx.NetworkXNoPath:
        return []

def route_packages(df):
    G = generate_fulfillment_graph()
    df['route'] = df.apply(lambda row: compute_shortest_path(G, row['origin'], row['destination']), axis=1)
    df['route_length'] = df['route'].apply(lambda x: len(x))
    return df

if __name__ == "__main__":
    df = pd.read_csv("ecommerce_logistics_data.csv")
    routed_df = route_packages(df)
    routed_df.to_csv("routed_logistics_data.csv", index=False)
