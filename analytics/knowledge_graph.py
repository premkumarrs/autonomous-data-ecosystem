import networkx as nx

def build_knowledge_graph(df):
    G = nx.Graph()

    for _, row in df.iterrows():
        G.add_edge(row["name"], row["department"])
        G.add_edge(row["department"], "Company")

    print(f"Knowledge Graph Nodes: {G.number_of_nodes()}")
    print(f"Knowledge Graph Edges: {G.number_of_edges()}")

    return G
