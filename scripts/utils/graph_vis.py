import networkx as nx
from nxpd import draw, nxpdParams
import matplotlib.pyplot as plt
from fa2 import ForceAtlas2
import random
import community
from community import community_louvain
import matplotlib.pyplot as plt
    
class NxpdGraph(object):
    def __init__(self):
        self.G = nx.Graph()
        self.coocurence = {}
        self.ocurence = {}
        
    def add_node(self, n):
        self.G.add_node(n)
        self.occurance(n)
        return n

    def add_edge(self, u, v):
        uv = sorted([u,v])
        u = uv[0]
        v = uv[1]
        self.G.add_edge(u, v)
        if((u,v) in self.coocurence):
            self.coocurence[(u,v)] = self.coocurence[(u,v)] + 1
        else:
            self.coocurence[(u,v)] = 1

    def occurance(self, n):
        if(n in self.ocurence):
            self.ocurence[n] = self.ocurence[n] + 1
        else:
            self.ocurence[n] = 1
    
    def print_top_occurance(self, k=20):
        print(sorted(self.ocurence.items(), key=lambda x: x[1], reverse=True)[:k])

    def community_layout(self, g, partition):
        """
        Compute the layout for a modular graph.


        Arguments:
        ----------
        g -- networkx.Graph or networkx.DiGraph instance
            graph to plot

        partition -- dict mapping int node -> int community
            graph partitions


        Returns:
        --------
        pos -- dict mapping int node -> (float x, float y)
            node positions

        """

        pos_communities = self._position_communities(g, partition, scale=400.)

        pos_nodes = self._position_nodes(g, partition, k=100, scale=60.)

        # combine positions
        pos = dict()
        for node in g.nodes():
            pos[node] = pos_communities[node] + pos_nodes[node]

        return pos

    def _position_communities(self, g, partition, **kwargs):

        # create a weighted graph, in which each node corresponds to a community,
        # and each edge weight to the number of edges between communities
        between_community_edges = self._find_between_community_edges(g, partition)

        communities = set(partition.values())
        hypergraph = nx.DiGraph()
        hypergraph.add_nodes_from(communities)
        for (ci, cj), edges in between_community_edges.items():
            hypergraph.add_edge(ci, cj, weight=len(edges))

        # find layout for communities
        pos_communities = nx.spring_layout(hypergraph, **kwargs)

        # set node positions to position of community
        pos = dict()
        for node, community in partition.items():
            pos[node] = pos_communities[community]

        return pos

    def _find_between_community_edges(self, g, partition):

        edges = dict()

        for (ni, nj) in g.edges():
            ci = partition[ni]
            cj = partition[nj]

            if ci != cj:
                try:
                    edges[(ci, cj)] += [(ni, nj)]
                except KeyError:
                    edges[(ci, cj)] = [(ni, nj)]

        return edges

    def _position_nodes(self, g, partition, **kwargs):
        """
        Positions nodes within communities.
        """

        communities = dict()
        for node, community in partition.items():
            try:
                communities[community] += [node]
            except KeyError:
                communities[community] = [node]

        pos = dict()
        for ci, nodes in communities.items():
            subgraph = g.subgraph(nodes)
            pos_subgraph = nx.spring_layout(subgraph, **kwargs)
            pos.update(pos_subgraph)

        return pos

    def get_cmap(self, n, name='hsv'):
        '''Returns a function that maps each index in 0, 1, ..., n-1 to a distinct 
        RGB color; the keyword argument name must be a standard mpl colormap name.'''
        return plt.cm.get_cmap(name, n)

    def draw_nx_community2(self, limitdeg=1, limitcoo=1, title="noun network in article", min_component_size = 1):
        # to install networkx 2.0 compatible version of python-louvain use:
        # pip install -U git+https://github.com/taynaud/python-louvain.git@networkx2

        full_graph = nx.Graph(self.G)
        
        for H in nx.connected_component_subgraphs(full_graph, copy=True):
            if(min_component_size < H.number_of_nodes()):
        
                partition = community_louvain.best_partition(H)
                pos = self.community_layout(H, partition)

                nx.draw_networkx_labels(H,pos,fontsize=14)
                font = { 'color'      : 'k',
                        'fontweight' : 'bold',
                        'fontsize'   : 15}
                plt.title(title, font)

                plt.axis('off')
                nx.draw(H, pos, cmap=plt.get_cmap("Set3"), node_color=[float(x) for x in partition.values()])#[self.get_cmap(x) for x in partition.values()])

                plt.show()

    def draw_nx_community(self, limitdeg=1, limitcoo=1, title="noun network in article"):

        # make new undirected graph H without multi-edges
        H = nx.Graph(self.G)

        #H.remove_nodes_from([u for (u,w) in self.ocurence.items() if w < limitdeg])
        #H.remove_edges_from([e for (e,w) in self.coocurence.items() if w < limitcoo])

        # edge width is proportional number of games played
        edgewidth=[]
        for (u,v) in H.edges():
            edgewidth.append(self.coocurence[tuple(sorted((u,v)))])

        nodesize=[]
        for u in H.nodes():
            nodesize.append(self.ocurence[u]*70 + 100)

        #betweenness = nx.betweenness_centrality(g.G)
        #nodealpha=[]
        #for u in H.nodes():
        #    nodealpha.append(betweenness[u]*4 + 0.4)
        
        
        #first compute the best partition
        partition = community.best_partition(H)
        #drawing
        size = float(len(set(partition.values())))
        pos = nx.spring_layout(H)
        count = 0.
        for com in set(partition.values()) :
            count = count + 1.
            list_nodes = [nodes for nodes in partition.keys()
                                        if partition[nodes] == com]
            nx.draw_networkx_nodes(H, pos, list_nodes, node_size = 20,
                                        node_color = str(count / size))

        plt.rcParams['text.usetex'] = False
        plt.figure(figsize=(12,12))
        
        nx.draw_networkx_edges(H,pos,alpha=0.3,width=edgewidth, edge_color='m')
        #nx.draw_networkx_edges(H,pos,alpha=0.3, edge_color='m')
        #nodesize=[wins[v]*50 for v in H]
        #nx.draw_networkx_nodes(H,pos,node_size=nodesize,node_color='w',alpha=0.4)
        #nx.draw_networkx_nodes(H,pos,node_color='w',alpha=0.4)
        nx.draw_networkx_edges(H,pos,alpha=0.4,node_size=0,width=1,edge_color='k')
        nx.draw_networkx_labels(H,pos,fontsize=14)
        font = { 'color'      : 'k',
                'fontweight' : 'bold',
                'fontsize'   : 15}
        plt.title(title, font)

        plt.axis('off')
        #plt.savefig("noun_network.png",dpi=75)
        plt.show() # display
        #draw(H)


    def draw_nx_force(self, limitdeg=1, limitcoo=1, title="noun network in article", forceatlas2=None):

        # make new undirected graph H without multi-edges
        H = nx.Graph(self.G)

        #H.remove_nodes_from([u for (u,w) in self.ocurence.items() if w < limitdeg])
        #H.remove_edges_from([e for (e,w) in self.coocurence.items() if w < limitcoo])

        # edge width is proportional number of games played
        edgewidth=[]
        for (u,v) in H.edges():
            edgewidth.append(self.coocurence[tuple(sorted((u,v)))])

        nodesize=[]
        for u in H.nodes():
            nodesize.append(self.ocurence[u]*70 + 100)

        #betweenness = nx.betweenness_centrality(g.G)
        #nodealpha=[]
        #for u in H.nodes():
        #    nodealpha.append(betweenness[u]*4 + 0.4)

        #degree = nx.degree_centrality(g.G)
        if(not forceatlas2):
            forceatlas2 = ForceAtlas2(
                                  # Behavior alternatives
                                  outboundAttractionDistribution=True,  # Dissuade hubs
                                  linLogMode=False,  # NOT IMPLEMENTED
                                  adjustSizes=False,  # Prevent overlap (NOT IMPLEMENTED)
                                  edgeWeightInfluence=1.0,

                                  # Performance
                                  jitterTolerance=1.0,  # Tolerance
                                  barnesHutOptimize=True,
                                  barnesHutTheta=1.2,
                                  multiThreaded=False,  # NOT IMPLEMENTED

                                  # Tuning
                                  scalingRatio=200.0,
                                  strongGravityMode=False,
                                  gravity=1.0,

                                  # Log
                                  verbose=True)

        #positions = { i : (random.random(), random.random()) for i in H.nodes()}
        pos = forceatlas2.forceatlas2_networkx_layout(H, pos=None, iterations=2000)

        plt.rcParams['text.usetex'] = False
        plt.figure(figsize=(12,12))
        
        nx.draw_networkx_edges(H,pos,alpha=0.3,width=edgewidth, edge_color='m')
        #nx.draw_networkx_edges(H,pos,alpha=0.3, edge_color='m')
        #nodesize=[wins[v]*50 for v in H]
        nx.draw_networkx_nodes(H,pos,node_size=nodesize,node_color='w',alpha=0.4)
        #nx.draw_networkx_nodes(H,pos,node_color='w',alpha=0.4)
        nx.draw_networkx_edges(H,pos,alpha=0.4,node_size=0,width=1,edge_color='k')
        nx.draw_networkx_labels(H,pos,fontsize=14)
        font = { 'color'      : 'k',
                'fontweight' : 'bold',
                'fontsize'   : 15}
        plt.title(title, font)

        plt.axis('off')
        #plt.savefig("noun_network.png",dpi=75)
        plt.show() # display
        #draw(H)

    def draw_nx(self, limitdeg=1, limitcoo=1, title="noun network in article"):

        # make new undirected graph H without multi-edges
        H = nx.Graph(self.G)

        #H.remove_nodes_from([u for (u,w) in self.ocurence.items() if w < limitdeg])
        #H.remove_edges_from([e for (e,w) in self.coocurence.items() if w < limitcoo])

        # edge width is proportional number of games played
        edgewidth=[]
        for (u,v) in H.edges():
            edgewidth.append(self.coocurence[tuple(sorted((u,v)))])

        nodesize=[]
        for u in H.nodes():
            nodesize.append(self.ocurence[u]*70 + 100)

        #betweenness = nx.betweenness_centrality(g.G)
        #nodealpha=[]
        #for u in H.nodes():
        #    nodealpha.append(betweenness[u]*4 + 0.4)

        #degree = nx.degree_centrality(g.G)

        try:
            pos=nx.nx_agraph.graphviz_layout(H)
        except:
            pos=nx.spring_layout(H,iterations=20)

        plt.rcParams['text.usetex'] = False
        plt.figure(figsize=(12,12))
        
        nx.draw_networkx_edges(H,pos,alpha=0.3,width=edgewidth, edge_color='m')
        #nx.draw_networkx_edges(H,pos,alpha=0.3, edge_color='m')
        #nodesize=[wins[v]*50 for v in H]
        nx.draw_networkx_nodes(H,pos,node_size=nodesize,node_color='w',alpha=0.4)
        #nx.draw_networkx_nodes(H,pos,node_color='w',alpha=0.4)
        nx.draw_networkx_edges(H,pos,alpha=0.4,node_size=0,width=1,edge_color='k')
        nx.draw_networkx_labels(H,pos,fontsize=14)
        font = { 'color'      : 'k',
                'fontweight' : 'bold',
                'fontsize'   : 15}
        plt.title(title, font)

        plt.axis('off')
        #plt.savefig("noun_network.png",dpi=75)
        plt.show() # display
        #draw(H)