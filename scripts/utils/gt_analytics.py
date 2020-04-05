from graph_tool.all import *
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://192.168.99.100:7687", auth=basic_auth("neo4j","test"), encrypted=False)

session = driver.session()
result = session.run('MATCH (n:Word {wtype:"NN"})-[:IN]-(p:Paragraph) With Collect(n.name) as names, p.id as pid Return names')
#for multiple dates "Where p.date in ["2012-02-01", ...]"
session.close()

net = Graph()
#localVertices = []
#localEdges = []
for rec in result:
    paragraph = rec[0]
    for w in paragraph:
        net.addNode()
    for i in range(0, len(paragraph)):
        for n in paragraph[i:]:
            n, paragraph[i]
    localVertices.extend([v_row(s) for s in paragraph])
    localEdges.extend([e_row(n, paragraph[i]) for i in range(0, len(paragraph)) for n in paragraph[i:]])
