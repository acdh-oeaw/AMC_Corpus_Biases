from neo4j.v1 import GraphDatabase, basic_auth
from nx_graph import NxGraph

driver = GraphDatabase.driver("bolt://192.168.99.100:7687", auth=basic_auth("neo4j","test"), encrypted=False)

session = driver.session()
result = session.run('MATCH (n:Word {wtype:"NN"})-[:IN]-(p:Paragraph) With Collect(n.name) as names, p.id as pid Return names')
#for multiple dates "Where p.date in ["2012-02-01", ...]"
session.close()

g = NxGraph()
for rec in result:
    #print(str(rec[0]))
    g.addDoc(rec[0])
    g.endDoc()

g.draw(limitdeg=50, limitcoo=20)


"""
for rec in result:
    paragraph = rec[0]
    localVertices.extend(paragraph)
    localEdges.extend([tuple(sorted([n, paragraph[i]])) for i in range(0, len(paragraph)) for n in paragraph[i:]])

v = sqlContext.createDataFrame(localVertices, ["id"])
e = sqlContext.createDataFrame(localEdges, ["src", "dst"])
g = GraphFrame(v, e)
"""
