from neo4j.v1 import GraphDatabase, basic_auth
import networkit as nk
import graph_tool as gt
import networkx as nx
from pyspark.graphframes import *
from pyspark.sql import Row
import igraph as ig
import csv

from .graph_vis import NxpdGraph
from .config import bolt_url, results_path

entities = [":ORG", ":LOC", ":PER", ":MISC"]
entities_stop = [":ENT_ORG_STOP", ":ENT_LOC_STOP", ":ENT_PER_STOP", ":ENT_MISC_STOP"]
pos = [":ADJ", ":ADVERB", ":NOUN", "VERB"]
pos_stop = [":ADJ_SENT_STOP", ":ADVERB_SENT_STOP", ":NOUN_SENT_STOP", ":VERB_SENT_STOP"]
emotions = [":EKEL", ":FREUDE", ":FURCHT", ":TRAUER", ":UEBERRASCHUNG", ":VERACHTUNG", ":WUT"]
valence = [":POS", ":NEG", ":NEUT"]

def send_neo(req, uwpar=None, user="neo4j", pw="test"):
    driver = GraphDatabase.driver(bolt_url, auth=basic_auth(user,pw), encrypted=False)
    session = driver.session()
    if(uwpar):
        result = session.run(req, uwpar)
    else:
        result = session.run(req)
    session.close()
    return result

def filter_doc(docsrc, date, start, end, ressort):
    ret = ""
    if(start and end):
        ret = "USING INDEX d:Document(date)\n"
        ret += "Where d.date >= " + str(start) + " and d.date <= " + str(end)
    elif(date):
        ret = "USING INDEX d:Document(date)\n"
        ret += "Where d.date = " + str(date)
    
    if(docsrc):
        if(ret):
            ret += " and d.docsrc in " + str(docsrc)
        else:
            ret = "Where d.docsrc in " + str(docsrc)
    
    if(ressort):
        if(ret):
            ret += " and (d.ressort CONTAINS '" + str(ressort[0]) + "'"
        else:
            ret = "Where (d.ressort CONTAINS '" + str(ressort[0]) + "'"
            
        for i in range(1, len(ressort)):
            ret += " or d.ressort CONTAINS '" + str(ressort[i]) + "'"
        ret += ")"

    return ret

def get_center(center, wlabel="word"):
    if(center):
        return " {" + wlabel + ": '" + center + "'}"
    else:
        return ""
    
def read_paragraphs(center=None,stopwords=None,n1type=":Ent",n2type=":Ent",pos1=[":NOUN"], pos2=[":NOUN"],freq=10, docsrc=None, date=None, start=None, end=None, ressort=None, wlabel="word"):

    return send_neo("""
    Match (p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """

    Match """ + "(n1" + n1type + get_center(center, wlabel) + \
          ")-[" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(:Sentence)-[:PART]-(p)-[:PART]-(:Sentence)-[" + \
          "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2" + n2type + ")" + \
    (("Where n1." + wlabel + " < n2." + wlabel + " ") if pos1 == pos2 else "")  + \
    "With n1." + wlabel + " as n1name, n2." + wlabel + " as n2name, COUNT(n2." + wlabel + """) as nweight
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """

    Return n1name, n2name, nweight
    """)
def read_sentences(center=None,stopwords=None,n1type=":Ent",n2type=":Ent",pos1=[":NOUN"], pos2=[":NOUN"], freq=10, docsrc=None, date=None, start=None, end=None, ressort=None, wlabel="word", has_wid=False):

    #print("""
    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """

    Match """ + "(n1" + n1type + get_center(center, wlabel) + \
          ")-[e1" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(s)-[e2" + "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2" + n2type + ")" + \
    (("Where n1." + wlabel + " < n2." + wlabel + " " + \
     ("AND (exists(n1.wikidataid) OR NOT type(e1) = \"NOUN\") AND (exists(n2.wikidataid) OR NOT type(e2) = \"NOUN\")" if has_wid else "")
     ) if pos1 == pos2 else "")  + \
    "With n1." + wlabel + " as n1name, n2." + wlabel + " as n2name, COUNT(n2." + wlabel + """) as nweight
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """

    Return n1name, n2name, nweight
    """)

def read_occurance(stopwords=None,n1type=":Ent",pos1=[], freq=0, docsrc=None, date=None, start=None, end=None, ressort=None, wlabel="word", has_wid=False):

    #print("""
    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """
    Match """ + "(n1" + n1type + \
          ")-[e1" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(s)\n" + \
    ("Where (exists(n1.wikidataid) OR NOT type(e1) = \"NOUN\")" if has_wid else "") + \
    "With n1." + wlabel + " as n1name, n1.wikidataid as nwid, COUNT(n1." + wlabel + """) as nweight
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """

    Return n1name, nwid, nweight
    """)

def read_sna(center=None,stopwords=None,n1type=":Ent",n2type=":Ent",pos1=[":NOUN"], pos2=[":NOUN"],freq=1, docsrc=None, date=None, start=None, end=None, ressort=None, wlabel="word"):
    
    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """
    OPTIONAL Match (s:Sentence)-[r1:POS]-(pos:Pos)
        WITH s, SUM(CASE WHEN pos.valence IS NULL THEN 0 ELSE toFloat(pos.valence) END) as possum
    OPTIONAL MATCH(s:Sentence)-[r2:NEG]-(neg:Neg)
        WITH s, SUM(CASE WHEN neg.valence IS NULL THEN 0 ELSE toFloat(neg.valence) END) as negsum, possum
    With s, (CASE WHEN (possum - negsum) > 0 THEN "Positive" WHEN (possum - negsum) < 0 THEN "Negative" ELSE "Neutral" END) as valsum
    
    Match """ + "(n1" + n1type + get_center(center, wlabel) + \
          ")-[" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(s)-[" + "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2" + n2type + ")" + \
    (("Where n1." + wlabel + " < n2." + wlabel + " ") if pos1 == pos2 else "")  + \
    "With n1." + wlabel + " as n1name, n2." + wlabel + " as n2name, COUNT(n2." + wlabel + """) as nweight, valsum
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """
    
    WITH n1name, n2name, valsum, nweight
    Order By n1name, n2name, nweight DESC, valsum
    RETURN n1name, n2name, HEAD(COLLECT(nweight)), HEAD(COLLECT(valsum))""")

def read_sentence_emotion(center=None,stopwords=None,n1type=":Ent",n2type=":Ent",pos1=[":NOUN"], pos2=[":NOUN"],freq=1, docsrc=None, date=None, start=None, end=None, ressort=None, emotion=None, wlabel="word"):
    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """
    Match (s:Sentence)-[""" + "|".join(emotion) + """]-(e)
    With s, Count(e.word) as emcnt
    
    Match """ + "(n1" + n1type + get_center(center, wlabel) + \
          ")-[" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(s)-[" + "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2" + n2type + ")" + \
    (("Where n1." + wlabel + " < n2." + wlabel + " ") if pos1 == pos2 else "")  + \
    "With n1." + wlabel + " as n1name, n2." + wlabel + """ as n2name, SUM(emcnt) as nweight
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """

    Return n1name, n2name, nweight
    """)
def read_sentence_max_emotion(center=None,stopwords=None,n1type=":Ent",n2type=":Ent",pos1=[":NOUN"], pos2=[":NOUN"],freq=1, docsrc=None, date=None, start=None, end=None, ressort=None, filter_ent=False, wlabel="word"):

    #return send_neo("""
    print("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """
    Match (s:Sentence{sid: "KRONE_20160831326960173.90.3"})-[r""" + "|".join(emotions) + """]-(e)
    RETURN s, type(r) as rtype, COUNT(r) as rcnt
    ORDER BY rcnt DESC
    WITH s, HEAD(COLLECT(rcnt)) AS emcnt, HEAD(COLLECT(rtype)) AS emotion

    Match """ + "(n1" + n1type + get_center(center, wlabel) + \
          ")-[" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(s)-[" + "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2" + n2type + ")" + \
    (("Where n1." + wlabel + " < n2." + wlabel + " ") if pos1 == pos2 else "")  + \
    "With n1." + wlabel + " as n1name, n2." + wlabel + """ as n2name, SUM(emcnt) as nweight, emotion
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """

    Order BY n1name, n2name, nweight DESC, emotion
    RETURN n1name, n2name, HEAD(COLLECT(nweight)), HEAD(COLLECT(emotion))
    """)

#TODO: check if really works or multiples are returned
def read_sna_svo(center=None,stopwords=None,n1type=":Ent",n2type=":Ent",pos1=[":SVO_SUBJ"], pos2=[":SVO_OBJ"],freq=1, docsrc=None, date=None, start=None, end=None, ressort=None, wlabel="word", subjtype=[], objtype=[]):
    stopwords = False

    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """
    Match (v)-[r:POS|:NEG]-(s)-[:SVO_SENT]-(n:Nvn)-[:SVO_VERB]-(v), """ + "(n1" + n1type + get_center(center, wlabel) + \
          ")-[" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n)-[" + "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2" + n2type + ")" + \
          (("Where (n2)-[" + "|".join(subjtype) + "]-(s)-[" + "|".join(objtype) + "]-(n1)") if subjtype and objtype else "") + \
    (("Where n1." + wlabel + " < n2." + wlabel + " ") if pos1 == pos2 else "")  + \
    "With n1." + wlabel + " as n1name, n2." + wlabel + """ as n2name,
        (CASE WHEN type(r) = "POS" THEN "Positive" ELSE "Negative" END) as valsum, 
        COUNT(DISTINCT r) AS nweight
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """

    Order By n1name, n2name, nweight DESC, valsum
    RETURN n1name, n2name, HEAD(COLLECT(nweight)), HEAD(COLLECT(valsum))""")
    
def read_svo(center=None,stopwords=None,freq=1, docsrc=None, date=None, start=None, end=None, ressort=None, is_neg=False, is_neut=False, is_pos=False, wlabel="word"):
    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """
    
    Match (v)-[r:POS|:NEG]-(s)-[:SVO_SENT]-(n:Nvn)-[:SVO_VERB]-(v), """ + "(n1" + n1type + get_center(center, wlabel) + \
          ")-[" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n)-[" + "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2" + n2type + ")" + \
    (("Where n1." + wlabel + " < n2." + wlabel + " ") if pos1 == pos2 else "")  + \
    "and type(r) == " + ("POS" if is_pos else ("NEG" if is_neg else ("NEUT" if is_neut else "POS"))) + """
    With n1.""" + wlabel + " as n1name, n2." + wlabel + " as n2name, COUNT(n2." + wlabel + """) as nweight,
         SUM(v.valence) as vsum """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """
    Return n1name, n2name, nweight""")
    #(nweight*vsum*1000""" + (" * -1" if is_neg else "") + """)


def get_freq(word=None, wikidataid=None, docsrc=None, date=None, start=None, end=None, ressort=None):
    
    return list(send_neo(
        'MATCH (n:Ent {' + (('wikidataid:"' + wikidataid + '"') if wikidataid else "") + \
        (('word:"' + word + '"') if word else "") + \
        '})-[r]-(s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document) ' + \
            filter_doc(docsrc, date, start, end, ressort) + \
         "RETURN COUNT(DISTINCT s)"))[0][0]
    
#list svo + 1 sentence from vanderbellen
def list_svo(word=None, wikidataid=None, pos1=[":SVO_SUBJ"], pos2=[":SVO_OBJ"], asobj=False, docsrc=None, date=None, start=None, end=None, ressort=None):
    
    if(asobj):
        pos2=[":SVO_SUBJ"]
        pos1=[":SVO_OBJ"]
    
    return [(rec[0], rec[1], rec[2], rec[3]) for rec in send_neo(
        'MATCH (n:Ent {' + (('wikidataid:"' + wikidataid + '"') if wikidataid else "") + \
        (('word:"' + word + '"') if word else "") + \
        '})-[r' + "|".join(pos1) + ']-(x:Nvn)-[:SVO_SENT]-(s)' + \
        "-[:PART]-(p:Paragraph)-[:PART]-(d:Document)" + filter_doc(docsrc, date, start, end, ressort) + \
        "MATCH (x)-[y:SVO_VERB]-(v:Ent), (x)-[f" +  "|".join(pos2) + "]-(l:Ent) " + \
        "RETURN DISTINCT v.word, l.word, COUNT(DISTINCT s), collect(s.original)[0]\n" + \
        "ORDER BY COUNT(DISTINCT s) DESC")]

#TODO: don't understand what goal is of this network
def read_sentences_val(center=None,stopwords=None,n1type="Noun",n2type="Noun",freq=1, docsrc=None, date=None, start=None, end=None, ressort=None, valence=[":POS"]):
    
    #print("""
    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """ 
    OPTIONAL Match (s:Sentence)-[r1""" + "|".join(valence) + """]-(n)
    WITH s, SUM(CASE WHEN n.valence IS NULL THEN 0 ELSE toFloat(n.valence) END) as nsum
    
    
    Match """ + "(n1:" + n1type + get_center(center, wlabel) + \
          ")-[" + "|".join(pos1) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(s)-[" + "|".join(pos2) + ("|" + "|".join([p + "_STOP" for p in pos1]) if stopwords else "") + \
          "]-(n2:" + n2type + ")" + \
    (("Where n1." + wlabel + " < n2." + wlabel + " ") if pos1 == pos2 else "")  + \
    "With n1." + wlabel + " as n1name, n2." + wlabel + " as n2name, COUNT(n2." + wlabel + """) as nweight, nsum
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """

    Return n1name, n2name, SUM(nweight)""")
    #SUM(nweight * nsum * 1000""" + (" * -1" if is_neg else "") + """)

#TODO: rework to many Persons - adjectives networks, calculate similarity of person based on adjectives
def read_assoc_adjectives(center=None,stopwords=None,freq=1, docsrc=None, date=None, start=None, end=None, ressort=None, filter_ent=False):
    return send_neo("""
    Match (s:Sentence)-[:PART]-(p:Paragraph)-[:PART]-(d:Document)
    """ + filter_doc(docsrc, date, start, end, ressort) + """
    Match (n:An)-[:IN]-(s:Sentence), (n:An)-[:PART]-(nn:Noun"""  + get_center(center) + ")" + \
                    ("-[:PART]-(:Ent)-[:IN]-(s:Sentence)" if filter_ent else "") + """, (n:An)-[:PART]-(na:Adjective)
    """ + ("Where nn:Stop and NOT na:Stop " if stopwords else "") + \
    """With nn.wid as n1name, na.wid as n2name, COUNT(na.wid) as nweight
    """ + (("Where nweight > " + str(freq)) if freq > 1 else "") + """
    Return n1name, n2name, nweight
    """)

def load_nxpd_graph(result):
    nnx = NxpdGraph()
    wmap_nx = load_graph(nnx, result, lambda g, w: g.add_node(w), lambda g, v1, v2, w: [g.add_edge(v1,v2) for _ in range(w)])
    return (nnx, wmap_nx)


def load_gf_graph(result):
    localVertices = set()
    localEdges = []
    print("parse result...")
    for rec in result:
        paragraph = rec[0]
        #print(paragraph)
        for s in paragraph:
            localVertices.add(s)
        localEdges.extend([(n, paragraph[i]) for i in range(0, len(paragraph)) for n in paragraph[i:]])

    vertex = Row("id")
    v = sqlContext.createDataFrame([vertex(lv) for lv in localVertices])
    e = sqlContext.createDataFrame(localEdges).toDF("src", "dst")
    
    ngf = GraphFrame(v, e)

    return ngf

def load_nx_graph(result):
    def add_nx_node(g,w):
        g.add_node(w)
        return w
    
    nnx = nx.Graph()
    wmap_nnx = load_graph(nnx, result, add_nx_node, lambda g, v1, v2, w: g.add_edge(v1,v2,weight=w))
    return (nnx, wmap_nnx)

def load_ig_graph(result):
    nig = ig.Graph(directed=False)
    wmap_nig = load_graph(nig, result, lambda g, w: g.add_vertex(w), lambda g, v1, v2 ,w: [g.add_edge(v1,v2) for _ in range(w)])
    return (nig, wmap_nig)

def load_gt_graph(result):
    ngt = gt.Graph(directed=False)
    wmap_ngt = load_graph(ngt, result, lambda g, w: g.add_vertex(), lambda g, v1, v2, w: [g.add_edge(v1,v2) for _ in range(w)])
    return (ngt, wmap_ngt)

def load_nk_graph(result):
    nkit = nk.Graph(weighted=True)
    wmap_nkit = load_graph(nkit, result, lambda g, w: g.addNode(), lambda g, v1, v2, w: [g.addEdge(v1,v2) for _ in range(w)])
    return (nkit, wmap_nkit)

def load_graph(graph, result, add_node, add_edge, attrs=False):#, occurance=lambda graph, x: None):
    wmap = {}

    for rec in result:
        if(not rec[0] in wmap):
            v1 = add_node(graph, rec[0])
            wmap[rec[0]] = v1
        else:
            v1 = wmap[rec[0]]
        if(not rec[1] in wmap):
            v2 = add_node(graph, rec[1])
            wmap[rec[1]] = v2
        else:
            v2 = wmap[rec[1]]

        if(attrs):
            add_edge(graph, v1, v2, int(rec[2]), [rec[i] for i in range(3, len(rec))])
        else:
            add_edge(graph, v1, v2, int(rec[2]))

    return wmap

def store_graph_gexf(fname, result, rpath=results_path, mincompsize=None):
    (nnx, wm) = load_nx_graph(result)
    if(mincompsize):
        joinedg = nx.Graph()
        for H in nx.connected_component_subgraphs(nnx, copy=True):
            if(min_component_size < H.number_of_nodes()):
                disjoint_union(joinedg, H)
        nnx = joinedg

    nx.write_gexf(nnx, rpath + "/" + fname + ".gexf")

def store_graph_gexf_color(fname, result, dcolors, rpath=results_path, mincompsize=None):
    def add_node(g,w):
        g.add_node(w)
        return w
    def add_edge(g, v1, v2, w, attrs):
        c = dcolors[attrs[0]]
        return g.add_edge(v1,v2,weight=w, viz={"color": {"a":1, "r":c[0], "g":c[1], "b":c[2]}})

    nnx = nx.Graph()
    wmap_nnx = load_graph(nnx, result, add_node, add_edge, attrs=True)
    if(mincompsize):
        joinedg = nx.Graph()
        for H in nx.connected_component_subgraphs(nnx, copy=True):
            if(min_component_size < H.number_of_nodes()):
                disjoint_union(joinedg, H)
        nnx = joinedg

    nx.write_gexf(nnx, rpath + "/" + fname + ".gexf")

def write_colorcode(fname, dcolors):
    zcolors = list(dcolors.items())
    with open(results_path + "/" + fname, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(zcolors)

def store_graph_gml(fname, result):
    (nnx, wm) = load_nx_graph(result)
    nx.write_graphml(nnx, results_path + "/" + fname + ".gml")

def store_graph_csv(fname, result):
    with open(results_path + "/" + fname + ".csv", "w") as f:
        load_graph(None, result, lambda g, w: w, lambda g, v1, v2, w: f.write(v1 + ";" + v2 + ";" + str(w) + "\n"))