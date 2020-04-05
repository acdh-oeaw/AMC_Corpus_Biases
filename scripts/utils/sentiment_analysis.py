from os import listdir, walk
from os.path import isfile, join
import spacy
    
from .config import *
from .graph_io import *

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]
        
def add_neo4j_valence(ws, stype):
    for sendws in batch(ws, 100):
        send_neo("""
            UNWIND {ws} AS sentiw
            Match (n)
            WHERE (n:Noun OR n:Verb OR n:Adjective) AND n.wid = sentiw.wid
            SET n :""" + stype + """
            SET n.valence = sentiw.val
            """, uwpar={'ws': sendws})
            
def add_neo4j_sentiment():
    (positives, negatives) = load_sentiws()
    positives = [w for (w, v) in positives.items()]#{"wid": w, "val": v}
    negatives = [w for (w, v) in negatives.items()]
    
    for ntype in ["Noun", "Verb", "Adjective"]:
        for pos in batch(positives, 500):
            add_neo4j_label(ntype, pos, "Positive")
            print("send pos")
        for nes in batch(negatives, 500):
            add_neo4j_label(ntype, nes, "Negative")
    #add_neo4j_valence(positives, "Positive")
    #add_neo4j_valence(negatives, "Negative")

def load_emotions():
    emotions = dict()
    
    for (root, dirs, files) in walk(emotion_dir):
        for f in files:
            with open(join(root, f), "r") as emo_f:
                emotions[f.split(".")[0]] = emo_f.read().lower().split()
    
    return emotions

def add_neo4j_label(ntype, ws, stype, wlabel="wid"):
#USING INDEX n:""" + ntype + """(""" + wlabel + """)
    #send_neo("""
    print("""
Match (n:""" + ntype + """)
WHERE 0 < size([w in """ + str(ws) + """ WHERE p in toLower(n.""" + wlabel + """)])
SET n :""" + stype + """
        """)

def add_neo4j_emotions():
    emos = load_emotions()
    for (stype, ws) in emos.items():
        for ntype in ["Ent"]:#["Noun", "Verb", "Adjective", "Adverb"]:
            add_neo4j_label(ntype, ws, stype)

def add_stop_words():
    ger_stopwords = load_stop_words()

    wlabel="Word"
    for ntype in ["Ent"]: # ["Noun", "Verb"]:#, "Adjective", "Adverb"]:
        add_neo4j_label(ntype, ger_stopwords, "Stop", wlabel)

def load_stop_words():
    with open("/tmp/data/stopwords-json/dist/de.json", "r") as f:
        ger_stopwords = list(set(["mehr", "ganz", "kurz", "macht", "geht", "list", "radio-tipps", "switch", "neue", \
                                  "heute", "euro", "beim", "zwei", "gibt", "drei", "jahre", "neuer", "wurde", \
                                  "schon", "zurück", "neues", "überblick", "the", "apa", "prozent", "utl", \
                                  "österreich", "amp", "laut", "-jährige", "leben", "http", "land", "stadt", \
                                  "montag", "dienstag", "mittwoch", "donnerstag", "freitag", "samstag", "sonntag", \
                                  "derzeit", "wwwotsat", "ots", "geben", "stehen", "montag", "dienstag", "mittwoch", \
                                  "donnerstag", "freitag", "samstag", "sonntag", "jänner", "jahr", "prozent", \
                                  "dame", "herr", "punkt", "alter", "januar", "februar", "märz", "april", "mai", \
                                  "juni", "juli", "august", "september", "oktober", "november", "dezember", "fax", \
                                  "hund", "katze", "kater", "presse", "agentur", "bild", "3", "standard", \
                                 'tel', 'hl', 'd', 'omu', '\uecab', "zeit", "tag", "für", "\ueaf6", "seite"] + \
                                 [s.lower() for s in json.load(f)]) | spacy.lang.de.STOP_WORDS)
    return ger_stopwords


def load_sentiws_line(line, word_filter):
    row = line.lower().split()
    if(word_filter and not any(lambda x: x in row[0], word_filter)):
        return []
    else:
        f = [(row[0].split("|")[0],row[1])]
        if(len(row) > 2):
            return f + [(v,row[1])for v in row[2].split(",")]
        else:
            return f            

def load_sentiws(word_filter=None):
    with open(positives_f, encoding="utf-8") as f:
        positives = dict([t for line in f.readlines() for t in load_sentiws_line(line, word_filter)])
    positives.update({"punktet": 0.4, "stimmten": 0.1, "gewann": 0.3,
                      "siegte": 0.3, "gewinnt": 0.3, "bedankte": 0.4, "überzeugt": 0.3,
                      "pries": 0.3, "inspiriert": 0.2})

    del positives["erklärte"]
    del positives["investierte"]

    with open(negatives_f, encoding="utf-8") as f:
        negatives = dict([t for line in f.readlines() for t in load_sentiws_line(line, word_filter)])
    negatives.update({"verhindern": -0.4, "reklamiert": -0.2, "bedauerte": -0.3,
                      "anfechten": -0.2, "verliert": -0.2,
                      "konterte": -0.2, "bezweifeln": -0.2, "spaltet": -0.1,
                      "widerspricht": -0.2})
    
    return (positives, negatives)