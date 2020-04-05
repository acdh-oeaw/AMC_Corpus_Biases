import io
from bs4 import BeautifulSoup as bs
import itertools as it
import functools as ft
from os import listdir, walk
import re
import os

#from .config import *

alnum = r'([^\s\w-])+'

def is_goodword(word):
    return len(word) > 1 and word.replace("-", "")[:-1].isalnum() and \
        (word[-1].isalnum() or word[-1] in [".", "-"])
    #MAYBE: remove stopwords

def process_empty_p(doc, pid, f, docid, helper):
    line = next(f)
    content = []

    while (line[0:4] != "</p>"):
        if(line[0:3] == "<s>"):
            s = process_empty_s(f, docid)
            if(s):
                content.append(s)
        else:
            if(line):
                content.append(line)
        line = next(f)

    return content

def process_empty_field(f, docid):
    line = next(f)
    content = []

    while (line[0:8] != "</field>"):
        if(line[0:2] == "<p"):
            p = process_empty_p(f, docid)
            if(p):
                content.append(p)
        elif(line[0:3] == "<s>"):
            s = process_empty_s(f, docid)
        else:
            if(line):
                content.append(line)

        line = next(f)

    return content

def process_empty_s(f, docid):
    line = next(f)
    content = []

    while (line[:4] != "</s>"):
        if(line[:3] in ["<g/>", "<p/>", "</g", "<g>"]):
            pass
        elif(line[0:6] == "<field"):
            if(line[13:-3] == "stichwort" or "inhalt" in line):
                line = next(f)
                while (line[0:8] != "</field>" and line[0:6] != "<field"):
                    line = next(f)
                if(line[0:6] == "<field"):
                    print("ERROR in " + docid + " : field in empty s should not contain other field.", line)
            else:
                line = next(f)
                if(line[0:8] != "</field>"):
                    print("ERROR in " + docid + " : field in s should be empty.", line)
        else:
            if(line):
                content.append(line)
        line = next(f)

    return content

def process_ent(ent, dict_ent, nerpos, word):                        
    if(nerpos == "B"):
        if(ent):
            nkey = " ".join(ent)
            if(nkey in dict_ent):
                dict_ent[nkey] += 1
            else:
                dict_ent[nkey] = 1

        ent = [word]
    elif(nerpos == "I"):
        if(not ent):
            ent = ["ERORR"]
        ent.append(word)
    else:
        if(ent):
            nkey = " ".join(ent)
            if(nkey in dict_ent):
                dict_ent[nkey] += 1
            else:
                dict_ent[nkey] = 1
            return None
    return ent

def process_s(doc, sid, f, docid, helper):
    line = next(f)

    #(ents_per, ents_org, ents_loc) = ([], [], [])
    cnchunk = None
    nchunks = []

    words_spacy = []
    good_words = []

    ent_spacy = None
    ent_combined = None
    ent_human = None
    
    sent = []

    helper[3] += 1
    while (line[:4] != "</s>"):
        #print(line.replace("\t", " "))
        if(line[:3] in ["<g/", "<p/", "</g", "<g>"]):
            pass
        elif(line[0:6] == "<field"):
            if(not line[13:-3] == "stichwort"):
                (pid, fps) = process_empty_field(f, docid)
                if(fps != []):
                    print("ERROR in " + doc["id"] + ": field in s should be empty")
                    print(fps)
        else:
            toks = line.split("\t")
            
            helper[0] += 1
            n_chars = len(toks[0])
            helper[1] += n_chars
            if (n_chars >= 7):
                helper[2] += 1
            
        line = next(f)

    return None

def process_p(doc, pid, f, docid, helper):
    line = next(f)

    i = 0
    sentences = []
    pid = docid + "." + pid

    while (line[0:4] != "</p>"):
        if(line[0:3] == "<s>"):
            s = process_s(doc, pid + "." + str(i), f, docid, helper)
            if(s):
                sentences.append(s)
            i += 1
        line = next(f)

    if(sentences != []):
        #autor = '' if not doc.has_attr("autor") else re.sub(alnum, '', doc["autor"])
        #region = '' if not doc.has_attr("region") else doc["region"]
        #docsrc = doc["docsrc"]

        #helper.write_p(pid, docid)#, doc["id"], doc["datum"], autor, docsrc, region)

        (nouns, adjectives, verbs) = (set(), set(), set())
        for (sid, n, a, v) in sentences:
            #helper.write_s_p(sid, pid)
            #add entity ids
            nouns.update(n)
            adjectives.update(a)
            verbs.update(v)
        return (nouns, adjectives, verbs)
    else:
        return None

def process_field(doc, field, pidx, f, docid, helper):
    line = next(f)

    content = []
    while (line[0:8] != "</field>"):
        if(line[0:2] == "<p"):
            p = process_p(doc, str(pidx), f, docid, helper)
            if(p):
                content.append(p)
            pidx += 1
        elif(line[0:3] == "<s>"):
            s = process_empty_s(f, docid)
            if(s and not (field == "stichwort" or field == "inhalt")):
                print("ERROR in " + doc["id"] + ": should not reach s in field " + field)
                print("ERROR: s should be empty")
                print(s)

        line = next(f)
    return (pidx, content)

def process_doc(doc, f, docid, pidx, helper):
    line = next(f)
    docid = str(doc["id"])#.split("_")[1]
    date = str(doc["datum"]).replace("-", "")
    ressort = str(doc["ressort2"]) #if "ressort2" in doc else ''
    #mediatype = doc["mediatype"]
    #autor = '' if not doc.has_attr("autor") else re.sub(alnum, '', doc["autor"])
    region = '' if not doc.has_attr("region") else doc["region"]
    docsrc = str(doc["docsrc"])
    
    #if(docsrc == "STANDARD"):
    #   print(docid + " " + ressort)

    #helper.write_doc(docid, date, docsrc, ressort, region)
    #if(ressort not in ["seite1", "seite3", "chronik", "titelseite", \
    #               "seite5", "politik", "thema", "inland", "lokal", \
    #               "bundesland" ,"ausland"]):
    #    return (pidx,"")

    content = []
    while (line[0:6] != "</doc>"):
        if(line[0:2] == "<p"):
            p = process_p(doc, str(pidx), f, docid, helper)
            if(p):
                content.append(p)
            pidx += 1
        elif(line[0:3] == "<s>"):
            s = process_empty_s(f, docid)
            
            #does not need to be empty
            if(s):
                pass
                #print("ERROR in " + doc["id"] + ": s should be empty")
                #print(s)
        elif(line[0:7] == "<field "):
            (pidx, fps) = process_field(doc, line[13:-3], pidx, f, docid, helper)
            content.extend(fps)

        line = next(f)
    return (pidx+1, content)

def process_file(fp, collect=True, helper=None):
    (path, text) = fp
    #print(path)
    lines = io.StringIO(text)
    fname = path.split("/")[-1].split(".")[0].split("_")
    #print(fname)
    
    #docid = fname[0][0:3] + str(int(fname[1][0:4])-2013) + fname[1][4:]
    #docsrc = fname[0]
    #date = fname[1]

    #helper.write_doc(docid, date, docsrc)
    
    pidx = 0
    ws = (set(), set(), set())
    for line in lines:
        if(line[:4] == "<doc"):
            tag = bs(line, "lxml")
            #tag = {"id":"1", "datum":"01-01-2001", "ressort2":"politik", "docsrc":"KURIER"}
            #print(line)
            (pidx, pd) = process_doc(tag.doc, lines, 0, pidx, helper)#docid, pidx, helper)
            #(pidx, pd) = process_doc(tag, lines, 0, pidx, helper)#docid, pidx, helper)
            for p in pd:
                for i in range(0,3):
                    ws[i].update(p[i])
            #ws = ft.reduce(lambda x,y: tuple([ x[i].union(y[i]) for i in range(0, 3)]), [ws] + pd)

    return pidx

def processcsv(datapath, helper): 
    ws = (set(), set(), set())
    for (root, dirs, files) in walk(datapath):
        #print(root)
        for file in files:
            process_file((file, open(os.path.join(root, file), encoding="utf-8").read()), helper=helper)