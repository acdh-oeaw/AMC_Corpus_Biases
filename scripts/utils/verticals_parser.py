import io
from bs4 import BeautifulSoup as bs
import itertools as it
import functools as ft
from os import listdir, walk
import re

from .config import *
from .verticals_svo_extraction import *

alnum = r'([^\s\w-])+'

def is_goodword(word):
    return len(word) > 1 and word.replace("-", "")[:-1].isalnum() and \
        (word[-1].isalnum() or word[-1] in [".", "-"])
    #MAYBE: remove stopwords

def process_empty_p(doc, pid, f, docid, csvwriter):
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
        if(line[:4] in ["<g/>", "<p/>"]):
            pass
        elif(line[0:6] == "<field"):
            line = next(f)
            if(line[0:8] != "</field>"):
                print("ERROR in " + docid + " : field in s should be empty.", line)
        else:
            if(line):
                content.append(line)
        line = next(f)

    return content

def process_s(doc, sid, f, docid, csvwriter):#, evocab):
    line = next(f)

    #(ents_per, ents_org, ents_loc) = ([], [], [])
    cnchunk = None
    nchunks = []

    ents = []
    words_spacy = []
    good_words = []

    ent = None
    sent = []

    while (line[:4] != "</s>"):
        #print(line.replace("\t", " "))
        if(line[:4] in ["<g/>", "<p/>"]):
            pass
        elif(False and line[0:6] == "<field"):
            if(not line[13:-3] == "stichwort" and not "inhalt" in line):
                (pid, fps) = process_empty_field(f, docid)
                if(fps != []):
                    print("ERROR in " + doc["id"] + ": field in s should be empty")
                    print(fps)
        else:
            toks = line.split("\t")
            word = toks[0]

            lemma = toks[3]
            #conversion for tree tagger
            """
            if(lemma[-1] in ["x", "m"] or lemma[:-2] == "<unknown>"):
                lemma = word
            else:
                lemma = lemma[:-2]
            """

            spacyner_offset = 2

            ent_tag = toks[8+spacyner_offset]
            ent_offset = 0
            if(ent_tag != "0"):
                ent_pos = toks[9+spacyner_offset]
            else:
                ent_pos = ""
                ent_offset = 1
            
            ent_id = toks[7+spacyner_offset]
            noun_chunk = toks[(10-ent_offset)+spacyner_offset]

            dep = toks[(11-ent_offset)+spacyner_offset]
            head = int(toks[(12-ent_offset)+spacyner_offset])

            rf_pos = toks[1]
            tt_pos = toks[2]
            spacy_pos = toks[5]
            spacy_tag = toks[6]

            if(ent_pos == "B"):
                if(ent):
                    ents.append(ent)
                ent = (ent_id, ent_tag, [word])#([ent_id], ent_tag, [word])
            elif(ent_pos == "I"):
                #ent[0].append(ent_id)
                ent[2].append(word)
            else:
                if(ent):
                    ents.append(ent)
                ent = None

            good_word = is_goodword(word) or ent

            wtype = [0] * 9
            if(ent):
                wtype[0] = 3.5
            else:
                spacy_factor = 3 #1.5
                #noun
                wtype[0] = int(rf_pos.startswith("N")) + int(tt_pos in ["NE", "NN"]) + \
                                int(spacy_pos in ["NOUN", "PROPN"]) * spacy_factor
                #adjective
                wtype[1] = int(rf_pos.startswith("ADJ")) + int(tt_pos.startswith("ADJ")) + \
                                int(spacy_pos in ["ADJ"]) * spacy_factor
                #verb
                wtype[2] = int(rf_pos.startswith("V")) + int(tt_pos.startswith("V")) + \
                                int(spacy_pos in ["VERB", "AUX"]) * spacy_factor
                #adverb
                wtype[3] = int(rf_pos.startswith("ADV")) + int(tt_pos in ["ADV"]) + \
                                int(spacy_pos in ["ADV"]) * spacy_factor
                #attributive pronoun
                wtype[4] = int("Pro" in rf_pos and "Attr" in rf_pos) + int(tt_pos in \
                                    ["PDAT", "PIAT", "PWAT", "PPER", "PRF", "PRELAT", "PPOSAT"]) + \
                                int(spacy_pos in ["DET", "ADP"]) * spacy_factor #--> relation finden
                #foreign word
                wtype[5] = int("FM" in rf_pos) + int(tt_pos in ["FM"]) + \
                                int(spacy_tag in ["FM"]) * spacy_factor
                #trucated word
                wtype[6] = int("TRUNC" in rf_pos) + int(tt_pos in ["TRUNC"]) + \
                                int(spacy_tag in ["TRUNC"]) * spacy_factor
                #negation
                wtype[7] = int("PTKNEG" in rf_pos) + int(tt_pos in ["PTKNEG"]) + \
                    int(spacy_tag in ["PTKNEG"]) * spacy_factor

                #other
                wtype[8] = int("PTKNEG" in rf_pos) + int(tt_pos in ["PTKNEG"]) + \
                    int(spacy_pos in ["PUNCT", "NUM", "SPACE", "PART", "PRON", "INTJ", "CONJ", "SCONJ"]) * spacy_factor + \
                    int(spacy_tag in ["XY"]) * spacy_factor

            #words_spacy.append( (word, ent_id, spacy_pos, spacy_tag, dep, good_word, head, wtype, lemma) )
            sent.append(word)

            if(good_word):
                if(noun_chunk):
                    if(cnchunk):
                        if(cnchunk[0] != noun_chunk):
                            nchunks.append(cnchunk)
                            cnchunk = (noun_chunk, [ent_id])
                        else:
                            cnchunk[1].append(ent_id)
                    else:
                        cnchunk = (noun_chunk, [ent_id])
                else:
                    if(cnchunk):
                        nchunks.append(cnchunk)
                    cnchunk = None

                if(not ent):
                    good_words.append( (word, ent_id, wtype) )

        line = next(f)
        #print(line.replace("\t", " "))

    if(ent):
        ents.append(ent)

        #sid = doc["id"] + "." + sid

    if(good_words):
        csvwriter.write_s(sid, good_words, " ".join(sent))
    if(ents):
        csvwriter.write_ents(sid, ents)

    #if(nchunks):
    #    csvwriter.write_nchunks(sid, nchunks)

    #TODO: write_a_n_s(self, awid, nwid, sid)

    #csvwriter.write_svo(sid, svo_triples( WordTree(words_spacy) ) )

        """
        (nouns, adjectives, verbs, adverbs) = ([], [], [], [])
        for (w, eid, wtype) in good_words:
            mw = maxwtype(wtype)
            if(mw == 0):
                nouns.append(w)
            elif(mw == 1):
                adjectives.append(w)
            elif(mw == 2):
                verbs.append(w)

        return (sid, nouns, adjectives, verbs)
        """
    return sid
    #else:
    #    return None

"""
def process_p(doc, pid, f, docid, csvwriter):
    line = next(f)

    i = 0
    sentences = []
    pid = docid + "." + pid

    while (line[0:4] != "</p>"):
        if(line[0:3] == "<s>"):
            s = process_s(doc, pid + "." + str(i), f, docid, csvwriter)
            if(s):
                sentences.append(s)
            i += 1
        line = next(f)

    if(sentences != []):
        #autor = '' if not doc.has_attr("autor") else re.sub(alnum, '', doc["autor"])
        #region = '' if not doc.has_attr("region") else doc["region"]
        #docsrc = doc["docsrc"]

        csvwriter.write_p(pid, docid)#, doc["id"], doc["datum"], autor, docsrc, region)

        (nouns, adjectives, verbs) = (set(), set(), set())
        for (sid, n, a, v) in sentences:
            csvwriter.write_s_p(sid, pid)
            #add entity ids
            nouns.update(n)
            adjectives.update(a)
            verbs.update(v)
        return (nouns, adjectives, verbs)
    else:
        return None
"""

def process_field(doc, field, pidx, f, docid, csvwriter):
    line = next(f)

    #sentences = []
    while (line[0:8] != "</field>"):
        if(line[0:3] == "<s>"):
            s = process_s(doc, pidx, f, docid, csvwriter)
            if(s):
                if(field == "inhalt"):
                    csvwriter.write_content(s, docid)
                elif(field == "stichwort"):
                    csvwriter.write_keywords(s, docid)
                elif(field == "titel"):
                    csvwriter.write_title(s, docid)
                else:
                    print("ERROR in " + doc["id"] + ": should not reach s in field '" + field + "'")
                    print(s)
                
                #sentences.append(s)
            pidx += 1
        line = next(f)
    return (pidx, None)
"""
    if(sentences != []):
        #autor = '' if not doc.has_attr("autor") else re.sub(alnum, '', doc["autor"])
        #region = '' if not doc.has_attr("region") else doc["region"]
        #docsrc = doc["docsrc"]

        csvwriter.write_p(pid, docid)#, doc["id"], doc["datum"], autor, docsrc, region)

        (nouns, adjectives, verbs) = (set(), set(), set())
        for (sid, n, a, v) in sentences:
            if(field == "inhalt"):
                csvwriter.write_content(sid, docid)
            if(field == "stichwort"):
                csvwriter.write_keywords(sid, docid)
            if(field == "titel"):
                csvwriter.write_title(sid, docid)
            else:
                print("ERROR in " + doc["id"] + ": should not reach s in field " + field)
                print(s)
            #csvwriter.write_s_p(sid, pid)
            #add entity ids
            #nouns.update(n)
            #adjectives.update(a)
            #verbs.update(v)
        return (pidx, None)#sentences)#(nouns, adjectives, verbs)
    else:
        return (pidx, None)
"""

"""
        if(line[0:2] == "<p"):
            p = process_p(doc, str(pidx), f, docid, csvwriter)
            if(p):
                content.append(p)
            pidx += 1
        elif(line[0:3] == "<s>"):
            s = process_empty_s(f, docid)
            if(s and not field in ["stichwort", "inhalt"]):
                print("ERROR in " + doc["id"] + ": should not reach s in field " + field)
                print("ERROR: s should be empty")
                print(s)

        line = next(f)
    return (pidx, content)
"""

def process_doc(doc, f, docid, pidx, csvwriter):
    line = next(f)
    docid = str(doc["id"])#.split("_")[1]
    date = str(doc["datum"]).replace("-", "")
    ressort = str(doc["ressort2"]) #if "ressort2" in doc else ''
    #mediatype = doc["mediatype"]
    #autor = '' if not doc.has_attr("autor") else re.sub(alnum, '', doc["autor"])
    region = '' if not doc.has_attr("region") else doc["region"]
    docsrc = str(doc["docsrc"])
    parser_params = str(doc["parser_params"])

    print(docid + " " + ressort, flush=True)

    csvwriter.write_doc(docid, date, docsrc, ressort, region, parser_params)

    #content = []
    while (line[0:6] != "</doc>"):
        #if(line[0:2] == "<p"):
        #    p = process_p(doc, str(pidx), f, docid, csvwriter)
        #    if(p):
        #        content.append(p)
        #    pidx += 1
        #el
        if(line[0:3] == "<s>"):
            s = process_empty_s(f, docid)
            if(s):
                print("ERROR in " + doc["id"] + ": s should be empty")
                print(s)
        elif(line[0:7] == "<field "):
            (pidx, fps) = process_field(doc, line[13:-3], pidx, f, docid, csvwriter)
            #content.extend(fps)

        line = next(f)
    return (pidx, None)#content)

def process_file(fp, collect=True, csvwriter=None):
    (path, text) = fp
    #print(path)
    lines = io.StringIO(text)
    fname = path.split("/")[-1].split(".")[0].split("_")
    print(fname, flush=True)

    #docid = fname[0][0:3] + str(int(fname[1][0:4])-2013) + fname[1][4:]
    #docsrc = fname[0]
    #date = fname[1]

    #csvwriter.write_doc(docid, date, docsrc)

    pidx = 0
    #ws = (set(), set(), set())
    for line in lines:
        if(line[:4] == "<doc"):
            tag = bs(line, "lxml")
            #tag = {"id":"1", "datum":"01-01-2001", "ressort2":"politik", "docsrc":"KURIER"}
            #print(line)
            (pidx, pd) = process_doc(tag.doc, lines, 0, pidx, csvwriter)#docid, pidx, csvwriter)
            #(pidx, pd) = process_doc(tag, lines, 0, pidx, csvwriter)#docid, pidx, csvwriter)
            #for p in pd:
            #    for i in range(0,3):
            #        ws[i].update(p[i])
            #ws = ft.reduce(lambda x,y: tuple([ x[i].union(y[i]) for i in range(0, 3)]), [ws] + pd)

    return None#ws