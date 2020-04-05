from .neocsvwriter import NeoCSVWriter
from .verticals_parser import *
from .ner_wikidata import *
from .file_io import *
import spacy
from spacy.tokens import Doc
import pickle
import os
import io
from os.path import isfile
from article_parser.coref import *
from bs4 import BeautifulSoup as bs
from .extract_wikidataentities import *

#special spacy tokenizer to override spacys builtin one and use verticals tokenization instead
class VerticalsTokenizer(object):
    def __init__(self, nlp):
        self.vocab = nlp.vocab

    def __call__(self, text):
        d = Doc(self.vocab, words=text[0], spaces=text[1])
        d.user_data = text[2]
        return d
    
    # add dummy methods for to_bytes, from_bytes, to_disk and from_disk to
    # allow serialization (see #1557)
    def to_bytes(self, **exclude):
        return b''

    def from_bytes(self, bytes_data, **exclude):
        return self

    def to_disk(self, path, **exclude):
        return None

    def from_disk(self, path, **exclude):
        return self

def enrich_vertical_files(sc, fpaths, opath, nlp_vert=None, entity_vocab={},\
                          candidatefreq={}, id_counter=0, ents={}):
    
    #load spacy nlp for german
    if(not nlp_vert):
        nlp_vert = spacy.load("de_core_news_sm")
        nlp_vert.make_doc = VerticalsTokenizer(nlp_vert)
        print("nlp_loaded...")
    
    ie_vocab_bc = sc.broadcast(load_vocabulary("/notebooks/data/new_vocab.pickle", "/notebooks/data/org_at_dict.pickle"))
    for fpath in fpaths:
        print(fpath, flush=True)
        #preprocess articles and store sentences with metadata such as found wikidata entities in pickle files
        ra = read_articles(fpath, sc, \
            map_fpartioned=lambda idx, it: extract_tok_sents(idx, it, ie_vocab_bc, opath, "pickle_")).count()

        #read previously stored pickle files and parse senteces via spacy and consolidate with wikidata entitites
        #finally create new vertical files
        id_counter = pickle_to_spacy_verticals(opath, nlp_vert, entity_vocab, candidatefreq, id_counter, ents)
        #delete prev. created pickle files 
        for (root, dirs, files) in walk(opath):
                for fname in files:
                    os.remove(os.path.join(root, fname))

    #return dictionary with words to entitiy id found, frequencies for words, and the highest current entitiy id 
    return (entity_vocab, candidatefreq, id_counter, ents)    

def pickle_to_spacy_verticals(ppath, nlp_vert, entity_vocab, candidatefreq, id_counter, ents):

    for (root, dirs, files) in walk(ppath):
        for fname in files:
            if(not ".obj" in fname):
                print(os.path.join(ppath, fname))
                with open(os.path.join(ppath,fname), "rb") as f:
                    sents = []
                    while 1:
                        try:
                            sents.extend(pickle.load(f))
                        except EOFError:
                            break
                    id_counter = verticals_integrate_spacy(sents, nlp_vert, entity_vocab, candidatefreq, id_counter, ents)
    return id_counter
            
def next_candidate(ents, i):
    ent_types = ["PER", "LOC", "ORG"]
    ent_start = -1
    ent_end = -1
    ret = None
    done = False

    while(not done):
        done = True
        for i, e in enumerate(ents):
            if(e and not (ret and e[0] is ret[1]) ):
                if(ent_start == -1 or \
                   (ent_start > e[0][0] and ent_start > e[0][1])):
                    ent_start = e[0][0]
                    ent_end = e[0][1]
                    ret = (i, e[0])
                elif(ent_start >= e[0][0]):
                    if( ((not (ret[1][2][0] and ret[1][2][1]) ) and (e[0][2][0] and e[0][2][1]) ) or \
                       ((ent_end - ent_start) < (e[0][1] - e[0][0]) and (e[0][2][0] and e[0][2][1])) ):
                        ent_start = e[0][0]
                        ent_end = e[0][1]
                        del ents[ret[0]][0]
                        ret = (i, e[0])
                        done = False
                    else:
                        del ents[i][0]
                elif(ent_end >= e[0][0]):
                    if ( ((not (ret[1][2][0] and ret[1][2][1]) ) and (e[0][2][0] and e[0][2][1]) )):
                        ent_start = e[0][0]
                        ent_end = e[0][1]
                        del ents[ret[0]][0]
                        ret = (i, e[0])
                        done = False
                    else:
                        del ents[i][0]

    if(ret):
        del ents[ret[0]][0]
        return tuple( list(ret[1]) + [ent_types[ret[0]]] )
    else:
        return (-1, -1, False, None, [], "")

def update_vocab(vocab, entity, id_counter, in_ent=False):
    eid = -1

    if(entity in vocab):
        vocab[entity][1] += 1 if(not in_ent) else 0
        eid = vocab[entity][0]
    else:
        eid = id_counter
        vocab[entity] = [id_counter, 1]
        id_counter += 1

    return (eid, id_counter)

def extract_candidatefreq(sent, docsrc, candidatefreq, ents_people, ents_place, ents_org):
    sent_comp = [str(sw) for sw in sent]
    for (startidx, endidx, certain, cid, rep_names) in ents_people:
        if(rep_names):
            for r in rep_names:
                sent_comp = sent_comp[:r[0]] + r[1] + sent_comp[r[0]+1:]
        w = " ".join(sent_comp[startidx:endidx+1])
        #print(w)

        wpat = "person" + ";" + docsrc + ";" + str(certain) + ";" + w
        if(cid in candidatefreq):
            candidatefreq[cid][0] += 1
            if(wpat in candidatefreq[cid][1]):
                candidatefreq[cid][1][wpat][0] += 1
            else:
                candidatefreq[cid][1][wpat] = [1, str(sent_comp)]
        else:
            candidatefreq[cid] = [1, {wpat : [1, str(sent_comp)]}]

    for (startidx, endidx, certain, cid, rep_names) in ents_place:
        w = " ".join(sent_comp[startidx:endidx+1])
        wpat = "location" + ";" + docsrc + ";" + str(certain) + ";" + w
        if(cid in candidatefreq):
            candidatefreq[cid][0] += 1
            if(wpat in candidatefreq[cid][1]):
                candidatefreq[cid][1][wpat][0] += 1
            else:
                candidatefreq[cid][1][wpat] = [1, str(sent_comp)]
        else:
            candidatefreq[cid] = [1, {wpat : [1, str(sent_comp)]}]
    for (startidx, endidx, certain, cid, rep_names) in ents_org:
        w = " ".join(sent_comp[startidx:endidx+1])
        wpat = "organization" + ";" + docsrc + ";" + str(certain) + ";" + w
        if(cid in candidatefreq):
            candidatefreq[cid][0] += 1
            if(wpat in candidatefreq[cid][1]):
                candidatefreq[cid][1][wpat][0] += 1
            else:
                candidatefreq[cid][1][wpat] = [1, str(sent_comp)]
        else:
            candidatefreq[cid] = [1, {wpat : [1, str(sent_comp)]}]

def add_lemma_vocab(rftt_probs, entity_vocab, id_counter):
    lemma = rftt_probs[3][:-2]
    if(lemma == "<unknown>"):
        lemma = rftt_probs[0]

    if("|" in lemma):
        lemmas = lemma.split("|")
        (ent_id, id_counter) = update_vocab(entity_vocab, lemmas[0].lower(), id_counter)
        for lemma_part in lemmas[1:]:
            (_, id_counter) = update_vocab(entity_vocab, lemma_part.lower(), id_counter)
    else:
        (ent_id, id_counter) = update_vocab(entity_vocab, lemma.lower(), id_counter)

    return (ent_id, id_counter)

def verticals_integrate_spacy(tok_sents, nlp_vert, entity_vocab, candidatefreq, id_counter, ents):
    out_file = None
    errs = 0
    docsrc = ""
    ent = None
    insert_words = []

    for (i, sent) in enumerate(nlp_vert.pipe(tok_sents, batch_size=1000, n_threads=8)):
        if( isinstance(sent.user_data, str) ):
            if(out_file):
                out_file.close()

            out_file = open(sent.user_data.split(":")[1], "w")
            docsrc = sent.user_data.split(":")[1].split("/")[-1].split("_")[0]
        else:
            if(sent):
                (rftt_sent, docid, ents_people, ents_place, ents_org) = sent.user_data
                print(docid, flush=True)

                extract_candidatefreq(sent, docsrc, candidatefreq, ents_people, ents_place, ents_org)

                noun_chunk_dict = dict([(n.i, i) for (i, ncs) in enumerate(sent.noun_chunks) for n in ncs])
                
                (cstart, cend, certain, candidate, rep_names, cent_type) = \
                    next_candidate( (ents_people, ents_place, ents_org), -1 )

                i = 0
                in_ent = False
                widx = 0
                idx_shift = 0
                for word in sent:
                    while(rftt_sent[i][0] == "<" and not rftt_sent[i][1] == "\t"):
                        out_file.write(rftt_sent[i])
                        i += 1

                    rftt_probs = rftt_sent[i][:-1].split("\t")
                    ent_type = word.ent_type_
                    ent_iob = word.ent_iob_
                    
                    if(ent_iob == "B"):
                        if(ent):
                            full_ent = " ".join(ent[1])
                            if(not full_ent in ents):
                                ents[full_ent] = {"PER": 0, "LOC": 0, "ORG": 0, "MISC": 0}
                            ents[full_ent][ent[0]] += 1
                        ent = (ent_type, [word.text])
                    elif(ent_iob == "I"):
                        ent[1].append(word.text)
                    else:
                        if(ent):
                            full_ent = " ".join(ent[1])
                            if(not full_ent in ents):
                                ents[full_ent] = {"PER": 0, "LOC": 0, "ORG": 0, "MISC": 0}
                            ents[full_ent][ent[0]] += 1
                        ent = None

                    #TODO: 
                    if(widx == cstart and ((certain[0] and certain[1]) or word.ent_type_ != "")): #entity found
                        (ent_id, id_counter) = update_vocab(entity_vocab, candidate, id_counter)
                        ent_type = cent_type#"PER"
                        ent_iob = "B"#Beginn
                        
                        in_ent = True
                    elif(widx > cstart and widx <= cend and in_ent): #in entity
                        (ent_id, id_counter) = update_vocab(entity_vocab, candidate, id_counter, in_ent=True)
                        ent_type = cent_type#"PER"
                        ent_iob = "I"#Intermediate
                    else:
                        (ent_id, id_counter) = add_lemma_vocab(rftt_probs, entity_vocab, id_counter)

                    verticals_entry = [rftt_probs[0], #original word
                            rftt_probs[1], #rf pos tag with morpholocical information
                            rftt_probs[2], #tt pos tag
                            rftt_probs[3], #lemma
                            str(word.i),
                            word.pos_,
                            word.tag_,
                            str(word.ent_type_),
                            str(word.ent_iob_),
                            str(ent_id),
                            ent_type,
                            ent_iob,
                            str(noun_chunk_dict[word.i]) if word.i in noun_chunk_dict else "",
                            word.dep_,
                            str(word.head.i) if word.head else ""]
                    
                    if(rep_names and rep_names[0][0] == widx):
                        insert_words = rep_names[0][1]
                        del rep_names[0]
                        
                    if(insert_words):
                        for iw in insert_words:
                            verticals_entry[0] = iw
                            verticals_entry[3] = str(word) + "-n"
                            out_file.write("\t".join(verticals_entry) + "\n")
                            widx += 1
                            i += 1
                            while(len(rftt_sent) > i and rftt_sent[i][0] == "<" and not rftt_sent[i][1] == "\t"):
                                out_file.write(rftt_sent[i])
                                i += 1
                        insert_words = []
                    else:
                        out_file.write("\t".join(verticals_entry) + "\n")
                        widx += 1
                        i += 1
                        
                    if(widx-1 == cend):
                        in_ent = False
                        (cstart, cend, certain, candidate, rep_names, cent_type) = \
                            next_candidate( (ents_people, ents_place, ents_org) , widx-1)

                while(len(rftt_sent) > i and rftt_sent[i][0] == "<" and not rftt_sent[i][1] == "\t"):
                    out_file.write(rftt_sent[i])
                    i += 1
                if(rftt_sent[i-1] != "</s>\n"):
                    errs += 1
                    print(docid)
                    print(rftt_sent)
                    print(ents_people[:])
                    print(ents_place[:])
                    print(ents_org[:])
                    print(len(rftt_sent), i, widx, len(sent), flush=True)
                    print()
                    print(sent)
                    break
            else:
                if(isinstance(sent.user_data, tuple)):
                    out_file.write("".join(sent.user_data[0]))
                else:
                    out_file.write("".join(sent.user_data))

    if(ent):
        full_ent = " ".join(ent[1])
        if(not full_ent in ents):
            ents[full_ent] = {"PER": 0, "LOC": 0, "ORG": 0, "MISC": 0}
            ents[full_ent][ent[0]] += 1    
    if(out_file and not out_file.closed):
        out_file.close()
    if(errs > 0):
        print("ERROR", errs)

    return id_counter
