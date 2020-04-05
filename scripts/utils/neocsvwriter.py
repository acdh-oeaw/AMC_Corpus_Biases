from os.path import join

from .verticals_svo_extraction import *
from .verticals_parser import is_goodword

def is_negative(word, negatives):
    word = word.lower()
    #using contains instead of "=" yields many false positives
    return word in negatives
def is_positive(word, positives):
    word = word.lower()
    return word in positives
def is_stopword(word, stopw):
    return word in stopw
    
def get_word(eid, entity_vocab, wikidata_vocab):
    pid = entity_vocab[int(eid)]
    in_wikidata = pid in wikidata_vocab
    if(in_wikidata):
        pid = wikidata_vocab[pid][0]
        if(not isinstance(pid, str)):
            pid = next(iter(pid))

    return pid.lower()
    
def write_ents(path, entity_vocab, wikidata_vocab, positives, negatives):
    ents_neutral = open(join(path, "entities_neutral.csv"), "w")
    ents_pos = open(join(path, "entities_pos.csv"), "w")
    ents_neg = open(join(path, "entities_neg.csv"), "w")

    for (pid, id_freq) in entity_vocab.items():
        in_wikidata = pid in wikidata_vocab
        if(in_wikidata):
            wid = pid.split("/entity/")[1]
            pid = wikidata_vocab[pid][0]
            if(not isinstance(pid, str)):
                pid = next(iter(pid))
        else:
            wid = ""
        pidl = pid.lower()
        #if (is_goodword(pidl) or in_wikidata):
            #sentiment = "0"
        gpid = pid.replace("\n", " ").replace(",", ";").replace("´", "'")
        if(is_positive(pidl, positives)):#[p for p in positives if p in pidl]):
                #sentiment = str(positives[pidl])
            ents_pos.write(str(id_freq[0]) + "," + gpid + "," + wid + "," + str(positives[pidl]) + "\n")
        elif(is_negative(pidl, negatives)):#[n for n in negatives if n in pidl]):#pidl in negatives):
                #sentiment = str(negatives[pidl])
            ents_neg.write(str(id_freq[0]) + "," + gpid + "," + wid + "," + str(float(negatives[pidl]) * -1.0) + "\n")
        else:
            ents_neutral.write(str(id_freq[0]) + "," + gpid + "," + wid + "\n")


            #ents.write(str(id_freq[0]) + "," + pid + "," + sentiment + "\n")
    ents_neutral.close()
    ents_pos.close()
    ents_neg.close()

class NeoCSVWriter():
    def __init__(self, path, open_w=False, suffix="", mode="w", complex_rel=False, vocab=None, stopw=None, emos=None, vals=None):
        self.complex_rel = complex_rel

        if(open_w):
            self.nodes_nouns = open(join(path, "nouns" + suffix + ".csv"), mode)
            self.nodes_adjectives = open(join(path, "adjectives" + suffix + ".csv"), mode)
            self.nodes_verbs = open(join(path, "verbs" + suffix + ".csv"), mode)
        #nodes_word.write("wid:ID(Word)\n")
        self.nodes_sentence = open(join(path, "sentence" + suffix + ".csv"), mode)
        #nodes_sentence.write("sid:ID(Sentence)\n")
        #self.nodes_paragraph = open(join(path, "paragraph" + suffix + ".csv"), mode)
        #nodes_paragraph.write("pid:ID(Paragraph),docid, date\n")
        self.nodes_document = open(join(path, "document" + suffix + ".csv"), mode)

        self.relation_noun_sentence =  open(join(path, "noun_sentence" + suffix + ".csv"), mode)
        self.relation_adjective_sentence =  open(join(path, "adjective_sentence" + suffix + ".csv"), mode)
        self.relation_verb_sentence =  open(join(path, "verb_sentence" + suffix + ".csv"), mode)
        
        if(vocab):
            (self.entity_vocab, self.wikidata_vocab) = vocab

            if(stopw):
                self.stopw = stopw
                self.relation_noun_sentence_stop =  open(join(path, "noun_sentence_stop" + suffix + ".csv"), mode)
                self.relation_adjective_sentence_stop =  open(join(path, "adjective_sentence_stop" + suffix + ".csv"), mode)
                self.relation_verb_sentence_stop =  open(join(path, "verb_sentence_stop" + suffix + ".csv"), mode)
                if(complex_rel):
                    self.relation_nvns_subj_stop = open(join(path, "nvns_subj_stop" + suffix + ".csv"), mode)
                    self.relation_nvns_obj_stop = open(join(path, "nvns_obj_stop" + suffix + ".csv"), mode)
                    self.relation_nvns_verb_stop = open(join(path, "nvns_verb_stop" + suffix + ".csv"), mode)
                    
                    self.relation_adverb_sentence_stop = open(join(path, "adverb_sentence_stop" + suffix + ".csv"), mode)

                    self.relation_ent_sentence_per_stop = open(join(path, "ent_sentence_per_stop" + suffix + ".csv"), mode)
                    self.relation_ent_sentence_loc_stop = open(join(path, "ent_sentence_loc_stop" + suffix + ".csv"), mode)
                    self.relation_ent_sentence_org_stop = open(join(path, "ent_sentence_org_stop" + suffix + ".csv"), mode)
                    self.relation_ent_sentence_misc_stop = open(join(path, "ent_sentence_misc_stop" + suffix + ".csv"), mode)

            if(emos):
                self.emos = emos#dict([(w, stype) for stype, w in emos.items()])
                self.relation_emo_ent_sentence = {}
                for etype in emos["__all__"]:
                    self.relation_emo_ent_sentence[etype] = open(join(path, etype.lower() + "_ent_sentence" + suffix + ".csv"), mode)
                
                #self.relation_emo_ent_sentence.update("other", open(join(path, "other_ent_sentence" + suffix + ".csv"), mode))
            else:
                self.emos = None

            if(vals):
                (self.pos, self.neg) = vals
                self.relation_pos_ent_sentence = open(join(path, "pos_ent_sentence" + suffix + ".csv"), mode)
                self.relation_neg_ent_sentence = open(join(path, "neg_ent_sentence" + suffix + ".csv"), mode)
                self.relation_neu_ent_sentence = open(join(path, "neu_ent_sentence" + suffix + ".csv"), mode)
            else:
                self.pos = None
                self.neg = None
            

        if(complex_rel):
            #self.relation_ans_adjective_noun_sentence = open(join(path, "ans_adjective_noun_sentence" + suffix + ".csv"), mode)
            #self.relation_ans_sentence = open(join(path, "ans_sentence" + suffix + ".csv"), mode)
            #self.relation_ans_noun = open(join(path, "ans_noun" + suffix + ".csv"), mode)
            #self.relation_ans_adjective = open(join(path, "ans_adjective" + suffix + ".csv"), mode)

            self.relation_nvns_noun_verb_noun_sentence = open(join(path, "nvns_noun_verb_noun_sentence" + suffix + ".csv"), mode)
            self.relation_nvns_subj = open(join(path, "nvns_subj" + suffix + ".csv"), mode)
            self.relation_nvns_obj = open(join(path, "nvns_obj" + suffix + ".csv"), mode)
            self.relation_nvns_verb = open(join(path, "nvns_verb" + suffix + ".csv"), mode)
            self.relation_nvns_sentence = open(join(path, "nvns_sentence" + suffix + ".csv"), mode)

            #self.relation_ent = open(join(path, "ent" + suffix + ".csv"), mode)
            #self.relation_ent_n = open(join(path, "ent_noun" + suffix + ".csv"), mode)
            #self.relation_ent_sentence = open(join(path, "ent_sentence" + suffix + ".csv"), mode)
            self.relation_ent_sentence_per = open(join(path, "ent_sentence_per" + suffix + ".csv"), mode)
            self.relation_ent_sentence_loc = open(join(path, "ent_sentence_loc" + suffix + ".csv"), mode)
            self.relation_ent_sentence_org = open(join(path, "ent_sentence_org" + suffix + ".csv"), mode)
            self.relation_ent_sentence_misc = open(join(path, "ent_sentence_misc" + suffix + ".csv"), mode)

            self.relation_nchunks = open(join(path, "nchunks" + suffix + ".csv"), mode)
            self.relation_nchunks_sentence = open(join(path, "nchunks_sentence" + suffix + ".csv"), mode)
            self.relation_nchunks_noun = open(join(path, "nchunks_noun" + suffix + ".csv"), mode)

            self.relation_adverb_sentence = open(join(path, "adverb_sentence" + suffix + ".csv"), mode)

        #relation_word_sentence.write(":START_ID(Word),:END_ID(Sentence), wtype\n")
        #self.relation_sentence_paragraph =  open(join(path, "sentence_paragraph" + suffix + ".csv"), mode)
        #relation_sentence_paragraph.write(":START_ID(Sentence),:END_ID(Paragraph)\n")
        #self.relation_paragraph_document =  open(join(path, "paragraph_document" + suffix + ".csv"), mode)
        self.relation_content_document =  open(join(path, "content_document" + suffix + ".csv"), mode)
        self.relation_keywords_document =  open(join(path, "keywords_document" + suffix + ".csv"), mode)
        self.relation_title_document =  open(join(path, "title_document" + suffix + ".csv"), mode)

    def write_doc(self, docid, date, docsrc, ressort, region, parser_params):
        self.nodes_document.write(str(docid) + "," + str(date) + "," + str(docsrc) + \
                                  "," + str(ressort.replace(",", ";")) + "," + str(region) + \
                                  "," + str(parser_params.replace(",", ";")) + "\n")

    #def write_p(self, pid, docid): #, date, autor, docsrc, region):
    #    self.nodes_paragraph.write(str(pid) + "\n") #"," + str(docid) + "," + str(date) + "," +
                                   #str(autor) + "," + str(docsrc) + "," + region + "\n")
    #    self.relation_paragraph_document.write(str(pid) + "," + str(docid) + "\n")
    
    
    def write_content(self, sid, docid): #, date, autor, docsrc, region):
        self.relation_content_document.write(str(sid) + "," + str(docid) + "\n")
    def write_keywords(self, sid, docid): #, date, autor, docsrc, region):
        self.relation_keywords_document.write(str(sid) + "," + str(docid) + "\n")
    def write_title(self, sid, docid): #, date, autor, docsrc, region):
        self.relation_title_document.write(str(sid) + "," + str(docid) + "\n")

    def write_s(self, sid, good_words, sent=""):
        if(sent):
            self.nodes_sentence.write(str(sid) + "," + sent.replace(",", ";").replace("\n", " ").replace("´", "'") + "\n")
        else:
            self.nodes_sentence.write(str(sid) + "\n")

        for (wid, eid, wtype) in good_words:
            mw = maxwtype(wtype)
            if(mw == 0):
                self.write_n_s(eid, sid)
            elif(mw == 1):
                self.write_a_s(eid, sid)
            elif(mw == 2):
                self.write_v_s(eid, sid)
            elif(mw == 3):
                self.write_av_s(eid, sid)

            #additional relationships
            if(self.emos):
                word = get_word(eid, self.entity_vocab, self.wikidata_vocab)
                if(word in self.emos):
                    etypes = self.emos[word]
                    for etype in etypes:
                        self.relation_emo_ent_sentence[etype].write(str(eid) + "," + str(sid) + "\n")
            if(self.pos):
                word = get_word(eid, self.entity_vocab, self.wikidata_vocab)
                if(is_positive(word, self.pos)):
                    self.relation_pos_ent_sentence.write(str(eid) + "," + str(sid) + "\n")
                elif(is_negative(word, self.neg)):
                    self.relation_neg_ent_sentence.write(str(eid) + "," + str(sid) + "\n")
                else:
                    self.relation_neu_ent_sentence.write(str(eid) + "," + str(sid) + "\n")
                    
    def write_s_old(self, sid, nouns, adjectives, verbs, adverbs=[], sent=""):
        if(sent):
            self.nodes_sentence.write(str(sid) + "," + sent + "\n")
        else:
            self.nodes_sentence.write(str(sid) + "\n")

        for wid in nouns:
            self.write_n_s(wid, sid)
        for wid in adjectives:
            self.write_a_s(wid, sid)
        for wid in verbs:
            self.write_v_s(wid, sid)
        for wid in adverbs:
            self.write_av_s(wid, sid)

    def write_n_s(self, wid, sid):
        if(self.stopw and is_stopword(get_word(wid, self.entity_vocab, self.wikidata_vocab), self.stopw)):
            self.relation_noun_sentence_stop.write(str(wid) + "," + str(sid) + "\n")
        else:
            self.relation_noun_sentence.write(str(wid) + "," + str(sid) + "\n")
    def write_a_s(self, wid, sid):
        if(self.stopw and is_stopword(get_word(wid, self.entity_vocab, self.wikidata_vocab), self.stopw)):
            self.relation_adjective_sentence_stop.write(str(wid) + "," + str(sid) + "\n")
        else:
            self.relation_adjective_sentence.write(str(wid) + "," + str(sid) + "\n")
    def write_v_s(self, wid, sid):
        if(self.stopw and is_stopword(get_word(wid, self.entity_vocab, self.wikidata_vocab), self.stopw)):
            self.relation_verb_sentence_stop.write(str(wid) + "," + str(sid) + "\n")
        else:
            self.relation_verb_sentence.write(str(wid) + "," + str(sid) + "\n")
    def write_av_s(self, wid, sid):
        if(self.stopw and is_stopword(get_word(wid, self.entity_vocab, self.wikidata_vocab), self.stopw)):
            self.relation_adverb_sentence_stop.write(str(wid) + "," + str(sid) + "\n")
        else:
            self.relation_adverb_sentence.write(str(wid) + "," + str(sid) + "\n")


    def write_n(self, wid):
        self.nodes_nouns.write(str(wid) + "\n")
    def write_a(self, wid):
        self.nodes_adjectives.write(str(wid) + "\n")
    def write_v(self, wid):
        self.nodes_verbs.write(str(wid) + "\n")

    def write_ents(self, sid,  ents):
        for ent in ents:
            self.write_ent(sid, ent[2], ent[1], ent[0])
    def write_ent(self, sid, words, etype, skey=""):
        if(not skey):
            skey = str("-".join(words)) + "-" + str(sid)
        #self.relation_ent.write(skey + "," + str(etype) + "\n")
        if(etype == "PER" or etype == "PERSON"):
            if(self.stopw and is_stopword(get_word(skey, self.entity_vocab, self.wikidata_vocab), self.stopw)):
                self.relation_ent_sentence_per_stop.write(skey + "," + str(sid) + "\n")
            else:
                self.relation_ent_sentence_per.write(skey + "," + str(sid) + "\n")
        elif(etype == "LOC"):
            if(self.stopw and is_stopword(get_word(skey, self.entity_vocab, self.wikidata_vocab), self.stopw)):
                self.relation_ent_sentence_loc_stop.write(skey + "," + str(sid) + "\n")
            else:
                self.relation_ent_sentence_loc.write(skey + "," + str(sid) + "\n")
        elif(etype == "ORG"):
            if(self.stopw and is_stopword(get_word(skey, self.entity_vocab, self.wikidata_vocab), self.stopw)):
                self.relation_ent_sentence_org_stop.write(skey + "," + str(sid) + "\n")
            else:
                self.relation_ent_sentence_org.write(skey + "," + str(sid) + "\n")
        elif(etype == "MISC"):
            if(self.stopw and is_stopword(get_word(skey, self.entity_vocab, self.wikidata_vocab), self.stopw)):
                self.relation_ent_sentence_misc_stop.write(skey + "," + str(sid) + "\n")
            else:
                self.relation_ent_sentence_misc.write(skey + "," + str(sid) + "\n")
        #for n in words:
        #    self.relation_ent_n.write(skey + "," + str(n) + "\n")

    def write_nchunks(self, sid, nchunks):
        for chunk in nchunks:
            skey = chunk[0] + "-" + str(sid)
            self.relation_nchunks.write(skey + "\n")
            self.relation_nchunks_sentence.write(skey + "," + str(sid) + "\n")
            for nch in chunk[1]:
                self.relation_nchunks_noun.write(skey + "," + str(nch) + "\n")

    def write_a_n_s(self, awid, nwid, sid):
        skey = str(awid) + "-" + str(nwid) + "-" + str(sid)
        self.relation_ans_adjective_noun_sentence.write(skey + "\n")
        self.relation_ans_sentence.write(skey + "," + str(sid) + "\n")
        self.relation_ans_noun.write(skey + "," + str(nwid) + "\n")
        self.relation_ans_adjective.write(skey + "," + str(awid) + "\n")

    def write_svo(self, sid, triplets):
        for (subj, verb, obj) in set(triplets):
            self.write_n_v_n_s(subj, obj, verb, sid)
    def write_n_v_n_s(self, nwid1, nwid2, vwid, sid):
        skey = str(nwid1) + "-" + str(nwid2) + "-" + str(vwid) + "-" + str(sid)
        self.relation_nvns_noun_verb_noun_sentence.write(skey + "\n")
        
        if(self.stopw and is_stopword(get_word(nwid1, self.entity_vocab, self.wikidata_vocab), self.stopw)):
            self.relation_nvns_subj_stop.write(skey + "," + str(nwid1) + "\n")
        else:
            self.relation_nvns_subj.write(skey + "," + str(nwid1) + "\n")
        if(self.stopw and is_stopword(get_word(nwid1, self.entity_vocab, self.wikidata_vocab), self.stopw)):
            self.relation_nvns_verb_stop.write(skey + "," + str(vwid) + "\n")
        else:
            self.relation_nvns_verb.write(skey + "," + str(vwid) + "\n")
        if(self.stopw and is_stopword(get_word(nwid1, self.entity_vocab, self.wikidata_vocab), self.stopw)):
            self.relation_nvns_obj_stop.write(skey + "," + str(nwid2) + "\n")
        else:
            self.relation_nvns_obj.write(skey + "," + str(nwid2) + "\n")

        self.relation_nvns_sentence.write(skey + "," + str(sid) + "\n")

    #def write_s_p(self, sid, pid):
    #    self.relation_sentence_paragraph.write(str(sid) + "," + str(pid) + "\n")

    def close(self, close_w=False):
        self.nodes_sentence.close()
        self.nodes_document.close()
        #self.nodes_paragraph.close()
        self.relation_noun_sentence.close()
        self.relation_adjective_sentence.close()
        self.relation_verb_sentence.close()
        
        
        if(self.emos):
            for fre in self.relation_emo_ent_sentence.values():
                fre.close()
                
        if(self.stopw):
            self.relation_noun_sentence_stop.close()
            self.relation_adjective_sentence_stop.close()
            self.relation_verb_sentence_stop.close()
            
            if(self.complex_rel):
                self.relation_nvns_subj_stop.close()
                self.relation_nvns_obj_stop.close()
                self.relation_nvns_verb_stop.close()
                
                self.relation_adverb_sentence_stop.close()

                self.relation_ent_sentence_per_stop.close()
                self.relation_ent_sentence_loc_stop.close()
                self.relation_ent_sentence_org_stop.close()
                self.relation_ent_sentence_misc_stop.close()
        
        if(self.pos):
            self.relation_pos_ent_sentence.close()
            self.relation_neg_ent_sentence.close()
            self.relation_neu_ent_sentence.close()

        if(self.complex_rel):
            #self.relation_ans_adjective_noun_sentence.close()
            #self.relation_ans_sentence.close()
            #self.relation_ans_noun.close()
            #self.relation_ans_adjective.close()

            self.relation_nvns_noun_verb_noun_sentence.close()
            self.relation_nvns_subj.close()
            self.relation_nvns_obj.close()
            self.relation_nvns_verb.close()
            self.relation_nvns_sentence.close()

            #self.relation_ent.close()
            #self.relation_ent_n.close()
            self.relation_ent_sentence_per.close()
            self.relation_ent_sentence_loc.close()
            self.relation_ent_sentence_org.close()
            self.relation_ent_sentence_misc.close()
            
            self.relation_nchunks.close()
            self.relation_nchunks_sentence.close()
            self.relation_nchunks_noun.close()
            
            self.relation_adverb_sentence.close()

        #self.relation_sentence_paragraph.close()
        self.relation_content_document =  open(join(path, "content_document" + suffix + ".csv"), mode)
        self.relation_keywords_document =  open(join(path, "keywords_document" + suffix + ".csv"), mode)
        self.relation_title_document =  open(join(path, "title_document" + suffix + ".csv"), mode)
        self.nodes_sentence.close()

        if(close_w):
            self.nodes_nouns.close()
            self.nodes_adjectives.close()
            self.nodes_verbs.close()
