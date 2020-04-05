SUBJ_DEPS = {"sb", "ph", "sbp", "sp", "ep"}#EN: {'agent', 'csubj', 'csubjpass', 'expl', 'nsubj', 'nsubjpass'}
ATTR = {"nmc", "mo", "nk", "mnr"}

ATTR_TAGS = {"nk": "ADJA", "mo": "ADJA", "": "NUM", "mo": "TRUNC"}
OBJD_TAGS = {"mo": "PWS"}

OBJ_DEPS = ATTR | {"oa", "da", "oa2", "op", "og", "oc"}#EN: {'attr', 'dobj', 'dative', 'oprd'}
AUX_DEPS = {"ng"}#EN: {'aux', 'auxpass', 'neg'}
NOT_POS_AUX = {"AUX"}

from spacy.parts_of_speech import NOUN, PROPN, VERB

class WordTree():
    def __init__(self, words):
        self.words = words
        self.tree = [(i, w[6]) for (i, w) in enumerate(words)]

    def word(self, i):
        return self.words[i][8]
        #return self.words[i][0]
    def ent_id(self, i):
        return self.words[i][1]
    def pos(self, i):
        return strwtype(self.words[i][7])
        #return self.words[i][2]
    def tag(self, i):
        return self.words[i][3]
    def dep(self, i):
        return self.words[i][4]
    def good_word(self, i):
        return self.words[i][5]
    def children(self, i):
        return [nid for nid, head in self.tree if head == i]

    def rights(self, i):
        return [nid for nid, head in self.tree if head == i and nid > i]

    def lefts(self, i):
        return [nid for nid, head in self.tree if head == i and nid < i]

    def length(self):
        return len(self.words)

def maxwtype(wtype):
    ret = 0
    for i in range(len(wtype)):
            if(wtype[i] > wtype[ret]):
                ret = i
    return ret

def strwtype(wtype):
    mw = maxwtype(wtype)
    if(mw == 0):
        return "NOUN"
    elif(mw == 1):
        return "ADJ"
    elif(mw == 2):
        return "VERB"
    elif(mw == 3):
        return "ADV"
    elif(mw == 4):
        return "DET"
    elif(mw == 5):
        return "FM"
    elif(mw == 6):
        return "TRUNC"
    elif(mw == 7):
        return "PTKNEG"
    else:
        return "OTHER"

def _get_conjuncts(sent, i):
    """
    Return conjunct dependents of the leftmost conjunct in a coordinated phrase,
    e.g. "Burton, [Dan], and [Josh] ...".
    """
    return [right for right in sent.rights(i)
            if sent.dep(right) == 'cj' and sent.good_word(i)]

def get_main_verbs_of_sent(sent):
    """Return the main (non-auxiliary) verbs in a sentence."""
    return [i for i in range(sent.length()) if sent.pos(i) == "VERB" and sent.good_word(i)]# \
            #and if not [ch for ch in tok.children if ch.pos == "VERB"]] #TODO: no auxilary verbs??
    #EN: and tok.dep_ not in {'aux', 'auxpass'}]


def get_subjects_of_verb(sent, verb):
    """Return all subjects of a verb according to the dependency parse."""
    #TODO: sollte .children vlt. eher rekursiv sein
    subjs = [i for i in sent.children(verb) #Subjekt kann vorn und hinten sein  #EN: .lefts
             if sent.dep(i) in SUBJ_DEPS and sent.pos(i) == "NOUN" and sent.good_word(i)]

    # get additional conjunct subjects
    subjs.extend(tok for i in subjs for tok in _get_conjuncts(sent, i) if sent.pos(tok) == "NOUN")
    return subjs


def get_objects_of_verb(sent, verb):
    """
    Return all objects of a verb according to the dependency parse,
    including open clausal complements.
    """
    #TODO: sollte .children vlt. eher rekursiv sein
    objs = [i for i in sent.children(verb) #Subjekt kann vorn und hinten sein  #EN:.rights
            if sent.dep(i) in OBJ_DEPS and sent.pos(i) == "NOUN" and sent.good_word(i)]

    #TODO: find equivalent in german
    # get open clausal complements (xcomp)
    #objs.extend(tok for tok in verb.rights
    #            if tok.dep_ == 'xcomp')

    # get additional conjunct objects
    objs.extend(tok for i in objs for tok in _get_conjuncts(sent, i) if sent.pos(tok) == "NOUN")
    return objs

def svo_triples(sent):
    # TODO: What to do about questions, where it may be VSO instead of SVO?
    # TODO: What about non-adjacent verb negations?
    # TODO: What about object (noun) negations?

    #start_i = sent[0].i
    ret = []
    verbidx = get_main_verbs_of_sent(sent)
    #print([sent.word(i) for i in verbidx])
    for verb in verbidx:
        subjs = get_subjects_of_verb(sent, verb)
        if not subjs:
            continue
        objs = get_objects_of_verb(sent, verb)
        if not objs:
            continue
        #print( ([sent.word(subj) for subj in subjs], sent.word(verb), [sent.word(obj) for obj in objs]) )

        #TODO: compounds, auxiliaries, .. missing
        for subj in subjs:
            for obj in objs:
                ret.append( (sent.ent_id(subj), sent.ent_id(verb), sent.ent_id(obj)) )
    return ret
"""
        # add adjacent auxiliaries to verbs, for context
        # and add compounds to compound nouns
        verb_span = get_span_for_verb_auxiliaries(verb)
        verb = sent[verb_span[0] - start_i: verb_span[1] - start_i + 1]
        for subj in subjs:
            subj = sent[get_span_for_compound_noun(subj)[0] - start_i: subj.i - start_i + 1]
            for obj in objs:
                if obj.pos == NOUN:
                    span = get_span_for_compound_noun(obj)
                elif obj.pos == VERB:
                    span = get_span_for_verb_auxiliaries(obj)
                else:
                    span = (obj.i, obj.i)
                obj = sent[span[0] - start_i: span[1] - start_i + 1]

                yield (subj, verb, obj)



                    found = []
                    add = []
                    has_so = True
                    for ch in w.children:
                        if(ch.dep_ in ["sb", "mo"] and ch.pos in [NOUN, PROPN] and \
                           len(ch.lemma_) > 1 and ch.lemma_.replace("-", "").isalpha()):
                            found.insert(0, ch.lemma_)
                        elif(ch.dep_ == "oa" and ch.pos in [NOUN, PROPN] and \
                             len(ch.lemma_) > 1 and ch.lemma_.replace("-", "").isalpha()):
                            found.append(ch.lemma_)
                        else:
                            has_so = False
                            st = list(ch.children)
                            nfound = False
                            while(len(st) > 0 and not nfound):
                                c = st.pop(0)
                                if(c.pos in [NOUN, PROPN] and len(c.lemma_) > 1 and c.lemma_.replace("-", "").isalpha()):
                                    nfound = True
                                    found.append(c.lemma_)
                                else:
                                    st.extend(c.children)
                    if(len(found) > 1):
                        csvwriter.write_n_v_n_s(str(found[0]), str(found[1]), w.lemma_, dmeta[2] + "_" + str(i) + "_" + str(sid))
"""

def is_negated_verb(token):
    """
    Returns True if verb is negated by one of its (dependency parse) children,
    False otherwise.
    Returns:
        bool
    TODO: generalize to other parts of speech; rule-based is pretty lacking,
    so will probably require training a model; this is an unsolved research problem
    """
    if any(c.dep == 'ng' for c in token.children):
        return True
    return False

def get_span_for_compound_noun(noun):
    """
    Return document indexes spanning all (adjacent) tokens
    in a compound noun.
    """
    min_i = noun.i - sum(1 for _ in takewhile(lambda x: x.dep_ == 'compound',
                                              reversed(list(noun.lefts))))
    return (min_i, noun.i)


def get_span_for_verb_auxiliaries(verb):
    """
    Return document indexes spanning all (adjacent) tokens
    around a verb that are auxiliary verbs or negations.
    """
    min_i = verb.i - sum(1 for _ in takewhile(lambda x: x.dep_ in AUX_DEPS,
                                              reversed(list(verb.lefts))))
    max_i = verb.i + sum(1 for _ in takewhile(lambda x: x.dep_ in AUX_DEPS,
                                              verb.rights))
    return (min_i, max_i)
