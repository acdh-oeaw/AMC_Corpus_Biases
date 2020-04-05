import pickle
from datetime import datetime
from itertools import chain

def add_entry(lookup_table, pid, names):
    for name in names:
        toks = name.split()
        for (i, tok) in enumerate(toks):
            if(not tok.isupper()):
                tok = tok.lower()
                
            if (tok in lookup_table):
                lookup_table[tok].add((pid,len(toks),i+1))
            else:
                lookup_table[tok] = set([(pid,len(toks),i+1)])

def load_vocabulary(vpath, orgpath):
    vocab = pickle.load( open(vpath, "rb") )
    (vocab_place, vocab_org) = pickle.load( open(orgpath, "rb") )

    lookup_place = dict()
    for (k, v) in vocab_place.items():
        add_entry(lookup_place, k, v[0] | v[1])
    lookup_org = dict()
    for (k, v) in vocab_org.items():
        add_entry(lookup_org, k, v[0] | v[1])

    matcher_data = [(pid, (probs[0].strip()[0] + ".",set([probs[0]])|probs[5]|probs[6], probs[3], probs[4]))
                    for (pid, probs) in vocab.items()]
                        #if not probs[9] or (probs[9] and next(iter(probs[9])) > datetime(1920, 1, 1))]

    lookup_fname = dict()
    lookup_lname = dict()
    for (pid, (abbreviation, fullnames, firstnames, lastnames)) in matcher_data:
        (extr_fnames, extr_lnames) = ([], [])
        for name in fullnames:
            toks = name.split()
            if(len(toks) == 2):
                extr_fnames.append(toks[0])
                extr_lnames.append(toks[1])
            else:
                fntoks = []
                lntoks = []
                fidx = 0
                if(firstnames and [tok for tok in toks if tok in firstnames]):
                    for i, tok in enumerate(toks):
                        if (tok in firstnames):
                            #fntoks.append(tok)
                            fidx = i
                        #else:
                            #lntoks.append(tok)
                    fidx += 1
                    extr_fnames.append(" ".join(toks[:fidx]))
                    extr_lnames.append(" ".join(toks[fidx:]))
                    #extr_fnames.append(" ".join(fntoks))
                    #extr_lnames.append(" ".join(lntoks))
                elif(lastnames and [tok for tok in toks if tok in lastnames]):
                    is_fidx_set = False
                    for i, tok in enumerate(toks):
                        if (tok in lastnames and not is_fidx_set):
                            fidx = i
                            is_fidx_set = True
                        #if (not tok in lastnames):
                            #fntoks.append(tok)
                        #else:
                            #lntoks.append(tok)
                    fidx += 1
                    extr_fnames.append(" ".join(toks[:fidx]))
                    extr_lnames.append(" ".join(toks[fidx:]))
                    #extr_fnames.append(" ".join(fntoks))
                    #extr_lnames.append(" ".join(lntoks))
                else:
                    extr_fnames.extend(" ".join(toks[:-1]))
                    extr_lnames.append(toks[-1])
        firstnames = set([abbreviation] + \
                         [fn for fn in firstnames | set(extr_fnames) \
                             if len(fn) > 1 and fn.replace("-","").replace(" ", "").isalpha()])
        add_entry(lookup_fname, pid, firstnames)
        lastnames = set([ln for ln in lastnames | set(extr_lnames) \
                             if len(ln) > 1 and ln.replace("-","").replace(" ", "").isalpha()])

        #special case with s at the end
        lastnames = lastnames | set([ln + "s" for ln in lastnames])

        add_entry(lookup_lname, pid, lastnames)


    vocab = dict(list(vocab.items()) + list(vocab_place.items()) + list(vocab_org.items()))
    
    ie_vocab = (vocab, lookup_fname, lookup_lname, lookup_place, lookup_org)

    return ie_vocab

def update_candidate_list(clist, cid, start, clen, org_sent):
    #if(org_sent[start][0].isupper()):
    if(cid in clist):
        old_clen = clist[cid][1]
        if(old_clen < clen): #if same entity detected multiple times choose biggest
            clist[cid] = (start, clen) 
    else:
        clist[cid] = (start, clen)

def extract_candidates(candidate_names, org_sent, start_idx_block):
    candidate_entities = {}
    partial_names = {}

    for (idx, cnames) in enumerate(candidate_names):
        for (cid, plen, pos) in cnames:
            if(pos == 1):
                if(plen == 1):
                    update_candidate_list(candidate_entities, cid, start_idx_block+idx, 1, org_sent)
                else:
                    if(cid in partial_names):
                        partial_names[cid].append((plen, pos, idx))
                    else:
                        partial_names[cid] = [(plen, pos, idx)]
            else:                
                if(cid in partial_names):
                    for pn in partial_names[cid]:
                        (curlen, curpos, start) = pn
                        if(curpos == pos-1 and curlen == plen): #next part of entity belongs to entity
                            partial_names[cid].remove(pn)
                            if(plen == pos): #all partial entities found
                                if(idx - start == plen - 1): #check if words were consecutive
                                    update_candidate_list(candidate_entities, cid, start_idx_block+start, plen, org_sent)
                            else:
                                partial_names[cid].append((curlen, pos, start))
                    

    return candidate_entities

def rank_candidates(names_it, vocab):
    #mechanisms to rank candidates

    #get list of longest match in detected entity
    max_clen = -1
    max_ents = []
    for (cid, (start, clen, cnames)) in names_it:
        if(max_clen < clen):
            max_clen = clen
            max_ents = [(cid, start, clen, cnames)]
        elif(max_clen == clen):
            max_ents.append((cid, start, clen, cnames))
    
    #most reporting on politicians -> select politicians if possible
    return sorted(max_ents, reverse=True, key=lambda ent: sum(vocab[ent[0]][-1]))

def choose_candidate(start_idx, candiates_fname, candiates_lname, vocab, org_sent, found_candidates):
    dict_cfn = extract_candidates(candiates_fname, org_sent, start_idx)
    dict_cln = extract_candidates(candiates_lname, org_sent, start_idx)

    joined_names = {}
    maybe_names = {}
    
    for (cid, (start, clen)) in dict_cln.items():
        if (cid in dict_cfn):
            if(dict_cfn[cid][0]+dict_cfn[cid][1] == start):
                clname = [] if clen <= 1 else [ [start, start + (clen-1)] ]
                cfname = [] if dict_cfn[cid][1] <= 1 else [ [dict_cfn[cid][0], dict_cfn[cid][0] + (dict_cfn[cid][1]-1)] ]
                joined_names[cid] = (dict_cfn[cid][0], dict_cfn[cid][1]+clen, cfname + clname)
            elif(cid in found_candidates):
                joined_names[cid] = (start, clen, [] if clen <= 1 else [ [start, start + (clen-1)] ])
            else:
                maybe_names[cid] = (start, clen, [])
        elif(cid in found_candidates):
            joined_names[cid] = (start, clen, [] if clen <= 1 else [ [start, start + (clen-1)] ])
        else:
            maybe_names[cid] = (start, clen, [])

    if(joined_names):
        #TODO: (True, org_sent[start][0].isupper())
        (cid, start, clen, cnames) = rank_candidates(joined_names.items(), vocab)[0]
        return [(start, start + (clen-1), (True, org_sent[start][0].isupper()), cid, cnames)]

    elif(maybe_names):
        #TODO: (False, org_sent[start][0].isupper())
        return [ (-1, -1, (False, org_sent[start][0].isupper()), rank_candidates(maybe_names.items(), vocab), []) ]
    else:
        return []

def find_candidates(ie_vocab, found_candidates, s):
    (vocab, lookup_fname, lookup_lname, lookup_place, lookup_org) = ie_vocab

    candidates_fname = []
    candidates_lname = []
    candidates_place = []
    candidates_org= []
    pidset_fname = set()
    pidset_lname = set()
    
    in_entity = False
    entity_start_idx = -1
    esi_place = -1
    esi_org = -1

    ents_people = []
    ents_place = []
    ents_org = []

    for (i, w) in enumerate([ (w.lower() if (not w.isupper()) else w) for w in s]):
        if(w in lookup_fname and (not in_entity or (pidset_fname & set([x[0] for x in lookup_fname[w]]))) ):
            if(in_entity):
                if(w not in lookup_lname):
                    ents_people.extend(choose_candidate(entity_start_idx, candidates_fname, \
                        candidates_lname, vocab, s, found_candidates))

                    #NO entity found: firstnames are placed in front of lastnames
                    candidates_fname = [lookup_fname[w]]
                    candidates_lname = [[]]
                    
                    pidset_fname = set([x[0] for x in lookup_fname[w]])
                    pidset_lname = set()

                    entity_start_idx = i
                    in_entity = False
                    continue
                else:
                    candidates_fname.append(lookup_fname[w])
                    if(pidset_fname):
                        pidset_fname &= set([x[0] for x in lookup_fname[w]])
                    else:
                        pidset_fname = set([x[0] for x in lookup_fname[w]])
            else:
                if(entity_start_idx == -1):
                    entity_start_idx = i
                    candidates_fname = []
                    candidates_lname = []
                    pidset_fname = set([x[0] for x in lookup_fname[w]])
                    pidset_lname = set()
                candidates_fname.append(lookup_fname[w])
        else:
            candidates_fname.append([])

        if(w in lookup_lname):
            if(not (in_entity and not ( (pidset_fname|pidset_lname) & set([x[0] for x in lookup_lname[w]]) ) )):
                if(not in_entity):
                    in_entity = True
                    if(entity_start_idx == -1):
                        entity_start_idx = i
                        candidates_fname = [[]]
                        candidates_lname = []
                if(pidset_lname):
                    pidset_lname &= set([x[0] for x in lookup_lname[w]])
                else:
                    pidset_lname = set([x[0] for x in lookup_lname[w]])
                candidates_lname.append(lookup_lname[w])
            else:
                ents_people.extend(choose_candidate(entity_start_idx, candidates_fname, \
                    candidates_lname, vocab, s, found_candidates))
                if(w in lookup_fname):
                    candidates_fname = [lookup_fname[w]]
                    pidset_fname = set([x[0] for x in lookup_fname[w]])
                else:
                    candidates_fname = []
                    pidset_fname = set()
                candidates_lname = [lookup_lname[w]]
                pidset_lname = set([x[0] for x in lookup_lname[w]])
                entity_start_idx = i
        else:
            candidates_lname.append([])
            (in_entity, candidate_added) = store_person(in_entity, ents_people, entity_start_idx, candidates_fname, \
                                     candidates_lname, vocab, s, found_candidates)
            if(entity_start_idx != -1 and (not w in lookup_fname or candidate_added) ):
                candidates_fname = []
                candidates_lname = []
                pidset_fname = set()
                pidset_lname = set()
                entity_start_idx = -1
                
        (esi_place, candidates_place) = lookup_entity(w, s, i, ents_place, vocab, lookup_place, esi_place, candidates_place)
        (esi_org, candidates_org) = lookup_entity(w, s, i, ents_org, vocab, lookup_org, esi_org, candidates_org)

    store_person(in_entity, ents_people, entity_start_idx, candidates_fname, candidates_lname, vocab, s, found_candidates)
    store_entitiy(s, ents_place, vocab, esi_place, candidates_place)
    store_entitiy(s, ents_org, vocab, esi_org, candidates_org)

    return (ents_people, ents_place, ents_org)

def store_person(in_entity, ents_people, entity_start_idx, candidates_fname, candidates_lname, vocab, s, found_candidates):
    candidate_added = None
    if(in_entity):
        #print(pidset_lname, entity_start_idx, len(candidates_lname) )
        candidate_added = choose_candidate(entity_start_idx, candidates_fname, \
            candidates_lname, vocab, s, found_candidates)
        ents_people.extend(candidate_added)
        in_entity = False

    return (in_entity, candidate_added)

def lookup_entity(w, s, i, ents, vocab, lookup_ent, esi, candidates):
    if(w in lookup_ent):
        if(esi == -1):
            esi = i
            candidates = []
        candidates.append(lookup_ent[w])
        return (esi, candidates)
    else:
        candidates.append([])
        return store_entitiy(s, ents, vocab, esi, candidates)

def store_entitiy(s, ents, vocab, esi, candidates):
    if(esi > -1):
        cand_names = extract_candidates(candidates, s, esi)
        if(cand_names):
            cand_names = [(cid, (start, clen, [] if clen <= 1 else [ [start, start + (clen-1)] ]) ) \
                          for (cid, (start, clen)) in cand_names.items()]
            (cid, start, clen, cnames) = rank_candidates(cand_names, vocab)[0]
            #TODO: (True, True)
            ents.append( (start, start + (clen-1), (True, True), cid, []) )#cnames) )
        candidates = [[]]
        esi = -1
    
    return (esi, candidates)
    