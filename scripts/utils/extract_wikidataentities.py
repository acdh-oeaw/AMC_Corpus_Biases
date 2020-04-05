import sys
from bs4 import BeautifulSoup as bs
import io
from .ner_wikidata import *
        
def init_verticalfiles(pf, sents):
    (path, text) = pf

    # check: if newpath == path then exit! We don't want to overwrite our input!
    newpath = path.replace("rftt", "rfttsp").replace("203_", "208_")

    if newpath == path:
        print("Input file", path, "would be overwritten! Terminated process!") 
        sys.exit()
    else:
        path = newpath

    sents.append(([], [], str(path)))
    return io.StringIO(text)

def find_entitiy(candidates, found_candidates):
    for (cid, start, clen, cnames) in candidates:
        if(cid in found_candidates):
            return (start, start + (clen-1), True, cid, [] if clen <= 1 else [ [start, start + (clen-1)] ])
    (cid, start, clen, cnames) = candidates[0]
    return (start, start + (clen-1), False, cid, [])

def add_doc_sents_political(doc_sents, sents, found_candidates, politicians_at, docid, docprev, docprev_idx):
    #an article is considered political if an austrian politician is mentioned
    political = politicians_at & found_candidates
                    
    if(docid):
        docline = docprev[docprev_idx]
        docline = docline[:-2] + ' parser_params="'
        if(political):
            docline += "politik"
        docline += '" >\n'
        docprev[docprev_idx] = docline

    if(doc_sents):
        add_doc_sents(doc_sents, sents, found_candidates, political, politicians_at)

def add_doc_sents(doc_sents, sents, found_candidates, political=False, politicians_at=[]):
 
    for (dsent_words, dsent_spaces, pars) in doc_sents:
        rep_offset = 0
        if(dsent_words or dsent_spaces):
            (dprev, ddocid, (ents_people, ents_place, ents_org)) = pars
            new_candidates = []
            for (startidx, endidx, certain, candidates, rep_cnames) in ents_people:
                c = None
                #TODO: certain[0]
                if(not certain[0]):
                    (si, ei, cert, c, rc) = \
                        find_entitiy(candidates, found_candidates)
                    certain = (cert, certain[1])
                #TODO: not certain[0] and political
                if(not certain[0] and political):
                    (si, ei, cert, c, rc) = \
                        find_entitiy(candidates, politicians_at)
                    certain = (cert, certain[1])

                if(c):
                    candidates = c
                    rep_cnames = rc
                    endidx = ei
                    startidx = si

                for rep_cname in rep_cnames:
                    rep_start = rep_cname[0] - rep_offset
                    rep_end = (rep_cname[1] + 1) - rep_offset
                    rep_offset += (rep_end - rep_start) - 1
                 
                    org_names = dsent_words[rep_start:rep_end]
                    joined_names = "".join(org_names)
                    
                    if(joined_names):
                        del dsent_words[rep_start:rep_end]
                        del dsent_spaces[rep_start:(rep_end-1)]
                        dsent_words.insert(rep_start, joined_names[0].upper() + joined_names[1:].lower())

                        del rep_cname[1]
                        rep_cname.append(org_names)
                    else:
                        rep_cnames = []

                new_candidates.append( (startidx, endidx, certain, candidates, rep_cnames) )

            pars = (dprev, ddocid, new_candidates, ents_place, ents_org)

        sents.append( (dsent_words, dsent_spaces, pars) )

def extract_tok_sents(splitIndex, it, ie_vocab_bc, opath, file_name):
    sents = []
    found_candidates = set()
    ie_vocab = ie_vocab_bc.value

    politicians_at = set([pid for (pid, dat) in ie_vocab[0].items() if "Politiker" in dat[1] ])

    for pf in it:
        lines = init_verticalfiles(pf, sents)
        
        prev = []
        sent_words = []
        sent_spaces = []
        doc_sents = []
        in_sent = False

        docid = ""
        docprev = None
        docprev_idx = None
        
        #test_out = []
        
        for line in lines:
            prev.append(line)
            if(not in_sent):
                if(line[0:3] == "<s>"):
                    doc_sents.append(([], [], prev))
                    prev = []
                    in_sent = True
                #reset coreference resolution
                elif(line[1:5] == "doc "):
                    tag = bs(line, "lxml")

                    add_doc_sents_political(doc_sents, sents, found_candidates, politicians_at, docid, docprev, docprev_idx)
                    
                    doc_sents = []
                    found_candidates = set()

                    docprev = prev
                    docprev_idx = len(prev) - 1
                    docid = tag.doc["id"]
            else:
                if(line[0] == "<"):
                    #no space between words
                    if(line[1:4] == "g/>" or line[1:4] == "/g>"):
                        if(len(sent_spaces) == 0):
                            #print("ERROR in " + path + ": s should not contain " + line + " at start")
                            #print(prev)
                            pass
                        else:
                            sent_spaces[-1] = False
                    #end of sentence
                    elif(line[1:4] == "/s>"):
                        candidates = find_candidates(ie_vocab, found_candidates, sent_words)
                        #TODO: certain[0]
                        found_candidates = found_candidates | \
                            set([cid for (_,_,certain, cid, _) in candidates[0] if certain[0]])

                        doc_sents.append((sent_words, sent_spaces, (prev, docid, candidates)))

                        prev = []
                        sent_words = []
                        sent_spaces = []
                        in_sent = False
                    #special case if < is a word
                    elif("<\t" in line):
                        sent_words.append(line.split("\t")[0])
                        sent_spaces.append(True)

                    elif("<field name=\"inhalt\">" in line or "</field>" in line or \
                         "<field name=\"stichwort\">" in line or "<field name=\"titel\">" in line or \
                         "<to-be-deleted-by-tree-tagger/>" in line or line[1:3] in ["g>", "p>", "/p", "p/"]):
                        #print("ERROR in " + path + ": s should not contain " + line)
                        pass
                    else:
                        print("ERROR in " + pf[0] + ": s should not contain " + line, line[0] == "<")
                        print("In", docid)
                        print(prev)
                        pass
                else:
                    sent_words.append(line.split("\t")[0])
                    sent_spaces.append(True)
        
        if(doc_sents):
            add_doc_sents_political(doc_sents, sents, found_candidates, politicians_at, docid, docprev, docprev_idx)
        if(prev):
            sents.append(([], [], prev))
    
    #store sentences with metadata
    with open(opath + "/" + file_name + str(splitIndex), 'a+b') as outf:
        pickle.dump(sents,  outf)#test_out, outf)
    print("Write to: " + opath + file_name + str(splitIndex))

    return []