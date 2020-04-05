
def split_triplets(sentiment, triplets):
    (positives, negatives) = sentiment
    svo_neg = []
    svo_pos = []

    for svo in triplets:
        if svo[0] in positives:
            svo_pos.append(svo)
            #svo_pos.append((svo[1], svo[2]))
        elif svo[0] in negatives:
            svo_neg.append(svo)
            #svo_neg.append((svo[1], svo[2]))
        #else:
            #print(svo[0])
    return (svo_pos, svo_neg)

def neutral_verbs(selection, verbs, triplets):
    ret = {}
    for svo in triplets:
        for sp in selection:
            if (not svo[0] in verbs and any([sp in ws for ws in svo if isinstance(ws, list)])): #sp in svo and 
                if svo[0] in ret:
                    ret[svo[0]] += 1
                else:
                    ret[svo[0]] = 1
    return ret

def lookup_svo(selection, triplets):
    ret = dict([(sp, []) for sp in selection])
    for sp in selection:
        for svo in triplets:
            nouns = [w for ws in svo if isinstance(ws, list) for w in ws]
            if(sp in nouns):
                #ret[sp].append(nouns + [svo[-1]])
                other = [ws for ws in svo if isinstance(ws, list) and not sp in ws]
                if other:
                    ret[sp].append(other[0][0])#svo[(svo.index(sp) + 1) % 2)#[s for s in svo if not sp == s])#svo[(svo.index(sp) + 1) % 2])
                else:
                    ret[sp].append(svo)
    return ret.values()

def read_triplets(sc, folder, newspapers):
    def read_svo_line(line):
        l = line.split(";")
        if(l[1][0] == "[" and l[1][-1] == "]"):
            l[1] = l[1][1:-1].replace("'", "").split(",")
        if(l[2][0] == "[" and l[2][-1] == "]"):
            l[2] = l[2][1:-1].replace("'", "").split(",")
        return l

    svos = sc.parallelize([])
    print("#SVO Triplets")
    for np in newspapers:
        svos_np = sc.textFile(folder + "svo_triplets_" + np).map(read_svo_line)
        print(np + ": " + str(svos_np.count()))
        svos = svos.union(svos_np)
    return svos

def so_sentiment_spark(sentiment, svos):
    (positives, negatives) = sentiment

    svo_pos = svos.filter(lambda svo: svo[0] in positives)
    svo_neg = svos.filter(lambda svo: svo[0] in negatives)
    svo_neu = svos.filter(lambda svo: not (svo[0] in positives and svo[0] in negatives))
    
    so_pos = svo_pos.map(lambda svo: (svo[1][0], svo[2][0])).repartition(12).cache()
    so_neg = svo_neg.map(lambda svo: (svo[1][0], svo[2][0])).repartition(12).cache()
    so_neu = svo_neu.map(lambda svo: (svo[1][0], svo[2][0])).repartition(12).cache()
    
    return (so_pos, so_neg, so_neu)
    #return svos.groupBy(lambda svo: 0 if svo[0] in positives else 1 if svo[0] in negatives else 2)

def lookup_svo_spark(selection, triplets):
    filtered = triplets.filter(lambda svo: any([selection in ws for ws in svo if isinstance(ws, list)]))
    print(selection)
    return filtered.map(lambda svo: svo[1][0] if svo[2][0] == selection else svo[2][0]).collect()
    #return filtered.map(lambda svo: [ws[0] for ws in svo if isinstance(ws, list) and len(ws) > 0 and not selection in ws][0]).collect()

def lookup_so_spark(selection, triplets):
    #print(selection)
    filtered = triplets.filter(lambda so: selection in so)
    return filtered.map(lambda so: so[0] if so[1] == selection else so[1]).collect()
    
def svo_other_freq(s, triplets, visited):
    ret = {}
    for other in lookup_so_spark(s, triplets):
        if not other in visited:
            if not other in ret:
                ret[other] = 1
            else:
                ret[other] += 1
    return ret

def create_svo_net(start, triplets, depth):
    ps = {}
    visited = set()
    nv = set()
    
    for s in start:
        visited.add(s)
        ps[s] = svo_other_freq(s, triplets, visited)
        nv.update(ps[s].keys())
    #nv = nv - visited

    for i in range(depth-1):
        hnv = nv
        nv = set()

        for o in hnv:
            visited.add(o)
            ps[o] = svo_other_freq(o, triplets, visited)
            nv.update(ps[o].keys())
        #nv = nv - visited
            
    return ps

def write_svo_net(out_file, svo_net):
    with open(out_file, "w") as f:
        for (node, neighbors) in svo_net.items():
            for (neighbor, count) in neighbors.items():
                f.write(node+";"+neighbor+";"+str(count)+"\n")
