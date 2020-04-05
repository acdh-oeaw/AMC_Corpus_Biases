from operator import add

from .file_io import store_object

def select_per_token(sel, transform=None):

    if (not transform):
        return ("_".join(sel[:-1]), int(sel[-1]))
    else:
        return [("_".join(ps[:-1]), int(ps[-1])) for ps in transform(sel)]

def select_metadata_per_article(x, props):
    porp_value = [""] * len(props)
    for (k, v) in x[0]:
        if(k in props):
            porp_value[props.index(k)] = v

    return tuple(porp_value)

def count_metadata_per_article(corpus, *probs, transform=None, suffix=""):
    probs = list(probs)
    
    if(not transform):
        ret = corpus.map(lambda x: select_metadata_per_article(x, probs)).countByValue().items()
    else:
        ret = corpus.flatMap(lambda x: transform(select_metadata_per_article(x, probs))).countByValue().items()        
    ret = list(ret)

    if(transform): probs.append("transformed")
    store_object(ret, "count_metadata_per_article", "_".join(probs) + "_" + suffix)

    return ret

def count_metadata_per_token(corpus, *probs, transform=None, suffix=""):
    probs = list(probs)
    probs.append("tokens")

    if(not transform):
        ret = corpus.map(lambda x: select_per_token(select_metadata_per_article(x, probs)))
    else:
        ret = corpus.flatMap(lambda x: select_per_token(select_metadata_per_article(x, probs), transform))
    ret = [tuple(k.split("_") + [v]) for (k,v) in ret.reduceByKey(add).collect()]

    if(transform): probs.append("transformed")
    store_object(ret, "count_metadata", "_".join(probs) + "_" + suffix)

    return ret

def avg_metadata_per_token(corpus, *probs, transform=None, suffix=""):
    probs = list(probs)

    if(not transform):
        per_article = corpus.map(lambda x: select_metadata_per_article(x, probs)).countByValue().items()
        probs.append("tokens")
        per_token = corpus.map(lambda x: select_per_token(select_metadata_per_article(x, probs)))
    else:
        per_article = corpus.flatMap(lambda x: transform(select_metadata_per_article(x, probs))).countByValue().items()
        probs.append("tokens")
        per_token = corpus.flatMap(lambda x: select_per_token(select_metadata_per_article(x, probs), transform))

    per_token = per_token.reduceByKey(add).collectAsMap()

    ret = []
    for (k, v) in per_article:
        ret.append((k, float(per_token["_".join(k)]) / float(v)))

    if(transform): probs.append("transformed")
    store_object(ret, "avg_metadata", "_".join(probs) + "_" + suffix)

    return ret

def count_metadata_keys(corpus, suffix=""):
    ret = corpus.flatMap(lambda x: [k for (k,v) in x[0]]).countByValue().items()
    ret = list(ret)
    store_object(ret, "count_metadata_keys", "metadata_" + suffix)
    
    return ret