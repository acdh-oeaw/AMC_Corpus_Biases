from os import listdir, walk
import io
from os.path import isfile, join
from bs4 import BeautifulSoup as bs
import sys
import re
import numpy
import json

from .config import results_path
from .text_filter import filter_sentences, get_paragraphs


def read_one_file(path, process_file, post_processing=filter_sentences):
    with open(path, encoding="utf-8") as f:
        ps = process_file((path, f.read()))

    return post_processing(ps)


def read_articles(datapath, sc, map_fpartioned=get_paragraphs, map_f=None, flatmap_f=None):
    words = sc.parallelize([])
    for (root, dirs, files) in walk(datapath):
        print(root)

        if (map_fpartioned):
            tfs = sc.wholeTextFiles(root, minPartitions=8)
            tfs = tfs.mapPartitionsWithIndex(map_fpartioned)
        else:
            tfs = sc.wholeTextFiles(root)

        if (map_f):
            tfs = tfs.map(map_f)
        if (flatmap_f):
            tfs = tfs.flatMap(flatmap_f)

        # filtered = tfs.map(lambda ps: re.sub('[^a-zA-ZüäöÜÄÖß0-9-:?,.\s]', '', ps))
        # filtered = tfs.flatMap(lambda ps: [x for x in map(lambda row: re.sub('[^a-zA-ZüäöÜÄÖß0-9-.\s]', '', row), ps) if len(x) > 3])
        words = words.union(tfs)
    # words.cache()
    # print(words.count())

    return words


"""
def read_articles_slow(datapath, sc, process_file):
    inp = sc.parallelize([])
    for (root, dirs, files) in walk(datapath):
        print(root)
        #if(root != datapath):
        tfs = sc.wholeTextFiles(root)
        words = tfs.map(process_file)
        .flatMap()
        inp = inp.union(words.filter(lambda el: len(el) > 2))
        #for file in files:
        #    process_file((file, open(join(root, file), encoding="utf-8").read()))
    return inp

"""


def store_object(obj, prefix, suffix=None):
    if (suffix):
        with open(join(results_path, prefix + "_" + suffix), "w") as f:
            json.dump(obj, f, ensure_ascii=False)


def load_object(prefix, suffix):
    with open(join(results_path, prefix + "_" + suffix), "r") as f:
        return json.load(f)
