import os
import sys
import re
import pickle
import pandas as pd
import gensim
from gensim.models import KeyedVectors
from gensim.models.fasttext import FastText
from gensim.models import Word2Vec

import parser

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:{} took: {:2.4f} sec'.format(f.__name__, te-ts))
        return result
    return wrap


def articles_reader(begin, end):
    responses = []
    for filepath in df_files[begin:end].path:
        path = os.path.join(folder, filepath)
        response = parser.parse(path)
        response = list(response)
        responses.extend(response)
    return responses


if __name__ == "__main__":
    df_files = pd.read_pickle('../outputs/df_files.pkl')
    folder = '/home/jovyan/shared/C_amc_141/R_amc_3.1_12921/203_vert_spacy_rftt/'

    embedding_size = 300
    window_size = 8
    min_word = 2
    down_sampling = 1e-2


model = FastText(responses,
                 size=embedding_size,
                 window=window_size,
                 min_count=min_word,
                 sample=down_sampling,
                 workers = -1,
                 sg=1,
                 iter=100)


model.build_vocab(responses, update=True)  # Update the vocabulary
model.train(responses, total_examples=len(responses), epochs=model.epochs)


model.save("../outputs/amc.fasttext.300.model")


