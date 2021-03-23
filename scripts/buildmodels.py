import os
import sys
import re
import time
import pickle
import pandas as pd
import gensim
from gensim.models import KeyedVectors
from gensim.models.fasttext import FastText
from gensim.models import Word2Vec

import parser

df_files = pd.read_pickle('../outputs/df_files.pkl')
folder = '/home/jovyan/shared/C_amc_141/R_amc_3.1_12921/203_vert_spacy_rftt/'

def articles_reader(begin, end):
    responses = []
    for filepath in df_files[begin:end].path:
        path = os.path.join(folder, filepath)
        response = parser.parse(path)
        response = list(response)
        responses.extend(response)
    return responses

def build_model(list_sentences, parameters,mode=FastText):
    model = mode(list_sentences,
                 size=parameters['embedding_size'],
                 window=parameters['window_size'],
                 min_count=parameters['min_word'],
                 sample=parameters['down_sampling'],
                 workers = -1,
                 sg=1,
                 iter=100)
    return model

def update_model(model, list_sentences):
    model.build_vocab(list_sentences, update=True)
    model.train(list_sentences, total_examples=len(list_sentences), epochs=model.epochs)
    return model

if __name__ == "__main__":

    parameters = {'embedding_size':300,
              'window_size': 8,
              'min_word': 3,
              'down_sampling': 1e-2
             }
    
    for idx in range(0,len(df_files),50):
        print(f'Processing files from {idx} to {idx+50}')
        articles = articles_reader(idx,idx+50)
        if idx == 0:
            model = build_model(articles, parameters)
        else:
            model = update_model(model, articles)

    model.save("../outputs/amc.fasttext.300.model")


