import os
import sys
import re
import pickle
import json
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET
from lxml import etree
from bs4 import BeautifulSoup as bs
from bs4 import NavigableString

def parse(path_to_file):
    with open(path_to_file, "r") as f:
        soup = bs(f, "lxml-xml")
        lines = []
        headers = soup.find_all('doc')
        
        dict_headers = defaultdict(list)
        list_keys = ['id',
                     'datum_full',
                     'datum',
                     'year',
                     'yymm',
                     'bibl',
                     'mediatype',
                     'docsrc',
                     'docsrc_name',
                     'region',
                     'province',
                     'ressort2',
                     'autor', 
                     'mutation',
                     'deskriptor',
                     'keys',             
                     'tokens',
                     'wordcount',
                     'dupl']

        for header in headers:
            for key in list_keys:
                if key in header.attrs.keys():
                    dict_headers[key].append(header[key])
                else:
                    dict_headers[key].append(None)
        df_headers = pd.DataFrame(dict_headers)
        
        for idx, header in enumerate(headers):
            fields = header.find_all('field')
            for field in fields:
                name = field['name']
                for satz in field.find_all('s'):
                    text = re.sub('<g/>','',satz.text)
                    text = re.sub(r'[\n]+','\n',text)
                    rows = re.split('\n',text)[1:-1]
                    for row in rows:
                        list_attribs = re.split('\t',row)
                        lines.append([idx, name] + list_attribs)
                        
        df_fields = pd.DataFrame.from_records(lines, columns=['id_article',
                                                              'wert',
                                                              'word',
                                                              'lc',
                                                              'ix',
                                                              'posUDS',
                                                              'posTT',
                                                              'ner',
                                                              'iob',
                                                              'ixNP',
                                                              'dep',
                                                              'ixDEP',
                                                              'neg',
                                                              'lemma',
                                                              'pos',
                                                              'posTI',
                                                              'lempos',
                                                              'lemmasource',])
        
        df_fields = df_fields.reindex(columns=['id_article','wert', 
                                               'word', 'lc', 'lemma', 'lempos', 
                                               'pos', 'posTI', 'posUDS', 'posTT', 
                                               'dep', 'ner', 'ix', 'ixNP', 'ixDEP', 'iob', 
                                               'neg', 'lemmasource'])
        
        df_fields['ix'] = pd.to_numeric(df_fields['ix']).astype(int)
        df_fields['ixDEP'] = pd.to_numeric(df_fields['ixDEP'], errors='coerce').astype(int)
        df_fields['ixNP'] = pd.to_numeric(df_fields['ixNP'], errors='coerce')
        
        for idx in df_fields["id_article"].unique():
            # parsing title, keywords and text
            title = ' '.join(df_fields[(df_fields.id_article == idx) & (df_fields.wert == 'titel')]['word'])
            keywords = ', '.join(df_fields[(df_fields.id_article == idx) & (df_fields.wert == 'stichwort')]['word'])
            text = ' '.join(df_fields[(df_fields.id_article == idx) & (df_fields.wert == 'inhalt')]['word'])

            # delete space(s) before: ?!.,;:)
            text = re.sub(r'\s+([?!.,;:)])', r'\1', text)
            # delete space(s) after: (
            text = re.sub(r'([(])\s+', r'\1', text)
            # delete: *
            text = re.sub('\*', '', text)        

            # identifying and extracting subtitle, if available
            if text.find('Utl.') != -1:
                subtitle = text[text.find('Utl.')+6 : text.find('=')]
                text = text[text.find('=')+2:]
            else:
                subtitle = "None"

            # identifying and extracting place and news agency
            place_agency_apa = re.compile(r'^[\w\s]+\([\w\s\/]+\)[\s-]+',re.IGNORECASE)
            place_agency = re.findall(place_agency_apa, text)
            if place_agency:
                pos = place_agency[0].find('(')
                place = place_agency[0][:pos].strip()
                agency = place_agency[0][pos:].strip('-() ')
                text = text[len(place_agency[0]):]
            else:
                place = "None"
                agency = "None"

            # identifying and extracting final notes 
            # e.g. (Schluß),(Forts.),(Fortsetzung),(Forts. mögl.), (Forts. mgl.)
            final_notes_apa = re.compile(r'\(Schluß\)|\(Fort[^\n]+\)',re.IGNORECASE)
            final_notes_sn = re.compile(r'(Bild[.:\/0-9A-Za-z, ]{0,30})$|(Seit[en ]*[.:\/0-9A-Za-z, ]{0,30})$')
            final_notes = re.findall(final_notes_sn, text)
            if final_notes:
                if isinstance(final_notes[0], tuple):
                    final_notes[0] = final_notes[0][-1]
                pos = text.find(final_notes[0])
                final_note = text[pos:]
                text = text[:pos]
            else:
                final_note = 'None'
                
            # retrieving some fields from the header
            headers = df_headers.loc[idx,['id', 'datum']].to_list()
            #yield (idx, headers, place, agency, title, subtitle, keywords, text, final_note)
            yield text.split()
