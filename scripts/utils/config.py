from os import listdir, walk
import io
from os.path import isfile, join
import sys
import os
import zipfile
import spacy
import json

zippath = '/tmp/data/mylib.zip'      # some random filename in writable directory
ziplibpath = "/notebooks/article_parser"

articles2016 = "/tmp/data/R_amc_2_2016_7679/101_derived/artikel_"
suffix_2016 = "article_2016"

articles2015_16 = "/tmp/data/R_amc_2_2015-2016_6550/101_derived/artikel_"
suffix_2015_16 = "article_2015_16"

articles2013_15 = "/tmp/data/R_amc_2_2013-2015_4679/101_derived/artikel_"
suffix_2013_15 = "article_2013_15"

results_path = "/tmp/data/json_output"

test_dir = articles2015_16 + "/STANDARD"
test_file = test_dir + "/STANDARD_20151009.xml"

num_cores = 8
spark_data = "/tmp/data/spark_data"

nlp = None

#Ekel, Freude, Furcht, Trauer, Ueberraschung, Verachtung, Wut
emotion_dir = "/tmp/data/semantics/german-emotion-dictionary/fundamental"

bolt_url = "bolt://172.17.0.6:7687"

newspapers = ["HEUTE", "APA", "KRONE", "KLEINE", "KURIER", "NVB", "MEDIANET",
       "NVT", "OEREICHE", "OESTERREICH", "OOEN", "OTS", "PRESSE", "SN",
       "STANDARD", "TT", "TTKOMP", "VN", "WIBLATT", "WZ"]

def ziplib(fname):
    #libpath = os.path.dirname(fname)                  # this should point to your packages directory
    zf = zipfile.PyZipFile(zippath, mode='w')
    try:
        #zf.debug = 0                                              # making it verbose, good for debugging
        zf.writepy(fname)
        #zf.writepy(libpath)
        return zippath                                             # return path to generated zip archive
    finally:
        zf.close()
    
def spark_init(sc):
    if(isfile(zippath)):
        os.remove(zippath)
    zip_path = ziplib(ziplibpath)
    sc.addPyFile(zip_path)
    #nlp = spacy.load('de')

def spark_init_all_file(sc):
    sys.path.append(ziplibpath)

    for (root, dirs, files) in walk(ziplibpath):
        for f in files:
            if(f.endswith(".py")):
                sc.addPyFile(join(root, f))