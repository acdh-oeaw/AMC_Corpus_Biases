import re
#from pyspark.ml.feature import *
#from pyspark.ml.clustering import *
import json
from bs4 import BeautifulSoup as bs
from bs4 import NavigableString

def gen_reptok(fn, ln, short="", titel=""):
    lns = ln.split()
    prev_patt = "[^a-zA-ZüäöÜÄÖß0-9]"#"\s*"
    first_patt = "^"#"\s*"

    if(len(lns) > 2):
        rep = ""
        ln = ""
        for w in lns:
            ln += w + "\s*"
            rep += w
        ln = "\s*" + ln + "(e)?(s)?\s*"
        rep = rep[0].upper() + rep[1:].lower()
    else:
        rep = ln[0].upper() + ln[1:].lower()
        ln = "\s*" + ln + "(e)?(s)?\s*"
    
    rep_str = prev_patt + fn + ln + "|"
    rep_str += first_patt + fn + ln + "|"
    rep_str += prev_patt + fn[0] + "." + ln + "|"
    rep_str += first_patt + fn[0] + "." + ln + "|"
    rep_str += prev_patt + ln[3:] + "|"
    rep_str += first_patt + ln[3:]

    if(short):
        rep_str += "|" + prev_patt + short + "\s*"
        rep_str += "|" + first_patt + short + "\s*"
    if(titel):
        rep_str += "|" + prev_patt + titel + "\s*" + fn + ln + "|"
        rep_str +=  first_patt + titel + "\s*" + fn + ln + "|"
        rep_str +=  prev_patt + titel + "\s*" + fn[0] + "." + ln + "|"
        rep_str +=  first_patt + titel + "\s*" + fn[0] + "." + ln + "|"
        rep_str +=  prev_patt + titel + ln + "|"
        rep_str +=  first_patt + titel + ln
    
    return (" " + rep + " ", re.compile(rep_str, flags=re.MULTILINE|re.DOTALL|re.IGNORECASE))

pParagraph = r'<p>(.*?)</p>'
pReplaceToken = [gen_reptok("Alexander", "Van der Bellen", "vdb", "Dr."),
                 gen_reptok("Norbert", "Hofer", titel="Ing."),
                 gen_reptok("Irmgard", "Griss", titel="Dr."),
                 gen_reptok("Rudolf", "Hundstorfer"),
                 gen_reptok("Andreas", "Kohl", titel="Dr."),
                 gen_reptok("Richard", "Lugner", titel="Ing.")]
pWhitesp = r"\s+"

#politik für Kurier
#inland seite für Standard
#pDocs = r'<doc [^>]*?(?:inland|seite)[^>]*?>(.*?)</doc>'
pDocs = r'<doc ([^>]*?)>(.*?)</doc>'
pMetadata = r'([^=\s]*?)="([^"]*?)"'

def get_tags_strings(splitIndex, it):
    ret = []
    for in_file in it:
        
        soup = bs(in_file , "lxml-xml")
        ret.append(spacify_soup(soup.file))
        
    return ret

def combine_tags_strings(tags, strings):
    return "".join([tags[i] + strings[i] for i in range(len(strings))]) + tags[-1]

def spacify_soup(parent, tags=[""], strings=[]):
    tags[-1] += "<" + parent.name
    if(parent.attrs):
         tags[-1] += " " + " ".join([(k + '="' + a + '"') for k,a in parent.attrs.items()])
    if(parent.name == "doc"):
        strings.append("")
        tags.append("")
    tags[-1] += ">\n"

    for d in parent.children:
        if(isinstance(d, NavigableString)):
            if(str.strip(d)):
                strings.append(str.strip(d)  + "\n")
                tags.append("")            
        else:
            spacify_soup(d, tags, strings)
    tags[-1] += "</" + parent.name + ">\n"
    return (tags, strings)

def spacify_token_soup(parent, tags=[""], strings=[], spaces=[]):
    if((not parent.name in ["to-be-deleted-by-tree-tagger"])):
        tags[-1] += "<" + parent.name
        if(parent.attrs):
             tags[-1] += " " + " ".join([(k + '="' + a + '"') for k,a in parent.attrs.items()])

        if(not parent.name == "doc"):
            tags[-1] += ">\n"

##        if(parent.name in ["doc", "field", "meta_info"]):
##        # if(parent.name not in ["file"]):
##           strings.append([])
##           spaces.append([])
##           tags.append("")

        if(parent.name in ["doc"]):
            strings.append([])
            spaces.append([])
            tags.append("")
        ## hp 2019-04 : added elements for parlat : parent.name in ["field", "meta_info", "p", "s", "fmt", "section", "person", "rb", "comment", "timestamp", "pb"]
        ## hp 2019-10 : adapted to current state of parlat

        ## pb und milestone werden NICHT genannt, da sie keine children haben
        if(parent.name in ["field", "meta_info", "p", "s", "fmt", "section", "person", "rb", "comment", "timestamp", "pb", "cell", "desc", "div", "doc", "emph", "head", "incident", "item", "label", "list", "milestone", "name", "note", "p", "quote", "row", "seg", "table", "time", "u"] and \
           [d for d in parent.children if isinstance(d, NavigableString) and str.strip(d)]):
            strings.append([])
            spaces.append([])
            tags.append("")

        if(parent.name == "doc"):
            #print("doc", parent.attrs["id"], flush=True)
            tags[-1] += ">\n"
           
    for d in parent.children:
        if(isinstance(d, NavigableString)):
            if(str.strip(d)):
                words = [w for w in d.split("\n") if str.strip(w)]
                strings[-1].extend(words)
                spaces[-1].extend([True for _ in range(len(words))])
        elif (d.name == "g" and spaces[-1]):
            #print(parent.name, d.name, flush=True)
            spaces[-1][-1] = False
        else:
            spacify_token_soup(d, tags, strings, spaces)

    if((not parent.name in ["to-be-deleted-by-tree-tagger"])):
        tags[-1] += "</" + parent.name + ">\n"

    return (tags, strings, spaces)

def get_paragraphs_filtered_file(in_file, metadata_filter):
    (p, text) = in_file
    ret = []

    for (repl, patt) in pReplaceToken:
        text = re.sub(patt, repl, text)

    for (metadata, content) in re.findall(pDocs, text, flags=re.MULTILINE|re.DOTALL):
        metadata = re.findall(pMetadata, metadata, flags=re.MULTILINE|re.DOTALL)
        if(any(lambda mf: mf in [el for x in metadata for el in x], metadata_filter)):
            content = " ".join(re.findall(pParagraph, content.lower(), flags=re.MULTILINE|re.DOTALL))
            ret.append(content)

    return ret 
    #return re.findall(pParagraph, text, re.MULTILINE|re.DOTALL)
    
def get_paragraphs_filtered(splitIndex, it, metadata_filter):
    ret = []
    for in_file in it:
        ret.extend(get_paragraphs_filtered_file(in_file, metadata_filter))
        
    return ret

def get_paragraphs_file(in_file):
    (p, text) = in_file
    ret = []

    for (repl, patt) in pReplaceToken:
        text = re.sub(patt, repl, text)

    for (metadata, content) in re.findall(pDocs, text, flags=re.MULTILINE|re.DOTALL):
        #metadata = re.findall(pMetadata, metadata, flags=re.MULTILINE|re.DOTALL)
        content = " ".join(re.findall(pParagraph, content.lower(), flags=re.MULTILINE|re.DOTALL))
        ret.append(content)

    return ret 
    #return re.findall(pParagraph, text, re.MULTILINE|re.DOTALL)

def get_docs(splitIndex, it):
    ret = []
    for in_file in it:
        ret.append(get_paragraphs_file(in_file))
        
    return ret

def get_paragraph_file(in_file):
    (p, text) = in_file
    ret = []

    for (repl, patt) in pReplaceToken:
        text = re.sub(patt, repl, text)# flags=re.MULTILINE|re.DOTALL|re.IGNORECASE)

    for (metadata, content) in re.findall(pDocs, text, flags=re.MULTILINE|re.DOTALL):
        metadata = re.findall(pMetadata, metadata, flags=re.MULTILINE|re.DOTALL)
        content = re.findall(pParagraph, content.lower(), flags=re.MULTILINE|re.DOTALL)
        ret.extend(content)

    return ret 
    #return re.findall(pParagraph, text, re.MULTILINE|re.DOTALL)

def get_paragraph(splitIndex, it):
    ret = []
    for in_file in it:
        ret.extend(get_paragraph_file(in_file))
        
    return ret

def get_paragraphs(splitIndex, it):
    ret = []
    for in_file in it:
        ret.extend(get_paragraphs_file(in_file))
        
    return ret

def get_paragraphs_metadata_zipped2_file(in_file):
    (p, text) = in_file
    ret = []

    for (repl, patt) in pReplaceToken:
        text = re.sub(patt, repl, text)

    for (metadata, content) in re.findall(pDocs, text, flags=re.MULTILINE|re.DOTALL):
        metadata = dict(re.findall(pMetadata, metadata, flags=re.MULTILINE|re.DOTALL))
        content = re.findall(pParagraph, content.lower(), flags=re.MULTILINE|re.DOTALL)
        for p in content:
            ret.append((metadata["id"], p))

    return ret
    
def get_paragraphs_metadata_zipped2(splitIndex, it):
    ret = []
    for in_file in it:
        ret.extend(get_paragraphs_metadata_zipped2_file(in_file))
        
    return ret

def get_paragraphs_metadata_zipped_file(in_file):
    (p, text) = in_file
    ret = []

    for (repl, patt) in pReplaceToken:
        text = re.sub(patt, repl, text)

    for (metadata, content) in re.findall(pDocs, text, flags=re.MULTILINE|re.DOTALL):
        dmeta = dict(re.findall(pMetadata, metadata, flags=re.MULTILINE|re.DOTALL))
        content = re.findall(pParagraph, content, flags=re.MULTILINE|re.DOTALL)
        #smeta = dmeta["id"] + ";" + dmeta["datum"] + ";" + dmeta["docsrc"] + ";" + dmeta["ressort2"] + "."
        smeta = [dmeta["id"], dmeta["datum"], dmeta["docsrc"], dmeta["ressort2"]]
 
        for p in content:
            #p = re.sub('[^a-zA-ZüäöÜÄÖß0-9-,.:\s]', '', p)
            if(p and len(p.split()) > 3):
                ret.append((smeta, p))
                #ret.append(smeta + p)
        #ret.append((dict(metadata)["id"], metadata))

    return ret
    
def get_paragraphs_metadata_zipped(splitIndex, it):
    ret = []
    for in_file in it:
        ret.extend(get_paragraphs_metadata_zipped_file(in_file))
        
    return ret

def get_paragraphs_metadata_file(in_file):
    (p, text) = in_file
    ret = []

    for (repl, patt) in pReplaceToken:
        text = re.sub(patt, repl, text)

    for (metadata, content) in re.findall(pDocs, text, flags=re.MULTILINE|re.DOTALL):
        metadata = re.findall(pMetadata, metadata, flags=re.MULTILINE|re.DOTALL)
        #content = " ".join(re.findall(pParagraph, content, re.MULTILINE|re.DOTALL))
        ret.append((metadata, 0))#content))

    return ret
    
def get_paragraphs_metadata(splitIndex, it):
    ret = []
    for in_file in it:
        ret.extend(get_paragraphs_metadata_file(in_file))
        
    return ret


def filter_sentences(l):
    ret = []
    
    for paragraph in l:
        paragraph_list = []
        for sentence in re.sub('[^a-zA-ZüäöÜÄÖß0-9-.\s]', '', paragraph).split("."):
            tokenized_sentence = sentence.replace("\n", " ").split(" ")
            non_empty_string_sentece = filter(None, tokenized_sentence)
            if(non_empty_string_sentece):
                paragraph_list.extend(non_empty_string_sentece)
        if(paragraph_list):
            ret.append(paragraph_list)
    return ret

def remove_stopwords(tokenized_df):
    #Set params for StopWordsRemover
    german_stopw = StopWordsRemover.loadDefaultStopWords("german")
    with open("/tmp/data/stopwords-json/dist/de.json", "r") as f:
        ger_stopwords = set(["mehr", "ganz", "kurz", "macht", "geht", "list", "radio-tipps", "switch", "neue", "heute", "euro", "beim", "zwei", "gibt", "drei", "jahre", "neuer", "wurde", "schon", "zurück", "neues", "überblick", "the", "apa", "prozent", "utl", "österreich", "amp", "laut", "-jährige", "leben", "http", "land", "stadt", "montag", "dienstag", "mittwoch", "donnerstag", "freitag", "samstag", "sonntag", "derzeit", "wwwotsat", "ots", "geben", "stehen"] + json.load(f))
    german_stopw.extend(ger_stopwords)

    remover = StopWordsRemover().setStopWords(german_stopw).setInputCol("tokens").setOutputCol("filtered")

    #Create new DF with Stopwords removed
    return remover.transform(tokenized_df)

def tokenize(corpus_df):
    tokenizer = RegexTokenizer().setPattern("[^a-zA-ZüäöÜÄÖß_-]+").setMinTokenLength(3).setInputCol("text").setOutputCol("tokens")
    return tokenizer.transform(corpus_df)

def vectorize(filtered_df):
    vectorizer = CountVectorizer().setInputCol("filtered").setOutputCol("features").setMinDF(100).fit(filtered_df)
    #.setVocabSize(50000)
    return (vectorizer.vocabulary, vectorizer.transform(filtered_df))

def prepare_corpus_lda(corpus):
    corpus_df = corpus.zipWithIndex().toDF(["text", "id"])

    tokenized_df = tokenize(corpus_df)
    filtered_df = remove_stopwords(tokenized_df)
    (vocabArray, vectorized_df) = vectorize(filtered_df)

    return (vocabArray, vectorized_df.select("id", "features"))#.cache())

def tokenize_w2v(ps):
    return [[ el.lower() for el in y.split() if len(el) > 2] for x in map(lambda row: filter(lambda x: x != "", re.sub('[^a-zA-ZüäöÜÄÖß0-9.\s]', '', row).split(".")), ps) for y in x]
