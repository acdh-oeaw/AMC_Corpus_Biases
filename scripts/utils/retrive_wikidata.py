from SPARQLWrapper import SPARQLWrapper, JSON
from datetime import datetime
import requests

def query_wikidata(query):
    sparql = SPARQLWrapper("https://query.wikidata.org/bigdata/namespace/wdq/sparql")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results

def query_pageviews(sitelinkde):
    if(sitelinkde):
        sitelinkde = next(iter(sitelinkde))
        req = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/de.wikipedia.org/all-access/all-agents/" + \
          sitelinkde[30:].replace("%20", "_") + "/monthly/20150801/20170101"
           
        print(req)
        response = requests.get(req)
        data = response.json()
        if("items" in data):
            pageviews = [int(items["views"]) for items in data["items"]]
        else:
            pageviews = [0] * 11
    else:
        pageviews = [0] * 11

    return pageviews

def get_probs_by_type(res, names):
    ret = {}
    cprobs = []
    rid = names[0]
    other = names[1:]

    for r in res["results"]["bindings"]:
        if(rid in r):
            if(r[rid]["value"] in ret):
                cprobs = ret[r[rid]["value"]]
            else:
                cprobs = [set() for _ in range(len(other))]
                ret[r[rid]["value"]] = cprobs

            for (i, prob) in enumerate(other):
                if(prob in r):
                    cprobs[i].add(r[prob]["value"])
    return ret

def get_probs(res, *names):
    ret = []
    for name in names:
        resset = set()
        for r in res["results"]["bindings"]:
            if(name in r):
                resset.add(r[name]["value"])
        ret.append(resset)
    return ret

def retrive_type_of_ent():
    """
    SELECT distinct ?item ?enttype ?itemLabel ?itemDescription
    WHERE{
      {?item ?label "Wiener Neustadt"@de } UNION {?item ?label "Wiener Neustadt"@en }.
      #{?item ?label "Champions League"@de } UNION {?item ?label "Champions League"@en }.
      #{?item ?label "UEFA"@de } UNION {?item ?label "UEFA"@en }.
      {
      ?item wdt:P31 wd:Q5.
      BIND("PERSON" AS ?enttype).
      } UNION {
      { ?item wdt:P31/wdt:P279* wd:Q1190554 } UNION { ?item wdt:P279* wd:Q1190554 }.
      BIND("EVENT" AS ?enttype).
      } UNION {
      { ?item wdt:P31/wdt:P279* wd:Q618123 } UNION { ?item wdt:P279* wd:Q618123 }.
      BIND("PLACE" AS ?enttype).
      } UNION {
      { ?item wdt:P31/wdt:P279* wd:Q43229 } UNION { ?item wdt:P279* wd:Q43229 }.
      BIND("ORG" AS ?enttype).
      }
      ?article schema:about ?item .
      #?article schema:inLanguage "de" .
      #?article schema:isPartOf <https://de.wikipedia.org/>.	
      SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
    }
    """

def retrive_data_person(missing_people):
    addinfo = {}
    for (i, pid) in enumerate([m[0] for m in missing_people]):
        if(i > 243):
            print(pid)
            p_info = query_wikidata("""
                Select ?person ?sitelinkde (COUNT(DISTINCT(?sitelink)) as ?sitecnt) ?position ?positionLabel ?firstnameLabel ?secondnameLabel ?nicknameLabel ?alternativeLabel ?party ?birthplaceLabel ?dateofbirth
                Where {
                    BIND(<""" + pid + """> AS ?person).
                    OPTIONAL { ?person  wdt:P39 ?position . }
                    OPTIONAL { ?sitelinkde schema:about ?person ;
                                schema:inLanguage "de" . }
                    OPTIONAL { ?sitelink schema:about ?person . }
                    OPTIONAL { ?person wdt:P735 ?firstname . }
                    OPTIONAL { ?person  wdt:P734 ?secondname .}
                    OPTIONAL { ?person  wdt:P1449 ?nickname .}
                    OPTIONAL { ?person  skos:altLabel ?alternativeLabel .
                        FILTER(lang(?alternativeLabel) = "de")}
                    OPTIONAL { ?person  wdt:P102 ?party .}
                    OPTIONAL { ?person  wdt:P19 ?birthplace .}
                    OPTIONAL { ?person  wdt:P569 ?dateofbirth .}
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
                }
                GROUP BY ?person ?sitelinkde ?positionLabel ?position ?firstnameLabel ?secondnameLabel ?nicknameLabel ?alternativeLabel ?party ?birthplaceLabel ?dateofbirth
            """)
            probs = get_probs(p_info, "firstnameLabel", "secondnameLabel", "nicknameLabel", \
                              "alternativeLabel", "party", "birthplaceLabel", "dateofbirth", \
                              "sitelinkde", "sitecnt", "position", "positionLabel")

            if(probs[7]):
                sidelinkde = next(iter(probs[7]))
                req = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/de.wikipedia.org/all-access/all-agents/" + \
                  sidelinkde[30:].replace("%20", "_") + "/monthly/20150801/20170101"

                response = requests.get(req)
                data = response.json()
                if("items" in data):
                    pageviews = [int(items["views"]) for items in data["items"]]
                    if(len(pageviews) < 17):
                        pageviews = ( [0] * (17 - len(pageviews)) ) + pageviews
                else:
                    pageviews = [0] * 17
            else:
                pageviews = [0] * 17

            probs.append(pageviews)
            addinfo[pid] = probs
            print("#" + str(i) + " of " + str(len(missing_people)))
            #print((pid, probs))
    return addinfo

def retrive_at_organization():
    all_orgs_s = """
    Select ?org ?orgLabel ?alternativeLabel ?chair ?chairLabel ?sitelinkde (COUNT(DISTINCT(?sitelink)) as ?sites)
    Where {
      {
        ?org wdt:P159 ?loc.
        ?loc wdt:P17 ?country.
        FILTER(?country = wd:Q40)
        OPTIONAL { ?org  wdt:P488 ?chair . }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
        OPTIONAL { ?org  skos:altLabel ?alternativeLabel .
                  FILTER(lang(?alternativeLabel) = "de")}
        OPTIONAL { ?sitelinkde schema:about ?org ;
                    schema:inLanguage "de" . }
        OPTIONAL { ?sitelink schema:about ?org . }
      }
    }
    GROUP BY ?org ?orgLabel ?alternativeLabel ?chair ?chairLabel  ?sitelinkde ?classlbl
    """

    probs = ["org", "orgLabel", "alternativeLabel", "chair", "chairLabel", "sitelinkde", "sites"]

    return get_probs_by_type(query_wikidata(all_orgs_s), probs)


def retrive_at_person():
    people_unrestricted = """
    SELECT DISTINCT ?person ?fullName ?gender (group_concat(DISTINCT ?occupation;separator=";") as ?occupations)
    WHERE {
      ?person wdt:P31 wd:Q5 ;
              wdt:P27 wd:Q40 ;
              rdfs:label ?fullName .

      FILTER(lang(?fullName) = "de")
      OPTIONAL { ?person wdt:P21/rdfs:label ?gender .
                FILTER(lang(?gender) = "de") }
      OPTIONAL {?person wdt:P106/rdfs:label ?occupation.
                FILTER(lang(?occupation) = "de") }
    }
    GROUP BY ?person ?fullName ?gender
    """
    return get_probs_by_type(query_wikidata(people_unrestricted), ["person", "fullName", "gender", "occupations"])

def retrive_at_place():
    places_s = """
    Select ?org ?orgLabel ?alternativeLabel ?chair ?chairLabel ?sitelinkde (COUNT(DISTINCT(?sitelink)) as ?sites)
    Where {
      {
        ?org p:P31/ps:P31/wdt:P279* wd:Q43229 ;
             p:P31/ps:P31/wdt:P279* wd:Q361733 ;
             wdt:P17 wd:Q40.
        OPTIONAL { ?org  wdt:P488 ?chair . }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
        OPTIONAL { ?org  skos:altLabel ?alternativeLabel .
                  FILTER(lang(?alternativeLabel) = "de")}
        OPTIONAL { ?sitelinkde schema:about ?org ;
                    schema:inLanguage "de" . }
        OPTIONAL { ?sitelink schema:about ?org . }
      }
    }
    GROUP BY ?org ?orgLabel ?alternativeLabel ?chair ?chairLabel  ?sitelinkde ?classlbl
    """
    probs = ["org", "orgLabel", "alternativeLabel", "chair", "chairLabel", "sitelinkde", "sites"]

    return get_probs_by_type(query_wikidata(places_s), probs)

def retrive_at_parties():
    partyinfos = {}
    for pid in parties:
        p_info = query_wikidata("""
        Select ?party ?partyLabel ?alternativeLabel ?chair ?chairLabel  ?sitelinkde (COUNT(DISTINCT(?sitelink)) as ?sites)
        Where {
            {
                BIND(<""" + pid + """> AS ?party).
                OPTIONAL { ?party  wdt:P488 ?chair . }
                SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
                OPTIONAL { ?party  skos:altLabel ?alternativeLabel .
                    FILTER(lang(?alternativeLabel) = "de")}
                ?sitelinkde schema:about ?party ;
                    schema:inLanguage "de" .
                ?sitelink schema:about ?party .
            }
        }
        GROUP BY ?party ?partyLabel ?alternativeLabel ?chair ?chairLabel  ?sitelinkde
        """)
        probs = get_probs(p_info, "partyLabel", "alternativeLabel", "chair", "chairLabel", "sitelinkde", "sites")

        if(probs[4]):
            sidelinkde = next(iter(probs[0]))
            req = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/de.wikipedia.org/all-access/all-agents/" + \
              sidelinkde[30:].replace("%20", "_") + "/monthly/20150801/20170101"

            response = requests.get(req)
            data = response.json()
            if("items" in data):
                pageviews = [int(items["views"]) for items in data["items"]]
            else:
                pageviews = [0] * 11
        else:
            pageviews = [0] * 11

        probs.append(pageviews)
        partyinfos[pid] = probs

        print("#" + str(i) + " of " + str(len(vocab.keys())))
        print((pid, probs))
    return partyinfos

def fix_dateformat_error(vocab):
    for pid in vocab.keys():
        if(vocab[pid][9] and isinstance(vocab[pid][9], set)):
            date = next(iter(vocab[pid][9])).split("T")
            if(len(date) == 2):
                print(pid)
                print(vocab[pid][9])
                vocab[pid][9] = datetime.strptime(date[0], "%Y-%m-%d")
            else:
                print("Error: " + pid)
                vocab[pid][9] = datetime.strptime("0001-01-01", "%Y-%m-%d")
        elif(not vocab[pid][9]):
            print("Error: " + pid)
            vocab[pid][9] = datetime.strptime("0001-01-01", "%Y-%m-%d")