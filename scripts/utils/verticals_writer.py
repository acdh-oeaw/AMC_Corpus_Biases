from os.path import join
from os import walk

from .verticals_parser import process_file

def process_partition(splitIndex, it):

    ws = (set(), set(), set())
    first = True

    for pf in it:
        if(first):
            csvwriter = NeoCSVWriter(spark_data, suffix=str(splitIndex), mode="a")
            first = False
        navs = process_file(pf, csvwriter)
        for i in range(0,3):
                ws[i].update(navs[i])
    csvwriter.close()

    return [(i, ws[i]) for i in range(0,3)]
    #return [ws]

def writecsv_spark(sc, datapath, ws=None):
    for (root, dirs, files) in walk(datapath):
        print(root)
        if(files != []):
            tfs = sc.wholeTextFiles(root, minPartitions=num_cores)#.cache()
            #print(tfs.getNumPartitions())
            ws_partioned = tfs.mapPartitionsWithIndex(process_partition)#.collect()
            #print(len(ws_partioned))
            if(ws): ws_partioned = ws_partioned.union(ws)
            ws = ws_partioned.reduceByKey(lambda x,y: x.union(y))
            #ws = ws_partioned.reduce(lambda x,y: tuple([ sc.parallelize(x[i].union(y[i])) for i in range(0, 3)]))

    return ws

def save_words_spark(ws):
    print("save words...")
    ws = ws.cache()
    ws.flatMap(lambda x: [] if not x[0] == 0 else x[1]).saveAsTextFile(spark_data + "/nouns")
    ws.flatMap(lambda x: [] if not x[0] == 1 else x[1]).saveAsTextFile(spark_data + "/adjectives")
    ws.flatMap(lambda x: [] if not x[0] == 2 else x[1]).saveAsTextFile(spark_data + "/verbs")
    #m = ["/tmp/data/nouns0.csv", "/tmp/data/adjectives0.csv", "/tmp/data/verbs0.csv"]
    #ws.foreach(lambda x: with open(m[x[0]], mode='w', encoding='utf-8') as f: for w in x[1]: f.write(w + "\n"))

def writecsv(datapath, csvwriter): 
    #ws = (set(), set(), set())
    for (root, dirs, files) in walk(datapath):
        #print(root)
        #if(root != datapath):
        #tfs = sc.wholeTextFiles(root).cache()
        #tfs.foreach(process_file)
        for file in files:
            #print(file)
            pf = process_file((file, open(join(root, file), encoding="utf-8").read()), csvwriter=csvwriter)
            #for i in range(0,3):
            #    ws[i].update(pf[i])
    """
    for n in ws[0]: csvwriter.write_n(n)
    for a in ws[1]: csvwriter.write_a(a)
    for v in ws[2]: csvwriter.write_v(v)

    csvwriter.close(True)
    """

def write_verticals(sc, article_folder):
    ws = writecsv_spark(sc, article_folder[0])
    for af in article_folder[1:]:
        ws = writecsv_spark(sc, af, ws)

    save_words_spark(ws)