from pyspark.mllib.feature import Word2Vec
from pyspark.ml.clustering import LDA
from .config import store_object

#should be sentences and not paragraphs
#inp = sc.parallelize(words)#.map(lambda row: row.remove("\n").split(" "))

# Input data: Each row is a bag of words from a sentence or document.
#documentDF = sqlContext.createDataFrame(words, ["text"])
#word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result").setVecotr

# Learn a mapping from words to Vectors.
#k .. dimensionality of the word vectors - the higher the better (default value is 100) (needs memory)
def getWord2Vec(corpus, k=100, mincount=20, windowsize=10, learningrate=0.05):
    word2vec = Word2Vec().setVectorSize(k).setMinCount(20).setWindowSize(10).setLearningRate(0.05)
    model = word2vec.fit(corpus)

    return model

def findAnalogies(s, model, out_file=None, amount=50):
    qry = model.transform(s[0]) - model.transform(s[1]) - model.transform(s[2])
    res = model.findSynonyms((-1)*qry, amount)
    #res = [x[0] for x in res]

    res = filter(lambda el: el[0] not in s, res)
    res = list(res)[1:]
    store_object(res, "_".join(s), out_file)

    return res

def findSynonyms(s, model, out_file=None, amount=50):
    res = list(model.findSynonyms(s, amount))
    store_object(res, s, out_file)

    return res


def getLDA(count_vectors, k=10, optimizer="em", maxIter=20):
    #Set LDA params
    #lda = LDA().setK(k).setMaxIterations(3).setDocConcentration(-1).setTopicConcentration(-1)
    #ldaModel = lda.run(vec_rdd)
    lda = LDA(k=k, optimizer=optimizer, seed=1, maxIter=maxIter)
    ldaModel = lda.fit(count_vectors)

    # Cluster the documents into three topics using LDA
    #ldaModel = LDA.train(vec_rdd, k=3)

    # Output topics. Each is a distribution over words (matching word count vectors)
    print("Learned topics (as distributions over vocab of " + str(ldaModel.vocabSize())
          + " words):")
    #ldaModel.describeTopics().show()

    ll = ldaModel.logLikelihood(count_vectors)
    lp = ldaModel.logPerplexity(count_vectors)
    print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
    print("The upper bound bound on perplexity: " + str(lp))

    # Describe topics.
    #topics = ldaModel.describeTopics(k)
    #print("The topics described by their top-weighted terms:")
    #topics.show(truncate=False)

    # Shows the result
    #transformed = ldaModel.transform(countVectors)
    #transformed.show(truncate=False)

    #topics = ldaModel.topicsMatrix()
    #for topic in range(3):
    #    print("Topic " + str(topic) + " " + str(len(topics)) + ":")
    #    for word in range(0, ldaModel.vocabSize()):
    #        print(" " + str(topics[word][topic]))
    return ldaModel

def render_topics(ldaModel, vocabArray, out_file=None, maxTermsPerTopic=20):
    topics = ldaModel.topicsMatrix()

    topics_ = ldaModel.describeTopics(maxTermsPerTopic=maxTermsPerTopic)
    print(topics_)
 
    topicIndices = topics_.select("termIndices", "termWeights").rdd#sc.parallelize(topics_)

    def topic_render(topic):  # specify vector id of words to actual words
        terms = topic[0]
        vals = topic[1]
        result = []
        for i in range(maxTermsPerTopic):
            term = vocabArray[terms[i]]
            result.append((term, vals[i]))
        return result

    topics_final = topicIndices.map(lambda topic: topic_render(topic)).collect()

    store_object(topics_final, "lda_" + str(maxTermsPerTopic) + "_" + str(len(vocabArray)), out_file)

    for topic in range(len(topics_final)):
        print("\ntopic " + str(topic) + ":\n")
        for (term, val) in topics_final[topic]:
            print (term + " " + str(val))
