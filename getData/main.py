from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split
from operator import add
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, MapType, StringType
from collections import defaultdict
import csv
import re 
from string import punctuation
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA
from pyspark.ml.clustering import LocalLDAModel
import codecs
from pyspark.ml.linalg import DenseVector
   
def main():
    spark = SparkSession \
        .builder \
        .appName("Reddit Site:Get Data") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
 
    file="file:////l2/corpora/reddit/submissions/RS_2015-12.bz2"
    output=file[-14:-3]

    sc = spark.sparkContext
    print('\n\n\n starting read and filter')
    df = filterPosts(file,sc,spark)
 
    df= convertToVec(df, sc, spark, output, inputCol='tokens')

    num_topics=10
    
    print('\n\n\n LDA... \n\n\n')
    newLDA=False
    if newLDA:
        lda=LDA(featuresCol='vectors', k=num_topics, maxIter=50)
        lda_model=lda.fit(df.select('id','vectors'))
        lda_model.save(output+'_ldamodel')
    else:
        lda_model=LocalLDAModel.load(output+'_ldamodel')

    print('\n\n\n Describe Topics... \n\n\n')
    topic_indices=lda_model.describeTopics(maxTermsPerTopic=50)
    topic_indices.write.json(output+'_topics.json', mode='overwrite')
    

    print('\n\n\n reduce to subs\n\n\n')
    #subDF=df.select('subreddit','vectors').groupBy(df.subreddit).sum('vectors')
    subDF=df.select('subreddit','vectors').rdd.mapValues(lambda v: v.toArray()) \
        .reduceByKey(lambda x, y: x + y) \
        .mapValues(lambda x: DenseVector(x)) \
        .toDF(["subreddit", "vectors"])
        
    '''
    print('\n\n\n LDA... \n\n\n')

    lda=LDA(featuresCol='vectors', k=num_topics, maxIter=50)
    lda_model=lda.fit(subDF.select('subreddit','vectors'))
    
    print('\n\n\n Describe Topics... \n\n\n')
    topic_indices=lda_model.describeTopics(maxTermsPerTopic=50)
    topic_indices.write.json(output+'_topics.json', mode='overwrite')
    '''
    print('\n\n\n Transform DataSet \n\n\n')
    subDF=lda_model.transform(subDF)
    #topicDF=lda_model.transform(vecDF)
    subDF.write.json(output+'_transformed.json', mode='overwrite')

def tokenize(s):
    tokens=[]
    s=s.strip().lower()
    wordlist=re.split("[\s;,#]", s)
    for word in wordlist: 
        word=re.sub('^[\W\d]*','',word)
        word=re.sub('[\W\d]*$','',word)
        if word != '':
            tokens.append(word)
    return tokens

def filterPosts(filename, sc, ss, subs=set(), minwords='100'):
    tokensUDF = udf(tokenize, ArrayType(StringType()))
    alldata = ss.read.json(filename)
    if subs!=set():
        alldata=alldata.filter(alldata.subreddit.isin(subs))

    filtered= alldata \
        .filter(alldata['is_self'] == True) 	\
        .select('id','subreddit',tokensUDF('selftext').alias('tokens'))	\
        .withColumn('wordcount', size('tokens'))	\
        .filter('wordcount >='+minwords)
    return filtered

def convertToVec(df, sc, ss, outputName, inputCol='tokens'):
    print('\n\n\n Removing Stopwords... \n\n\n')
    remover=StopWordsRemover(inputCol=inputCol, outputCol='nostops', stopWords=StopWordsRemover.loadDefaultStopWords('english'))
    df=remover.transform(df)

    cv=CountVectorizer(inputCol='nostops', outputCol='vectors',minTF=1.0)
    vecModel=cv.fit(df)
    new=False
    if new:
        print('\n\n\n Get Vocab... \n\n\n')
        inv_voc=vecModel.vocabulary 
        f = codecs.open(outputName+'_vocab.txt', encoding='utf-8', mode='w')
        for item in inv_voc:
            f.write(u'{0}\n'.format(item))
        f.close()
    vectors= vecModel.transform(df).select('id','subreddit','vectors')
    return vectors

if __name__ == "__main__":
    main()
