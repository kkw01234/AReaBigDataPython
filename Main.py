from konlpy.tag import Kkma, Okt, Mecab
from pyspark.sql import SparkSession
from konlpy.utils import pprint
import Restaurant
import TopWordCloud
import re
import os
import json
import nltk
import traceback
position = ["Noun", "ProperNoun", "Foreign", "Alpha"]
i = 0
okt = Okt()
def strip_e(st):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    return RE_EMOJI.sub(r'', st)


def tokenize(doc):
    # norm은 정규화, stem은 근어로 표시하기를 나타냄
    return ['/'.join(t) for t in okt.pos(doc, norm=True, stem=True)]


def load_spark(table, place_id):
    global i
    spark_session = SparkSession.builder.appName(place_id).master("local[*]") \
        .config("spark.mongodb.output.uri", "mongodb://118.220.3.71/crawling")\
        .config("spark.mongodb.output.collection", table)\
        .config("spark.mongodb.input.uri", "mongodb://118.220.3.71/crawling")\
        .config("spark.mongodb.input.collection", table) \
        .config("spark.local.dir", "D:/spark-temp") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
        .getOrCreate()

    df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").load()
    df.printSchema()
    table_name = table+str(i)
    df.createOrReplaceTempView(table_name)
    result = spark_session.sql("SELECT * FROM "+table_name+" Where place_id = '" + place_id + "'").rdd

    seq_list = []
    rest_list = []

    for r in result.collect():
        rest = Restaurant.Restaurant(r['_id'], r['name'], r['date'], r['r_addr'], r['r_lat'], r['r_lng'], r['r_name'], strip_e(r['comment']))
        rest_list.append(rest)
        pos = okt.pos(strip_e(r['comment']).replace("#", " ").replace("\n", " "),norm=
                      True, stem=True)
        seq_list.append(pos)
        #print(rest)
    #print(seq_list)

    return spark_session, seq_list, rest_list


def lot_of_keywords(spark_session, seq_list):
    global i
    count_list = []
    for seq in seq_list:
       for p in seq:
            for pos in position:
                if p[1] == pos:
                    count_list.append({"word": p[0], "pos": p[1]})
    df = spark_session.createDataFrame(count_list)
    df.printSchema()
    table_name = "position"+str(i)
    i += 1
    df.createOrReplaceTempView(table_name)
    result = spark_session.sql("SELECT word, count(*) as pos FROM "+table_name+" GROUP BY word ORDER BY COUNT(*) DESC").limit(30).rdd.collect()
    result_list = [(row['word'], row['pos']) for row in result]

    print(result_list)
    return result_list


train_data = []
load_doc = False
selected_words = []


def what(doc):
    global load_doc
    global selected_words
    try:
        if not load_doc:
            if os.path.isfile('train_docs.json'):
                print("존재합니다")
                with open('train_docs.json', encoding="utf-8") as f:
                    train_docs = json.load(f)
            tokens = [t for d in train_docs for t in d[0]]
            print('토큰 수 :', len(tokens))
            text = nltk.Text(tokens, name='NMSC')
            selected_words = [f[0] for f in text.vocab().most_common(10000)]
            load_doc = True
    except FileNotFoundError:
        traceback.print_exc()
    return [doc.count(word) for word in selected_words]


def sentiment_analysis(rest_list):
    from keras.models import load_model
    import numpy as np
    model = load_model('./saved_model.h5')
    all_score = 0

    for rest in rest_list:
        token = tokenize(strip_e(rest.get_comment()).replace("#", " "))
        tf = what(token)
        data = np.expand_dims(np.asarray(tf).astype('float32'), axis=0)
        score = float(model.predict(data))
        all_score += score
        print(rest.get_comment(), ':', score * 100, '점')
    return all_score / len(rest_list)



def load_bigdata(place_id):

    spark_session, seq_list, rest_list = load_spark("trips", place_id)
    result = lot_of_keywords(spark_session, seq_list)
    TopWordCloud.make_word_cloud(result, place_id)
    score = sentiment_analysis(rest_list)

    try:
        spark_session.stop()
    except:
        pass
    return score*100