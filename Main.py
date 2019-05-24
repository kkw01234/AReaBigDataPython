from konlpy.tag import Kkma, Okt, Mecab
from pyspark.sql import SparkSession
from konlpy.utils import pprint
import Restaurant
import re
position = ["Noun", "ProperNoun", "Foreign", "Alpha"]

def strip_e(st):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    return RE_EMOJI.sub(r'', st)

def load_spark(table):
    query = "감성타코 강남역"
    spark_Session = SparkSession.builder.appName("test").master("local") \
        .config("spark.mongodb.output.uri", "mongodb://118.220.3.71/crawling")\
        .config("spark.mongodb.output.collection", "instagram")\
        .config("spark.mongodb.input.uri", "mongodb://118.220.3.71/crawling")\
        .config("spark.mongodb.input.collection", "instagram") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
        .getOrCreate()

    df = spark_Session.read.format("com.mongodb.spark.sql.DefaultSource").load()
    df.printSchema()
    df.createOrReplaceTempView(table)
    result = spark_Session.sql("SELECT * FROM "+table+" Where r_name = '" + query + "'").rdd

    okt = Okt()

    seq_list = []
    rest_list = []
    for r in result.collect():
        rest = [r['_id'], r['name'], r['date'], r['r_addr'], r['r_lat'], r['r_lng'], r['r_name'], strip_e(r['comment'])]
        rest_list.append(rest)
        pos = okt.pos(strip_e(r['comment']).replace("#", " ").replace("\n"," "))
        seq_list.append(pos)
        #print(rest)
    #print(seq_list)
    return spark_Session, seq_list, rest_list


def sentiment(spark_Session, seq_list):
    count_list = []
    for seq in seq_list:
       for p in seq:
            for pos in position:
                if p[1] == pos:
                    count_list.append({"word": p[0],"pos": p[1]})
    df = spark_Session.createDataFrame(count_list)
    df.printSchema()
    df.createOrReplaceTempView("position")
    result = spark_Session.sql("SELECT word, count(*) FROM position GROUP BY word ORDER BY COUNT(*) DESC").limit(20).rdd.collect()

    for r in result:
        print(r)
    return result



if __name__=="__main__":
    spark_Session, seq_list, rest_list = load_spark("instagram")
    sentiment(spark_Session, seq_list)

    spark_Session.stop()