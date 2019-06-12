import pymongo
from pyspark.sql import SparkSession
import pandas as pd
from gensim.models import Word2Vec
import pymysql
from konlpy.tag import Okt
import re

table_list = ['rest_mango_plate', 'rest_dining_code', 'rest_trip_advisor',
                  'dining_code', ' mango_plate', 'trips', 'rest_instagram']


# 자신이 댓글쓴거
def load_my_comment(u_id, positive, negative):
    try:
        conn = pymysql.connect(host='118.220.3.71', user='root', password='rjsdnrkkw4809!!', db='area', charset='utf8')
        curs = conn.cursor()
        # sql = "SELECT restName, reviewRate FROM review r, restaurant re WHERE r.reviewgoogleid =re.restgoogleid and reviewUserId='" +u_id + "'"
        sql = "SELECT reviewgoogleid, reviewRate FROM review r WHERE reviewUserId='" +u_id + "'"
        curs.execute(sql)
        rows = curs.fetchall()
        if len(rows) > 0:
           for row in rows:
                rate = row[1]
                if rate >= 3.5:
                    positive.append(row[0].replace(" ", ""))
                elif rate <= 2:
                    negative.append(row[0].replace(" ", ""))

    except:
        import traceback
        traceback.print_exc()
        pass
    return positive, negative


# 찜하기
def load_my_like(u_id, positive, negative):
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn['db']
    likes = db['likes'].find({'userId': u_id})
    db = conn['crawling']

    for like in likes:
        positive.append(like['restId'])


# 모든 음식점의 리뷰들 모음
def restaurant_data_load():
    global table_list
    rest_list = []
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn['crawling']
    for table in table_list: # Mongodb에 있는 정보
        reviews = db[table].find()
        for review in reviews:
            if review['rate'] <= 3:
                continue
            rest = {
                'place_id': review['place_id'],
                'name': review['name'],
                'r_name': review['r_name'],
                'rate': review['rate'],
                'date': review['date']
            }
            if review['rate'] >= 5:
                rest_list.append(rest)
            rest_list.append(rest)
    conn.close()

    conn = pymysql.connect(host='118.220.3.71', user='root', password='rjsdnrkkw4809!!', db='area', charset='utf8')
    curs = conn.cursor()
    sql = "Select r.reviewgoogleid, r.reviewUserId, re.restname, r.reviewRate, r.reviewdate FROM review r, restaurant re WHERE r.reviewgoogleid=re.restgoogleid"
    curs.execute(sql)
    rows =curs.fetchall()
    if len(rows) >= 1:
        for row in rows:
            if row[3] <= 3:
                continue
            rest = {
                'place_id': row[0],
                'name': row[1],
                'r_name': row[2],
                'rate': row[3],
                'date': row[4]
            }
            if review['rate'] >= 5:
                rest_list.append(rest)
            rest_list.append(rest)
    curs.close()
    conn.close()

    return rest_list


# 모든 음식점의 리뷰들을 모아서 임베딩 작업 진행
def restaurant_embedding(rest_list, test1=''):
    global table_list

    corpus = []
    df = pd.DataFrame(rest_list)
    grouped = df.groupby('name')
    for place_id, either in grouped:
        corpus.append(either.sort_values(['rate'])['place_id'].values.tolist())
    print(len(corpus))
    model = Word2Vec(corpus[:], size=200, window=5, min_count=3, workers=4, sg=1)
    model.save('similar_analysis'+test1+'.h5')


# 분석
def similar_analysis(test1=''):
    rest_list = restaurant_data_load()
    restaurant_embedding(rest_list, test1)


def propensity_restaurant(u_id, place_id=None):
    positive = []  # 찜하기 와 긍정적인 명단

    negative = []  # 부정적인 음식점 명단
    load_my_comment(u_id, positive, negative)
    load_my_like(u_id, positive, negative)
    model = Word2Vec.load('similar_analysis.h5')
    positive_set = set(positive)
    positive = list(positive_set)
    while True:
        try:
            if positive is []:
                result = 0
                break
            result = model.wv.n_similarity([place_id], positive)
            break
        except KeyError as k:
            s = str(k.args[0])
            s = s.split(" ")
            s = s[1].replace("'", "")
            try:
                positive.remove(s)
            except:
                print('not Found')
                result = 0
                break
    return result


if __name__ =='__main__':
    similar_analysis()  # place_id = default
    import test
    test.analysis('similar_analysisr_name.h5')