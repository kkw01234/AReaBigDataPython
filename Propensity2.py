import pymongo
from pyspark.sql import SparkSession
import pandas as pd
from gensim.models import Word2Vec
import pymysql
from konlpy.tag import Okt
import re

table_list = ['rest_mango_plate', 'rest_dining_code', 'rest_trip_advisor',
                  'dining_code', ' mango_plate', 'trips', 'rest_instagram']

positive = []
negative = []

rest_list = []

def strip_e(st):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    return RE_EMOJI.sub(r'', st)


def load_pymysql(u_id): # 자신이 댓글쓴거
    try:
        conn = pymysql.connect(host='118.220.3.71', user='root', password='rjsdnrkkw4809!!', db='area', charset='utf8')
        curs = conn.cursor()
        sql = "SELECT restName, reviewRate FROM review r, restaurant re WHERE r.reviewgoogleid =re.restgoogleid and reviewUserId='" +u_id + "'"
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


def load2_pymysql(u_id):  # 미완성
    global rest_list
    try:
        conn = pymysql.connect(host='118.220.3.71', user='root', password='rjsdnrkkw4809!!', db='area', charset='utf8')
        curs = conn.cursor()
        sql = "SELECT restgoogleid, reviewUserId, restName, reviewContent, reviewRate FROM review r, restaurant re WHERE r.reviewgoogleid =re.restgoogleid and reviewUserId='" +u_id + "'"
        curs.execute(sql)
        rows = curs.fetchall()
        if len(rows) > 0:
           for row in rows:
               rest = {
                   'place_id': row[0],
                   'name': row[1],
                   'r_name': row[2],
                   'rate': row[3],
                   'date': row[4]
               }
               rest_list.append(rest)
    except:
        import traceback
        traceback.print_exc()
        pass


def load_like(u_id):
    global train_data
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn['db']
    likes = db['likes'].find({'userId': u_id})
    db = conn['crawling']

    for like in likes:
        positive.append(like['restId'])


def restaurant_data_load():
    global table_list
    global rest_list
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn['crawling']
    for table in table_list:
        reviews = db[table].find()
        for review in reviews:
            if review['rate'] < 3:
                continue
            # print(Okt().pos(strip_e(review['comment']).replace('#', ' '))[0][0])
            pos = [t[0] for t in Okt().pos(strip_e(review['comment']).replace('#', ' '), norm=True, stem=True) if t[1] == 'Noun']
            for p in pos:
                rest = {
                    'place_id': review['place_id'],
                    'name': review['name'],
                    'r_name': review['r_name'],
                    'comment': p,
                    'rate': review['rate'],
                    'date': review['date']
                }
                print(rest)
                rest_list.append(rest)

    corpus = []
    df = pd.DataFrame(rest_list)
    grouped = df.groupby('place_id')

    for place_id, either in grouped:
        print(either.sort_values(['rate'])['comment'].values.tolist())
        corpus.append(either.sort_values(['rate'])['comment'].values.tolist())
    print(corpus)
    model = Word2Vec(corpus[:], size=200, window=5, min_count=3, workers=4, sg=1)
    model.save('Word2Vec')
    # a = model.wv.most_similar(positive=positive, negative=negative)

    b = model.wv.wmdistance(['양고기'], ['양꼬치', '맛있다'])

    # print(a)
    print(positive)
    print(b)

#load_pymysql('admin')
# load_like('admin')
#restaurant_data_load()
model2 = Word2Vec.load('./Word2Vec')
a = model2.wv.distance('양고기', '칭따오')
b = model2.wv.wmdistance(['양고기', '칭따오'], ['햄버거', '피자', '라면'])
print(a)
print(b)
