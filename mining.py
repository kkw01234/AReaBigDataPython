from konlpy.tag import Okt
from gensim.models import Word2Vec

import numpy as np
import pymongo
import re
import os
import json
import pymysql
import nltk

train_data = []
# 그사림이 남긴 글, 검색
restaurant_data = []


def load_pymongo(google_id):
    global restaurant_data
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn['crawling']
    mosts = db['most_common_place'].find({'place_id': google_id})
    for most in mosts:
        restaurant_data.append((most['token'].replace('\n', ' '), str(most['count'])))
    conn.close()


def load_pymysql(u_id): #자신이 댓글쓴거
    global train_data
    try:
        conn = pymysql.connect(host='118.220.3.71', user='root', password='rjsdnrkkw4809!!', db='area', charset='utf8')
        curs = conn.cursor()
        sql = "SELECT reviewgoogleid, reviewContent, reviewRate FROM review WHERE reviewUserId='" +u_id + "'"
        curs.execute(sql)
        rows = curs.fetchall()
        if len(rows) > 0:
           for row in rows:
            id = row[0]
            content = row[1]
            rate = int(row[2] - 2)
            if rate > 0:
                pos = Okt().pos(strip_e(content).replace("#", " ").replace("\n", " "), norm=True, stem=True)
                for p in pos:
                    if p[1] in ['Noun']:
                        for i in range(0, rate):
                            train_data.append(p[0])
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
    for like in likes :
        rests = db['most_common_place'].find({'place_id': like['restId']})
        for rest in rests:
            for i in range(0, rest['count']):
                train_data.append(rest['token'])

#load_pymongo()


def strip_e(st):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    return RE_EMOJI.sub(r'', st)


def tokenize(doc):
    # norm은 정규화, stem은 근어로 표시하기를 나타냄
    token_list = Okt().pos(doc, stem=True, norm=True)
    temp = []
    for word in token_list:
        if word[1] in ["Noun"]:
            temp.append(word[0])
    return temp


def similarity():
    for row in train_data:
        row[0] = strip_e(row[0]).replace('#', ' ')

    if os.path.isfile('train_docs2.json'): #저장되지 않음
        with open('train_docs2.json', encoding="utf-8") as f:
            train_docs = json.load(f)
    else:
        train_docs = [tokenize(row[0]) for row in train_data]
        print(train_docs)
        with open('train_docs.json', 'w', encoding="utf-8") as make_file:
            json.dump(train_docs, make_file, ensure_ascii=False, indent="\t")


    print('토큰 수 :', len(train_docs))

def study(r_list):
    model = Word2Vec(r_list, size=100, window=5, min_count=5, workers=4, sg=0)
    a = model.wv.most_similar("맛집")
    print(a)



# -------------test-----------

if __name__ =='__main__':
    load_pymongo('ChIJa3UOgEpaezURFl9HM1drLDs')
    load_pymysql('admin')
    load_like('admin')
    print(train_data)
    print(restaurant_data)
    pass





