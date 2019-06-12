from konlpy.tag import Kkma, Okt, Mecab
from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from konlpy.utils import pprint
import Restaurant
import TopWordCloud
import re
import os
import threading
import json
import nltk
import traceback
import pymongo
import datetime

def strip_e(st):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    return RE_EMOJI.sub(r'', st)


def tokenize(doc):
    # norm은 정규화, stem은 근어로 표시하기를 나타냄
    return ['/'.join(t) for t in Okt().pos(doc, norm=True, stem=True)]


selected_words = []
load_doc = False


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


model = None


def sentiment_analysis(rest_list):
    global model
    from keras.models import load_model
    import numpy as np
    if model is None:
        model = load_model('./saved_model.h5')
    else:
        pass
    for rest in rest_list:
        token = tokenize(strip_e(rest['comment']).replace("#", " "))
        tf = what(token)
        data = np.expand_dims(np.asarray(tf).astype('float32'), axis=0)
        score = round(float(model.predict(data))*5, 1)
        print(rest['comment'], ':', score, '점')
        update_rate(rest['place_id'], rest['r_name'], rest['name'], score)


def update_rate(place_id, r_name, name, rate):
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn.crawling
    db.rest_instagram.update({'place_id': place_id, 'r_name': r_name, 'name': name},
                                    {'$set': {'rate': rate}})


def update_instagram():
    star = []
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn.crawling
    reviews = db.rest_instagram.find()
    for review in reviews:
        if review['rate'] != 0 and review['rate'] <= 5:
            continue
        s = {
            'place_id': review['place_id'],
            'r_name': review['r_name'],
            'name': review['name'],
            'comment': review['comment'],
        }
        star.append(s)
    sentiment_analysis(star)

if __name__ =='__main__':
    update_instagram()