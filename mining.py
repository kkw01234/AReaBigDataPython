from konlpy.tag import Okt
from gensim.models import Word2Vec

import numpy as np
import pymongo
import re
import os
import json
import nltk

train_data = []
def load_pymongo():
    global train_data
    conn = pymongo.MongoClient('118.220.3.71', 27017)
    db = conn['crawling']
    reviews = db['reviews'].find()
    for review in reviews:
        train_data.append([review['comment'].replace('\n', ' '), str(review['rate'])])
    trips = db['trips'].find()
    for trip in trips:
        train_data.append([trip['comment'].replace('\n', ' '), str(trip['rate'])])
    conn.close()


load_pymongo()


def strip_e(st):
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    return RE_EMOJI.sub(r'', st)

okt = Okt()


def tokenize(doc):
    # norm은 정규화, stem은 근어로 표시하기를 나타냄
    token_list =okt.pos(doc, stem=True, norm=True)
    temp = []
    for word in token_list:
        if word[1] in ["Noun"]:
            temp.append(word[0])
    return temp


for row in train_data:
    row[0] = strip_e(row[0]).replace('#', ' ')

if os.path.isfile('train_docs2.json'):
    with open('train_docs2.json', encoding="utf-8") as f:
        train_docs = json.load(f)
else:
    train_docs = [tokenize(row[0]) for row in train_data]
    print(train_docs)
    with open('train_docs.json', 'w', encoding="utf-8") as make_file:
        json.dump(train_docs, make_file, ensure_ascii=False, indent="\t")


print('토큰 수 :', len(train_docs))


model = Word2Vec(train_docs, size=100,window=5,min_count=5, workers=4, sg=0)
a=model.wv.most_similar("맛집")

print(a)


