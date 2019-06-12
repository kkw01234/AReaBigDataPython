from konlpy.tag import Okt
from keras import models
from keras import layers
from keras import optimizers
from keras import losses
from keras import metrics

import numpy as np
import pymongo
import re
import os
import json
import nltk

reviews_data = ['reviews', 'trips', 'dining_code', 'mango_plate', 'rest_mango_plate', 'rest_dining_code',
                'rest_trip_advisor']



def deep_learning():
    global reviews_data
    train_data = []
    def load_pymongo():
        conn = pymongo.MongoClient('118.220.3.71', 27017)
        db = conn['crawling']
        for re in reviews_data:
            reviews = db[re].find()
            for review in reviews:
                train_data.append([review['comment'].replace('\n', ' '), str(review['rate'])])
        conn.close()

    load_pymongo()

    def strip_e(st):
        RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        return RE_EMOJI.sub(r'', st)

    def tokenize(doc):
        # norm은 정규화, stem은 근어로 표시하기를 나타냄
        return ['/'.join(t) for t in okt.pos(doc, norm=True, stem=True)]

    okt = Okt()
    for row in train_data:
        row[0] = strip_e(row[0]).replace('#', ' ')

    if os.path.isfile('train_docs2.json'):
        with open('train_docs2.json', encoding="utf-8") as f:
            train_docs = json.load(f)
    else:
        train_docs = [(tokenize(row[0]), row[1]) for row in train_data]
        print(train_docs)
        with open('train_docs.json', 'w', encoding="utf-8") as make_file:
            json.dump(train_docs, make_file, ensure_ascii=False, indent="\t")

    tokens = [t for d in train_docs for t in d[0]]
    print('토큰 수 :', len(tokens))

    text = nltk.Text(tokens, name='NMSC')
    selected_words = [f[0] for f in text.vocab().most_common(10000)]

    def term_frequency(doc):
        return [doc.count(word) for word in selected_words]

    train_x = [term_frequency(d) for d, _ in train_docs]
    train_y = [1 if float(c) >= 5 else 0 if float(c) <= 3 else 0.5 for _, c in train_docs]
    # train_y = [0 if float(c) < 1 else (float(c)-1)/4 for _, c in train_docs]
    x_train = np.asarray(train_x).astype('float32')
    y_train = np.asarray(train_y).astype('float32')

    model = models.Sequential()
    model.add(layers.Dense(64, activation='relu', input_shape=(10000,)))
    model.add(layers.Dense(64, activation='relu'))
    model.add(layers.Dense(1, activation='sigmoid'))

    model.compile(optimizer=optimizers.RMSprop(lr=0.001),
                  loss=losses.binary_crossentropy,
                  metrics=[metrics.binary_accuracy])

    model.fit(x_train, y_train, epochs=10, batch_size=1024)  # 처음 epochs : 10 BS : 512

    model.save('saved_model.h5')  # 모델 저장

    # model = load_model('mnist_mlp_model.h5') 모델 불러오기, import 필요

    def predict_pos_neg(review):
        token = tokenize(review)
        tf = term_frequency(token)
        data = np.expand_dims(np.asarray(tf).astype('float32'), axis=0)
        score = float(model.predict(data))
        print(review, ':', score * 100, '점')

    predict_pos_neg('정말 맛있는 집이였어요. 제 인생 최고의 맛집')
    predict_pos_neg('먹다가 토나올 뻔;; 오바잖아 이건')
    predict_pos_neg('숨은 맛집 인정?')
    predict_pos_neg('아는 사람은 다안다 여기는')
    predict_pos_neg('후회한다. 정말 후회한다. 여길 들어온 것을')
    predict_pos_neg('이거 모르면 간첩')
    predict_pos_neg('존맛탱구리')
    predict_pos_neg(' ')


if __name__ =='__main__':
    deep_learning()
