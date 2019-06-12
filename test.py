from sklearn.manifold import TSNE
import gensim.models as g
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc

font_location = 'C:/Windows/Fonts/HMFMPYUN.ttf'
font_name = font_manager.FontProperties(fname=font_location).get_name()
rc('font', family=font_name)

def analysis(model_name):
    model = g.Word2Vec.load(model_name)
    vocab = list(model.wv.vocab)
    X = model[vocab]
    tsne = TSNE(n_components=2)
    # 100개의 단어에 대해서만 시각화
    X_tsne = tsne.fit_transform(X[:200, :])
    df = pd.DataFrame(X_tsne, index=vocab[:200], columns=['x', 'y'])
    fig = plt.figure()
    fig.set_size_inches(40, 20)
    ax = fig.add_subplot(1, 1, 1)
    ax.scatter(df['x'], df['y'])

    for word, pos in df.iterrows():
        ax.annotate(word, pos, fontsize=30)
    plt.savefig("word2vec_200")
    plt.show()