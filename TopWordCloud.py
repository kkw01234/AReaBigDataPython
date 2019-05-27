import matplotlib.pyplot as plt
from wordcloud import WordCloud
font_path = 'C:/Users/gny32/PycharmProjects/spark/BMDOHYEON_ttf.ttf'


def make_word_cloud(top_list, keyword):
    wc = WordCloud(font_path=font_path, background_color='white', width=800, height=600)
    cloud = wc.generate_from_frequencies(dict(top_list))
    plt.figure(figsize=(10, 8))
    plt.axis('off')
    plt.imshow(cloud)
    plt.savefig('D:/upload_file/word_cloud/'+keyword+'.png')
