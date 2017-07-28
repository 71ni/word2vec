import sys
from imp import reload
#path='D:/project/adidas'
#sys.path.append(path)
import datetime
starttime=datetime.datetime.now()
import multi_read as mrd
reload(mrd)
from gensim import models

def senten(ct):
    sentences=[]
    for i in range(len(ct)):
        for j in range(len(ct[i])):
            sentences+=mrd.seperatewords(ct[i][j],mrd.stopwords)
    return sentences

sentences=senten(mrd.cchats)
model=models.Word2Vec(sentences,window=3)
model.save_word2vec_format('C:/Users/kate.qian/Documents/djcode/rule_relation/static/model.bin', binary=True)
endtime=datetime.datetime.now()
print(endtime-starttime)
