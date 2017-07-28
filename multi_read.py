import sys
import codecs
import os
import jieba
import jieba.posseg as pseg
import pandas as pd
import numpy as np
import re
import unicodedata
import pymssql
import xlwt
from sqlalchemy import Column,String,create_engine
import pylab as pl
from operator import itemgetter
from collections import OrderedDict,Counter
import scipy
import scipy.cluster.hierarchy as sch
from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from collections import Counter
import xlwt
from gensim import models,corpora
import gensim
from scipy.linalg import svd
import datetime

starttime=datetime.datetime.now()
def getListFiles(path):
    assert os.path.isdir(path),'%s not exist.' % path
    ret=[]
    filenm=[]
    for root,dirs,files in os.walk(path):
        for filespath in files:
            try:
#                if re.search('^2017-04',filespath) and int(filespath[8:10])<=31:
                ret.append(os.path.join(root,filespath))
                filenm.append(filespath)
            except:
                print(filespath)
    return ret,filenm

pths,filename=getListFiles('D:/project/adidas/4月-5月Tmall聊天记录/4月-5月Tmall聊天记录')

delsent=open('D:/project/adidas/话术1.txt','r').read()
delsent=re.sub('\n','',delsent)

def findconsumer(txs):
    for i in range(len(txs)):
        if not re.search('adidas官方旗舰店',txs[i][1]):
            return txs[i][1]

def findstuff(txs):
    for i in range(len(txs)):
        if re.search('adidas官方旗舰店',txs[i][1]) and txs[i][1] !='adidas官方旗舰店':
            return txs[i][1]

def splitchat(txs):
    chats=[]
    ichat=[]
    cs=[]
    for i in range(len(txs)):
        if txs[i] !='' and len(re.findall(' ',txs[i]))>=3:
            s=re.split(' ',txs[i],3)
            ss=[ichat[n][1] for n in range(len(ichat)) if re.search('adidas官方旗舰店',ichat[n][1])]
            if len(ichat)>0 and len(ss)>0 and s[2][:len(s[2])-1] !=ss[0] and re.search('adidas官方旗舰店',s[2]) and s[2] !='adidas官方旗舰店' and ss[0] !='adidas官方旗舰店':
                if not re.search('adidas官方旗舰店',ichat[len(ichat)-1][1]):
                    chats.append(ichat[0:len(ichat)-1])
                    cs.append((s[0]+' '+s[1],findconsumer(ichat),findstuff(ichat)))
                    last=ichat[len(ichat)-1]
                    ichat=[]
                    ichat.append(last)
                else:
                    first=[]
                    chats.append(ichat)                    
                    cs.append((s[0]+' '+s[1],findconsumer(ichat),findstuff(ichat)))
                    for u in range(len(ichat)-1,-1,-1):
                        if not re.search('adidas官方旗舰店',ichat[u][1]):
                            first=ichat[u]
                            break
                    ichat=[]
                    if len(first)>0:
                        ichat.append(first)
            if len(s)==2:
                ichat.append([s[0]+' '+s[1],s[2][:len(s[2])-1],''])
            else:
                ichat.append([s[0]+' '+s[1],s[2][:len(s[2])-1],s[3]])
        elif len(txs[i]) ==10 and re.search('\d{4}-\d{2}-\d{2}',txs[i]):
            chats.append(ichat)
            cs.append((s[0]+' '+s[1],findconsumer(ichat),findstuff(ichat)))
            ichat=[]
    if len(ichat)>0:
        chats.append(ichat) #记录最后一段对话
        cs.append((s[0]+' '+s[1],findconsumer(ichat),findstuff(ichat)))
    return chats,cs        

    
def splitchat1(txs):
    chats=[]
    ichat=[]
    cs=[]
    for i in range(len(txs)):
        if txs[i] !='' and re.search('\(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\)',txs[i]):
            s=re.split('\(|\): ',txs[i],3)
            ss=[ichat[n][1] for n in range(len(ichat)) if re.search('adidas官方旗舰店',ichat[n][1])]
            if len(ichat)>0 and len(ss)>0 and s[0] !=ss[0] and re.search('adidas官方旗舰店',s[0]) and s[0] !='adidas官方旗舰店' and ss[0] !='adidas官方旗舰店':
                #对已读对话的最后一句的判断
                if not re.search('adidas官方旗舰店',ichat[len(ichat)-1][1]):
                    chats.append(ichat[0:len(ichat)-1])
                    cs.append((ichat[0][0][0:10],findconsumer(ichat),findstuff(ichat)))
                    last=ichat[len(ichat)-1]
                    ichat=[]
                    ichat.append(last)
                else:
                    chats.append(ichat)                    
                    cs.append((ichat[0][0][0:10],findconsumer(ichat),findstuff(ichat)))
                    for u in range(len(ichat)-1,-1,-1):
                        if not re.search('adidas官方旗舰店',ichat[u][1]):
                            first=ichat[u]
                            break
                    ichat=[]
                    try:
                        ichat.append(first)
                    except:
                        pass
            if len(s)==2:
                ichat.append([s[1],s[0],''])
            else:
                ichat.append([s[1],s[0],s[2]])
        elif re.search('-----',txs[i]):
            chats.append(ichat)
            cs.append((ichat[0][0][0:10],findconsumer(ichat),findstuff(ichat)))
            ichat=[]
        else:
            if len(ichat)>0:
                ichat[len(ichat)-1][2]+=txs[i]
    if len(ichat)>0:
        chats.append(ichat) #记录最后一段对话
        cs.append((ichat[0][0][0:10],findconsumer(ichat),findstuff(ichat)))
    return chats,cs


'''
        elif re.search('单聊',txt[3]):
            last_index=len(f)
            if re.search('\n\n================================================================\n                               群聊                             \n',f):
                last_index=f.index('\n\n================================================================\n                               群聊                             \n')
            first_index=f.index('\n\n\n')
            f=f[first_index+3:last_index]
            text=re.split('\n',f)
            textls+=text[1:len(text)]
            chats+=splitchat1(text[1:len(text)])[0]
            cs+=splitchat1(text[1:len(text)])[1]
            txtstyle.append(2)
        elif re.search('以下为一通会话',txt[0]):
            txtstyle.append(3)
        else:
            txtstyle.append(4)
            unreg.append((ip,txt[0:4]))
'''        
def single_split(ch):
    allsv=[]
    chs=[]
    ich=[]
    recent_sv=[]
    for i in range(len(ch)):
        if re.search('adidas官方旗舰店',ch[i][1]):
            if len(recent_sv)==0:
                recent_sv.append(ch[i][1])
                ich.append(ch[i])
            else:
                if len(set(recent_sv))==1 and ch[i][1] in recent_sv:
                    ich.append(ch[i])
                else:
                    cssc=[findconsumer(ich),tuple(list(set(recent_sv)))]
                    allsv.append(cssc)
                    chs.append(ich)
                    ich=[]
                    recent_sv=[]
                    recent_sv.append(ch[i][1])
                    if not re.search('adidas官方旗舰店',ch[i-1][1]):
                        ich.append(ch[i-1])                   
                    else:
                        for u in range(i-2,-1,-1):
                            if not re.search('adidas官方旗舰店',ch[u][1]):
                                ich.append(ch[u])
                                break
                    ich.append(ch[i])
        else:
            ich.append(ch[i])
    chs.append(ich)
    allsv.append([findconsumer(ich),tuple(list(set(recent_sv)))])
    return chs,allsv

#chats,cs=splitchat(textls)
            
def combine(ht,cs):
    ics=[]
    unics=[]
    ht1=[]
    for i in range(len(cs)):
        if cs[i][0] in unics:
            dupi1=unics.index(cs[i][0]) #dupi1是重复的cs[i]在已经判断过的chats中的位置
            alltext=re.sub('\n','',''.join([item[2] for item in ht1[dupi1]]))
            for k in range(len(ht[i])):
                if re.sub('\n','',ht[i][k][2]) not in alltext:
                    ht1[dupi1].append(ht[i][k])
            ht1[dupi1].sort(key=lambda x:x[0])
        else:
            ht1.append(ht[i])
            unics.append(cs[i][0])
    return ht1,unics

def combine1(ht,cs):
    ics=[]
    unics=[]
    ht1=[]
    for i in range(len(cs)):
        if cs[i] in unics:
            dupi1=unics.index(cs[i]) #dupi1是重复的cs[i]在已经判断过的chats中的位置
            alltext=re.sub('\n','',''.join([item[2] for item in ht1[dupi1]]))
            for k in range(len(ht[i])):
                if re.sub('\n','',ht[i][k][2]) not in alltext:
                    ht1[dupi1].append(ht[i][k])
        else:
            ht1.append(ht[i])
            unics.append(cs[i])
    return ht1,unics

def csonly(chats1):
    cchats=[]
    cdate=[]
    allchats=[]
    for i in range(len(chats1)):
        ichat=[]
        iachat=[]
        for j in range(len(chats1[i])):
            if not re.search('系统提示|系统提醒',chats1[i][j][2]):
                if not re.search('adidas官方旗舰店',chats1[i][j][1]):
                    ichat.append(re.sub('\n','',chats1[i][j][2]))
                iachat.append(re.sub('\n','',chats1[i][j][2]))
        cchats.append(ichat)
        allchats.append(iachat)
        try:
            cdate.append(chats1[i][0][0][0:10])
        except:
            print(i)
    return allchats,cdate,cchats

tbl=dict.fromkeys(i for i in range(sys.maxunicode) if unicodedata.category(chr(i)).startswith('P'))

def removes(text):
    r1='<recognition>.*\.amr|http[s]*://[A-z0-9-吗_./&%=;#?:]*|<.*?>|\s+|￥.*￥'
    r2=delsent
    text=re.sub(r2,'',text)
    text=re.sub(r1,'',text)
    text=text.translate(tbl)
    text=text.upper()
    return text

def read_stopwords():
    stopwords=open('D:/project/adidas/stopwords.txt','r')
    stopwords=stopwords.read()
    swds=stopwords.split('|')
    return swds

stopwords=read_stopwords()
def seperatewords(s,stopwords):      
    ic=[]
    nw=['小时']
#    nw=['不','没','没有','有','还有','什么','再','已','未','什么时候','是不是','有没有','无','可以','无法']
#    nw=['不','没','未','没有','还有']
    for w in jieba.cut(removes(s)):
        if w in nw and len(ic)>0:
            ic[-1]+=w
        elif w=='码的':
            ic.append('码')
        elif w not in ['，','。','！','：','’','“','”','‘','&',',','adidas','阿迪达斯'] and w not in stopwords:
            ic.append(w) 
    return ic


txtstyle=[]
unreg=[]
unread=[]
textls=[]
chats=[]
cs=[]
for ip in pths:
    try:
        f=open(ip,'r').read()
    except:
        try:
            f=open(ip,'r',encoding='utf-8').read()
        except:
            try:
                f=open(ip,'r',encoding='gb18030').read()
            except:
                unread.append(ip)
    txt=re.split('\n',f)
    if len(txt)>3:
        if re.search('即时消息',txt[2]):
            text=re.split('\n(?=\d{4}-\d{2}-\d{2})',f)
#            chats+=splitchat(text[2:len(text)])[0]
#            cs+=splitchat(text[2:len(text)])[1]
            textls+=text[2:len(text)]
            
            txtstyle.append(1)

chats,cs=splitchat(textls)
chats1,delc=combine(chats,cs)
chats2=[]
delc1=[]
for i in range(len(chats1)):
    iic,iia=single_split(chats1[i])
    chats2+=iic
    delc1+=iia
chats3,delcs=combine1(chats2,delc1)

allchats,cdate,cchats=csonly(chats3)
'''
sw=[]
for i in range(len(cchats)):
    for j in range(len(cchats[i])):
        sw.append(seperatewords(cchats[i][j],stopwords))
'''
senw=[]
for i in range(len(cchats)):
    isenw=[]
    for j in range(len(cchats[i])):
        isenw+=seperatewords(cchats[i][j],stopwords)
    senw.append(isenw)

'''
import FP_GROWTH
freqitems=FP_GROWTH.fpGrowth(senw,minSup=10)

f=open('D:/project/adidas/fp_freqitems.txt','w')
for i in range(len(freqitems)):
    if len(freqitems[i])>1:
        for w in freqitems[i]:
            f.write(w)
            f.write('\t')
        f.write('\n')
f.close()
'''
endtime=datetime.datetime.now()
print(endtime-starttime)
