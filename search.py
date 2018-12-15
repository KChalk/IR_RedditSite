from flask import Flask

from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for)
from collections import deque
import plyvel
import json
import pandas as pd

app = Flask(__name__)

def makedb():
    #needed info: is reddit in database, prefix iterator, reddit topics, topic words (convert from ints), topic docs
    # reading files..
    fast = False
    if fast:
        db = plyvel.DB("Database", create_if_missing=True)
        db.put(b'gaming',b'g')
        return db

    topicfile= '15-12_topics.json'
    subfile= '15-12_transformed.json'  #'small_transformed.json'
    vocabfile=  'RS_2015-12._vocab.txt'

    ## READING SUB TOPICS
    old_subs_pd=pd.read_json(subfile, lines=True)
    old_subs_pd=old_subs_pd.iloc[:10,:] #comment for full set

    subs_pd=old_subs_pd.copy()
    for i, row in subs_pd.iterrows():
        sub=row[0]
        topics=row[1]['values']
        temp_list=[]

        for j in range(len(topics)):
            temp_list.append(topics[j])
        subs_pd['t_'+str(i)]=temp_list
    subs_pd=subs_pd.drop('topicDistribution', axis=1)

    ## READING VOCAB
    vocablist=[]
    with open(vocabfile,encoding='utf-8') as v: 
        for line in v:
            vocablist.append(line[:-1])

    ##READING TOPICS VOCAB and TOPIC SUBS
    topic_pd=pd.read_json(topicfile, lines=True)

    metalist=[]
    for i, topic in topic_pd.iterrows():
        ind=topic[0]
        val=topic[1]
        topicNum=topic[2]
        words=[]
        for j in range(len(ind)):
            words.append(vocablist[ind[j]])
        metalist.append(words)
    topic_pd['words']=metalist
    
    metalist=[]
    for i in range(10):
        top10=subs_pd.nlargest(10,'t_'+str(i)).set_index('subreddit')
        metalist.append(top10.index.tolist())

    topic_pd['Top 10 reddits']=metalist

    db=plyvel.DB("Database", create_if_missing=True)
    wb=db.write_batch()


    for _,subreddit in subs_pd.iterrows():
        t_res=list(subreddit.iloc[1:])
        wb.put(bytes(name,'ascii'), i.to_bytes(2,byteorder='big'))

    # needed info: is reddit in database, prefix iterator, reddit topics,
    # topic subs
    for t,topic in topic_pd.iterrows():
        words=topic['words']
        weights=topic['termWeights']
        name=b'topic: '+t.to_bytes(2,byteorder='big')+b'words'

        pairs=zip(words, weights)
        wb.put(bytes(name,'ascii'), pairs)

        subs=topic['Top 10 reddits']
        wb.put(bytes(name,'ascii'), subs)

    wb.write()
    return db

@app.route('/')
def home():
    return redirect(url_for('simple'))

@app.route("/simple",methods=('GET', 'POST'))
def simple():
    #Initialize db
    if 'db' not in g:
        g.db=makedb()

    #manage inputs
    error=''
    if request.method=="POST":

        query=request.form['r']
        prefix=request.form['prefix']

        # search for full subreddit name
        if query != '':
            r=g.db.get(bytes(query,'ascii'),default=b'0')
            if r == b'0':
                error= 'Query "{}"does not match any subreddit. Try the prefix searcher?'.format(query)
                return render_template('/simple.html',p_res='', query='',error=error)
            #otherwise return /r/ page
            return redirect(url_for('subpage', subname=query))

        #search for sub by prefix
        if len(prefix)>2:
            results_it=g.db.iterator(prefix=bytes(prefix,'ascii'))
            results=[]
            for r in results_it:
                results.append(r)
            if len(results)==0:
                error = 'No results for {}... Try another /r/ prefix'.format(prefix)
                return render_template('/simple.html', p_res=[], query='',error=error)

        else:
            error='Enter the first 3 or more characters of the /r/ you are looking for.'

        if error !='':
            #print error
            return render_template('/simple.html', p_res=[], query='',error=error)

        return render_template('/simple.html', p_res=results, query=query, error=error)

    #just load the page
    return render_template('/simple.html', p_res=[], query='', error=error)

@app.route('/<subname>')
def subpage(subname=None):
    db_entry= g.db.get(bytes(subname,'ascii'),default=b'0')
    t_res=db_entry['topic']
    return render_template('r.html', rname=subname, t_res=t_res)

@app.route('/<topicID>')
def topicpage(topicID=None):
    db_entry= g.db.get(bytes(topicID,'ascii'),default=b'0')
    words=db_entry['words']
    docs=db_entry['docs']
    return render_template('t.html', topic=topicID, words=words, docs=docs)


if __name__ == "__main__":

    app.run()
