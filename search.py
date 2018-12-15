from flask import Flask

from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, abort)
from collections import deque
import plyvel
import json
import pandas as pd

app = Flask(__name__)

def makedb():
    #needed info: is reddit in database, prefix iterator, reddit topics, topic words (convert from ints), topic docs
    # reading files..

    topicfile= 'getData/15-12_topics.json'
    subfile= 'getData/15-12_transformed.json'  #'small_transformed.json'
    vocabfile=  'getData/RS_2015-12._vocab.txt'

    ## READING SUB TOPICS
    old_subs_pd=pd.read_json(subfile, lines=True)

    fast = True
    if fast:
        old_subs_pd=old_subs_pd.iloc[:10,:] #comment for full set

    subs_pd=old_subs_pd.copy()
    temp_list=[]
    for j in range(10):
        temp_list.append([])
        
        for i, row in subs_pd.iterrows():
            sub=row[0]
            topics=row[1]['values']
            temp_list[j].append(topics[j])
        
        print('templist len', len(temp_list),'sublist', len(subs_pd))
        subs_pd['t_'+str(j)]=temp_list[j]
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
    
    #openloop
    wb=db.write_batch()

    for _,subreddit in subs_pd.iterrows():
        t_res=list(subreddit.iloc[1:])
        b=bytes(json.dumps({'results':t_res}),"utf-8")
        name=bytes(str(subreddit['subreddit']),"utf-8")
        wb.put(name, b)

    # needed info: is reddit in database, prefix iterator, reddit topics,
    # topic subs
    for t,topic in topic_pd.iterrows():
        words=topic['words']
        weights=topic['termWeights']
        name=b'topic: '+t.to_bytes(2,byteorder='big')+b'words'

        #pairs=zip(words, weights)
        #wb.put(name, pairs)

        subs=topic['Top 10 reddits']
        b=bytes(json.dumps({'words':words,'weights':weights,'subs':subs}),"utf-8")
        wb.put(name, b)

    wb.write()
    db.close()
    return db

@app.route('/')
def home():
    if 'database' not in g:
        db=makedb()
        g.database=db
        print("105:", g.__dict__)

    return redirect(url_for('simple'))

@app.route("/simple",methods=('GET', 'POST'))
def simple():
    database=plyvel.DB("Database", create_if_missing=True)        
    #manage inputs
    error=''
    if request.method=="POST":

        query=request.form['r']
        prefix=request.form['prefix']

        # search for full subreddit name
        if query != '':
            r=database.get(bytes(query,'ascii'),default=b'0')
            if r == b'0':
                error= 'Query "{}"does not match any subreddit. Try the prefix searcher?'.format(query)
                return render_template('/simple.html',p_res='', query='',error=error)
            #otherwise return /r/ page
            return redirect(url_for('subpage', subname=query))

        #search for sub by prefix
        if len(prefix)>2:
            results_it=database.iterator(prefix=bytes(prefix,'ascii'))
            results=[]
            for r in results_it:
                results.append(r[0])
            if len(results)==0:
                error = 'No results for {}... Try another /r/ prefix'.format(prefix)
                database.close()
                return render_template('/simple.html', p_res=[], query='',error=error)

        else:
            error='Enter the first 3 or more characters of the /r/ you are looking for.'

        if error !='':
            #print error
            database.close()
            return render_template('/simple.html', p_res=[], query='',error=error)
        database.close()
        return render_template('/simple.html', p_res=results, query=query, error=error)

    database.close()#just load the page
    return render_template('/simple.html', p_res=[], query='', error=error)

@app.route('/favicon.ico')
def foo():
    abort(404)

    
@app.route('/<subname>')
def subpage(subname=None):
    database=plyvel.DB("Database", create_if_missing=True)        

    print("===SUBNAME:", subname)
    name=bytes(subname,'utf-8')
    print("155:", g.__dict__)
    db_entry= database.get(name,default=b'0')
    t_res=json.loads(db_entry)
    database.close()
    return render_template('r.html', rname=subname, t_res=t_res)

@app.route('/<topicID>')
def topicpage(topicID=None):
    database=plyvel.DB("Database", create_if_missing=True)        

    db_entry= database.get(bytes(topicID,'ascii'),default=b'0')
    d=json.loads(db_entry)
    
    words=d['words']
    docs=d['docs']
    database.close()
    return render_template('t.html', topic=topicID, words=words, docs=docs)


if __name__ == "__main__":

    app.run()
