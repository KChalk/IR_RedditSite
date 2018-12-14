from flask import Flask

from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for)
from collections import deque
import plyvel
import json

app = Flask(__name__)

def makedb():
    #needed info: is reddit in database, prefix iterator, reddit topics, topic words (convert from ints), topic docs
    # reading files..
    topicfile= '15-12_topics.json'
    subfile=   'RS_2015-12_transformed.json'
    vocabfile=  'RS_2015-12_vocab.txt'

    db=plyvel.DB("Database", create_if_missing=True)
    #sub_list= ['leagueoflegends', 'gaming', 'DestinyTheGame', 'DotA2', 'ContestofChampions', 'StarWarsBattlefront', 'Overwatch', 'WWII', 'hearthstone', 'wow', 'heroesofthestorm', 'destiny2', 'darksouls3', 'fallout', 'SuicideWatch', 'depression', 'OCD', 'dpdr', 'proED', 'Anxiety', 'BPD', 'socialanxiety', 'mentalhealth', 'ADHD', 'bipolar', 'buildapc', 'techsupport', 'buildapcforme', 'hacker', 'SuggestALaptop', 'hardwareswap', 'laptops', 'computers', 'pcmasterrace', 'relationshps', 'relationship_advice', 'breakups', 'dating_advice', 'LongDistance', 'polyamory', 'wemetonline', 'MDMA', 'Drugs', 'trees', 'opiates', 'LSD', 'tifu', 'r4r', 'AskReddit', 'reddit.com', 'tipofmytongue', 'Life', 'Advice', 'jobs', 'teenagers', 'HomeImprovement', 'redditinreddit', 'FIFA', 'nba', 'hockey', 'nfl', 'mls', 'baseball', 'BokuNoHeroAcademia', 'anime', 'movies', 'StrangerThings']

    wb=db.write_batch()


    # topic words (convert from ints), 
    vocablist=[]
    with open(vocabfile) as v: 
        for line in v:
            vocablist.append(line[:-1])

    
    with open(topicfile) as t:
        for line in t: 
            topic=json.loads(line)
            
            wb.put(bytes(name,'ascii'), i.to_bytes(2,byteorder='big'))
            
    # needed info: is reddit in database, prefix iterator, reddit topics, 
    # topic subs
    with open(subfile) as f:
        for line in f: 
            sub=json.loads(line)
            wb.put(bytes(name,'ascii'), i.to_bytes(2,byteorder='big'))
    
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