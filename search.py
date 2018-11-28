from flask import Flask

from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for)
from collections import deque
import plyvel

app = Flask(__name__)

@app.route('/')
def home():
    return redirect(url_for('simple'))

@app.route("/simple",methods=('GET', 'POST'))
def simple():
    #Initialize db 
    if 'db' not in g:
        g.db=plyvel.DB("Database", create_if_missing=True)
        sub_list= ['leagueoflegends', 'gaming', 'DestinyTheGame', 'DotA2', 'ContestofChampions', 'StarWarsBattlefront', 'Overwatch', 'WWII', 'hearthstone', 'wow', 'heroesofthestorm', 'destiny2', 'darksouls3', 'fallout', 'SuicideWatch', 'depression', 'OCD', 'dpdr', 'proED', 'Anxiety', 'BPD', 'socialanxiety', 'mentalhealth', 'ADHD', 'bipolar', 'buildapc', 'techsupport', 'buildapcforme', 'hacker', 'SuggestALaptop', 'hardwareswap', 'laptops', 'computers', 'pcmasterrace', 'relationshps', 'relationship_advice', 'breakups', 'dating_advice', 'LongDistance', 'polyamory', 'wemetonline', 'MDMA', 'Drugs', 'trees', 'opiates', 'LSD', 'tifu', 'r4r', 'AskReddit', 'reddit.com', 'tipofmytongue', 'Life', 'Advice', 'jobs', 'teenagers', 'HomeImprovement', 'redditinreddit', 'FIFA', 'nba', 'hockey', 'nfl', 'mls', 'baseball', 'BokuNoHeroAcademia', 'anime', 'movies', 'StrangerThings']
        i=0

        wb=g.db.write_batch()

        for name in sub_list:
            i+=1

            wb.put(bytes(name,'ascii'), i.to_bytes(2,byteorder='big'))
        
        wb.write()

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
            return render_template('/simple.html', p_res=[], query=str(r),error=error)
        
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
    

if __name__ == "__main__":
   
    app.run()