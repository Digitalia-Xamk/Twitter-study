import tweepy as tw
import os
import pandas as pd
import sqlite3
import json
import datetime as dt
import sys

# Your developer account info
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        #print(db_file)
    except Error as e:
        print(' Virhe ')
    finally:
        if conn:
            i_db == 0 
            conn.close()
 
create = 'CREATE TABLE IF NOT EXISTS '
table_stuff = '(id integer ,time text NOT NULL,message text,likes INTEGER,re INTEGER,hasht TEXT, mentions TEXT) '
write_stuff = '(id,time,message,likes,re,hasht,mentions)'
to_db = '(?,?,?,?,?,?,?)'

mlist = [] 
sublist = []
# Your account file
with open("your-account.txt","r") as reader:
        lines = reader.readlines()

# Iterate databases
total = 0 
for i_db in range(0,len(lines)):
    a = lines[i_db].replace(',\n','')
    b = a.replace('\n','')
    c = b.replace(' ','')
    mlist.append(c.splitlines())
    try:
         mlist.remove('')
    except:
        pass     
    sublist = mlist[i_db][0].split(',')
    sublist[0].replace(' ','')
    src = sublist[0]+".db"
    db ='.db'
    db_file = sublist[0].rstrip() + db 
    create_connection(db_file) 
    db ='.db'
    db_file = sublist[0].rstrip() + db 
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    search_screen=[]
    ## Get the accounts in database
    for i in range(1,len(sublist)):
        #init = False
        search_screen.append(sublist[i])
 

#  Iterate database accounts
    for i in range (0,len(search_screen)):
        t_name =  '\'' + search_screen[i] + '\''
        if_exist = ' SELECT count(name) FROM sqlite_master WHERE type=\'table\'  AND name='+ t_name + ' COLLATE nocase'
        #print (if_exist)
        c.execute(if_exist)
        if c.fetchone()[0]==1 :
            init = False
            
        else:
            init = True
            #f_max = 0 
            
        try:
            tweets = api.user_timeline(screen_name=search_screen[i], count=1,api_mode='extended',tweet_mode='extended')
        except:
            print(' Unable to get tweets via api, account, ', search_screen[i])
            break
        try:
            minid = tweets[-1].id 
        except:
            print(' No tweets from , ' ,search_screen[i])
            break
        id = tweets[-1].id 
        user = str(tweets[0].user.screen_name)
        path = " media/"+sublist[0].strip()+"/"+user
        
        ment =''
        f_max = 0 
        if init == False:
            minid = f_max-1
        else: 
            minid = id - 1    


        if init == True:
            initi = True
        else: 
            initi = False
        print('     Account ', user)    
        while id >minid:
            up_count = 0 
            if init == False:
                try:
                    tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',since_id=id_up)
                    #tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',max_id=minid)
                    #print('downloading')
                except:
                    #print(' Trying to get tweets failed')
                    pass
            else:
                
                tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',max_id=id)
                #tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',since_id=id)
            id = minid  - 1 
# We collect the recent tweets  and create tables if necessary:
            for tweet in tweets:
                search_screen[i]= tweet.user.screen_name
                
                ids = tweet.id
                

                minid = min(minid,ids)
                taulunnimi = tweet.user.screen_name
                sql = create+ taulunnimi + table_stuff
                c.execute(sql)
                sql = 'SELECT id FROM '+taulunnimi+ ' WHERE id ='+ str(ids) 
                value = c.execute(sql)
                value=c.fetchone()
                hasht=''
                ment = ''
                
                if value == None :
                    txt = tweet.full_text   
                    time = str(tweet.created_at)
                    f = tweet.favorite_count
                    r = tweet.retweet_count
                    if tweet.entities['hashtags'] !=[]:
                        hasht = tweet.entities['hashtags'][0]['text']
                        for j in range(1,len(tweet.entities['hashtags'])):
                            hasht = hasht + '; ' + tweet.entities['hashtags'][j]['text']
                    else:
                        hasht =''
                    
                    mentions = tweet.entities['user_mentions']
                    if mentions == []:
                        mentions = ''
                    else :
                        ment = ''
                    ment = ''    
                    for j in range(0, len(mentions)):
                        if j > 0:
                            ment   = ment + '; ' + tweet.entities['user_mentions'][j]['name']
                        else: 
                            ment = tweet.entities['user_mentions'][0]['name']
                
                    stat_count = tweet.user.followers_count
                    listed = tweet.user.listed_count
                    litania = 'INSERT INTO ' +taulunnimi+ write_stuff + ' VALUES' + to_db 
                    write_list = [ids,time,txt,f,r,hasht,ment] 
                    media = 'download-twitter-resources '+ '-c cr.json' + ' --video' + ' --tweet '+ str(ids) + path
                    c.execute(litania, write_list)
                    conn.commit()
                    os.system(media)
                else:
                    pass                 

    c.close()
    print(' Initialization/collection ready ',db_file)

