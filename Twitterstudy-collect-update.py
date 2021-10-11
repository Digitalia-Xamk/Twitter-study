import tweepy as tw
import os
import pandas as pd
import sqlite3
import json
import datetime as dt
import sys

" Add your developer account information"
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
    except sqlite3.Error as e:
        print(' Sqlite error ')
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
with open("your-accounts.txt","r") as reader:
        lines = reader.readlines()

#print(lines)
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
    #print(sublist)
    src = sublist[0]+".db"
    db ='.db'
    db_file = sublist[0].rstrip() + db 
    create_connection(db_file) 
    db ='.db'
    db_file = sublist[0].rstrip() + db 
    #print(db_file)
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    # Indexing if you want
    #index = 'CREATE INDEX index_id ON ' + sublist[1] + '( id )'
    #c.execute(index)c
    up_count = 0 
    search_screen=[]
    for i in range(1,len(sublist)):
        init = False
        search_screen.append(sublist[i])
        #print(sublist[i])
        

        total_count = 'SELECT Count(*) FROM '+ sublist[i]
        find_max = 'SELECT MAX(id) FROM '+ sublist[i]
        sort_db = 'SELECT * FROM ' + sublist[i] + '  ORDER BY id DESC'
        try:
            c.execute(sort_db)
        except:
            pass
        update_days  = dt.timedelta(days = 14 )
        now = dt.datetime.now()
        for row in c.fetchall():
            time_in_db = dt.datetime.strptime(row[1],'%Y-%m-%d %H:%M:%S')
            up_count = 0
            if now - time_in_db < update_days: 
                id_up = max(row[0], 0)

        #print('Updating ', up_count, ' messsages')
        #break
        initi = False
        try:
            t = c.execute(total_count)
            t = c.fetchone()[0]
            f_max = c.execute(find_max)
            f_max = c.fetchone()[0]
            total = total + t 
            #print(sublist[i])
            print(' Total amount of tweets in databases, ', total, ' and in table ' + sublist[i]  ,t  )
            #print('Max ', f_max)
            
        except:
            print(' Creating')
            initi = True
            pass

#  Database accounts
    for i in range (0,len(search_screen)):
        t_name =  '\'' + search_screen[i] + '\''
        if_exist = ' SELECT count(name) FROM sqlite_master WHERE type=\'table\'  AND name='+ t_name + ' COLLATE nocase'
        #print (if_exist)
        c.execute(if_exist)
        if c.fetchone()[0]==1 :
            init = False
        else:
            init = True
            
        try:
            tweets = api.user_timeline(screen_name=search_screen[i], count=1,api_mode='extended',tweet_mode='extended')
        except:
            print(init, ' No tweets in ', search_screen[i])
            pass
        try:
            minid = tweets[-1].id 
        except:
            print(' No tweets from , ' ,search_screen[i])
            break
        id = tweets[-1].id 
        user = str(tweets[0].user.screen_name)
        path = " media/"+sublist[0].strip()+"/"+user
        
        ment =''
        if init == False:
            minid = f_max-1
        #print(init,id,minid,id_up)
        #id_up = max(int(row[i]), 0)
        Updating = True
        up_count = 0 
        while Updating == True:
            #print(id,minid)
            
    #    print(id)
            if init == False:
                try:
                   # tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',since_id=id_up)
                    tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',max_id=id)
                    #print('downloading')
                except:
                    #print(' No new tweets')
                    pass
            else:
                #tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',max_id=id)
                tweets = api.user_timeline(screen_name=search_screen[i], count=200,api_mode='extended',tweet_mode='extended',since_id=minid)
            id = minid  - 1 
# We collect the most recent tweets or create new database or tables:
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
                #if init==True:
                #    print(' Luo taulu ', value)
                
                if value == None :
                    print(' New tweets found, running collection first')
                    #pass                  
                    txt = tweet.full_text   
                    #print(txt)
                    
                    
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
                    #print( "Adding)
            
                else:
                    up_count = up_count +1 
                    # update existing values
                    #print(up_count, taulunnimi)
                    time = str(tweet.created_at)
                    time_in_db = dt.datetime.strptime(time,'%Y-%m-%d %H:%M:%S')
                    #if now - time_in_db > update_days :
                    #    Updating == False

                    #print( ' Updating tweet created at ', time, up_count)
                    f = tweet.favorite_count
                    r = tweet.retweet_count
                    sql = ' UPDATE ' + taulunnimi +  ' SET likes = ? , re = ?  WHERE  id = ?'
                    c.execute(sql,(f,r,str(id))) 
                    conn.commit() 
                    id = tweet.id
                    stop_updating = now - tweet.created_at
                    if stop_updating > update_days: 
                        print(' Updated tweets ', up_count, ' at', taulunnimi, ' in ' , db_file)
                        Updating = False
                        break
                        
    c.close()
    #print(' Updated metrics ', up_count,sublist[i])

