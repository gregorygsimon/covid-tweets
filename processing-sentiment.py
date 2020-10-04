import os, sqlite3, datetime
import pandas as pd

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import flair


###to view table schema:
# with sqlite3.connect(db) as conn:
#     cursor = conn.cursor()
#     print(cursor.execute("PRAGMA table_info('covid19tweets')").fetchall())
#     cursor.close()

states = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado",
  "Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois",
  "Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland",
  "Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
  "Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
  "North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
  "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
  "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]
assert len(states)==50

flair_sentiment = flair.models.TextClassifier.load('en-sentiment')
analyser = SentimentIntensityAnalyzer()

def flair_score(sentence):
    s = flair.data.Sentence(sentence)
    flair_sentiment.predict(s)

    if len(s.labels)>1:
        print('Flair should only return POSITIVE or NEGATIVE and not both.')
        print('s.to_plain_string: ',s.to_plain)
        print('s.labels:',s.to_plain_string)

    if s.labels[0].value=='NEGATIVE':
        return -1*s.labels[0].score
    elif s.labels[0].value=='POSITIVE':
        return s.labels[0].score
    else:
        print('Flair should only return one of POSITIVE or NEGATIVE.')
        print('s.to_plain_string: ',s.to_plain_string)
        print('s.labels:',s.labels)
        return None


# I have several files locally of the form twitter-$date.db
try:
    dbs = ['/media/hdd_1tb/data/'+fn
           for fn in os.listdir('/media/hdd_1tb/data/')
           if 'twitter' in fn and '.db' in fn
    ]
    dbs.sort()
#any other other user will have their own db in ./wrangling/twitter.db
except FileNotFoundError:
    try:
        dbs = [os.path.join(os.path.dirname(__file__), '/wrangling/twitter.db')]
    except NameError: #if run interactively
        dbs = ['./wrangling/twitter.db']

## first time this is run, processed all months using:
#months_list = range(1,datetime.datetime.now().month)

## all subsequent runs, just do current month
months_list = [datetime.datetime.now().month]

months_list = [8,9,10]

year = datetime.datetime.now().year

for month in months_list:

    first_sec_of_month  = datetime.datetime(year,month,1,0,0).strftime('%Y-%m-%d %H:%M:%S')
    last_sec_of_month = (datetime.datetime(year,month+1,1,0,0)+datetime.timedelta(seconds=-1))\
        .strftime('%Y-%m-%d %H:%M:%S')

    #initialize blank df for the month
    df_month = pd.DataFrame(columns=['retweet_count', 'created_at',
                                     'text', 'coordinates', 'user_loc',
                                     'user_lat', 'user_lon', 'user_loc_display',
                                     'id', 'source', 'state'])

    for db in dbs:
        print(db)
        conn = sqlite3.connect(db)
        df = pd.read_sql(f"""SELECT * FROM covid19tweets
                             WHERE instr(user_loc_display,'United States')>0
                             and created_at>'{first_sec_of_month}'
                             and created_at<'{last_sec_of_month}';
        """,conn)
        conn.close()

        df['state'] = None
        # assign state name to correct states
        # careful of Washington State vs D.C.
        for state in states:
            df.loc[
                (df['user_loc_display'].str.contains(state+', ') &
                 ~df['user_loc_display'].str.contains('District of Columbia,')
                )
                ,'state'] = state
        df_month = pd.concat([df_month,df])

    filename = f"sentiments_{year}-{str(month).rjust(2,'0')}.csv"

    file_exists = filename in os.listdir('data/')

    if file_exists:
        # select rows from df_month that aren't in CSV yet
        df_month = df_month[~df_month.id.isin(pd.read_csv('data/'+filename)['id'].values)]

    df_month.reset_index(inplace=True,drop=True)

    n = df_month.shape[0]
    for i,row in df_month.iterrows():
        df_month.at[i,'vader'] = analyser.polarity_scores(row['text'])['compound']
        df_month.at[i,'flair'] = flair_score(row['text'])

        if i%1000==0:
             print(df_month.loc[i],'\n') 
             print("row ",i," completed of ",n)            

    #include header only if file did not exist, otherwise append
    if file_exists:
        df_month.to_csv("data/"+filename,mode='a',header=False,index=False)
    else:
        df_month.to_csv("data/"+filename,header=True,index=False)
