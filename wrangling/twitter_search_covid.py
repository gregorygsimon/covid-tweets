import pandas as pd
import datetime, sqlite3, json, os, time
import tweepy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

#load my personal Twitter credentials from ~/.ssh/
try:
    with open('/home/greg/.ssh/twitter_credentials.json','r') as f:
        creds = json.load(f)
    consumer_key    = creds['consumer_key']
    consumer_secret = creds['consumer_secret']
    access_key      = creds['access_key']
    access_secret   = creds['access_secret']
except FileNotFoundError:
    print('enter twitter developer credentials in twitter_search_covid.py')
    print('see http://developer.twitter.com/')
    consumer_key    = ''
    consumer_secret = ''
    access_key      = ''
    access_secret   = ''

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

results = api.search('covid19|coronavirus',
                     ### geocode seemingly no longer accurate, so disabling
                     ## radius centered on Lansing large enough to encompass Michigan
#                     geocode="42.7325,84.5555,250mi",
                     lang='en',
                     result_type='recent',
                     count = 100
)


# pull recent IDs to only spend time on tweets not yet in database
if 'recent_IDs.csv' in os.listdir():
    recent_IDs = pd.read_csv('recent_IDs.csv')
else:
    # create empty dataframe
    recent_IDs = pd.DataFrame(columns=['id'])
recent = recent_IDs['id'].values

df = pd.DataFrame([{'retweet_count':r.retweet_count,
                    'created_at':r.created_at,
                    'text':r.text,
                    'coordinates':r.coordinates,
                    'user_loc':r._json.get('user').get('location').encode('ascii',errors='replace').decode('ascii'),
                    'user_lat':None,
                    'user_lon':None,
                    'user_loc_display':None,
                    'id':r.id,
                    'source':r.source} for r in results if r.id not in recent])

# join newly found IDs to recent_IDs, keep last 5000, and resave list
recent_IDs = pd.concat((df[['id']],recent_IDs[['id']]))
recent_IDs.reset_index(inplace=True,drop=True)
recent_IDs = recent_IDs.iloc[:5000]
recent_IDs.to_csv('recent_IDs.csv',index=False)

#set up geocoder, per Nominatim's TOS
geolocator = Nominatim(user_agent="gregorygsimon@gmail.com")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# geocode Nominatim requires 1 second per request, important to run sparingly
# iterate through the rows of df
for i, row in df.iterrows():
    loc = row['user_loc']

    if loc and loc!='':
        gc = geocode(row['user_loc'])
        if gc:
            geo_dict = gc.raw
        else:
            geo_dict = {}

        df.set_value(i,'user_lat',geo_dict.get('lat'))
        df.set_value(i,'user_lon',geo_dict.get('lon'))
        df.set_value(i,'user_loc_display', geo_dict.get('display_name'))
        time.sleep(1)
        print(df.iloc[i])


### if too many automated tweets, consider restricting to human sources 

#human_sources = ['Twitter Web App', 'Twitter for Android', 'Twitter for iPhone']
#df = df[df['source'].isin(human_sources)]


### if errors arise with foreign alphabets / symbols:

# def string_to_ascii(string):
#     if type(string) == str:
#         return string.encode('ascii',errors='replace').decode('ascii')
#     else:
#         return string
#
# for col in ('text','user_loc_display','user_loc'):
#     df[col] = df[col].apply(string_to_ascii)


### if 'coordinates' is not blank, it is a dict that won't fit in our dataframe
### will be blank for less than 1% of tweets
df['coordinates'] = df['coordinates'].astype(str)

conn = sqlite3.connect('twitter.db')
df.to_sql(name='covid19tweets',con=conn,index_label='id',if_exists='append',index=False)
conn.close()

# to extract as df
#conn = sqlite3.connect('twitter.db')
#us = pd.read_sql("SELECT * FROM covid19tweets WHERE instr(user_loc_display,'United States')>0",conn)
#conn.close()
## list(us.columns) == 
## ['retweet_count', 'created_at', 'text', 'coordinates', 'user_loc', 'user_lat', 'user_lon', 'user_loc_display', 'id', 'source']
