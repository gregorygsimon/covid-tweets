from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import flair

flair_sentiment = flair.models.TextClassifier.load('en-sentiment')
analyser = SentimentIntensityAnalyzer()

sentence = 'The situation involving covid19 is getting much worse.'






dbs = ['/media/hdd_1tb/data/'+fn for fn in os.listdir('/media/hdd_1tb/data/') if 'twitter' in fn and '.db' in fn]

db = dbs[0]


s = flair.data.Sentence('The wall is red.')
flair_sentiment.predict(s)
s.get_labels()

#create function that calculates single flair score

#iterate through database(s), adding vader score and flair score

flair_sentiment.label_dictionary()
