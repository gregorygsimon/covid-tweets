d=$(date +"%Y-%m-%d")

FILE=/media/hdd_1tb/data/twitter-$d.db

if [ -f "$FILE" ]
then
    echo "$FILE already transfered today."
else
    scp gregorygsimon@emet:/home/gregorygsimon/scripts/covid-twitter/twitter.db "$FILE"
    ssh gregorygsimon@emet "cd /home/gregorygsimon/scripts/covid-twitter/; mv twitter.db twitter-$d.db"
fi

source activate covidtweets
python processing-sentiment.py
jupyter nbconvert --to notebook --inplace --execute EDA.ipynb
git add *.png
git commit -m "auto update for figures, $d"
git push origin master




