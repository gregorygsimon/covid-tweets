d=$(date +"%Y-%m-%d")

FILE=/media/hdd_1tb/data/twitter-$d.db

if [ -f "$FILE" ]; then
    echo "$FILE already transfered."
else
    scp gregorygsimon@emet:/home/gregorygsimon/scripts/covid-twitter/twittter.db "$FILE"
    ssh gregorygsimon@emet "cd /home/gregorygsimon/scripts/covid-twitter/; mv twitter.db twitter-$d.db"
    conda activate covidtweets
    python processing-sentiment.py
fi



