import apache_beam as beam
from datetime import datetime
from collections import Counter
import json

def parse_tweet(tweet_json):
    tweet = json.loads(tweet_json)
    tweet_date = tweet['date'][:10]
    username = tweet['user']['username']
    return (tweet_date, username)

def q1_time(input_file):
    with beam.Pipeline() as pipeline:
        
        parsed_tweets = (
            pipeline
            | 'Read input file' >> beam.io.ReadFromText(input_file)
            | 'Parse tweets' >> beam.Map(parse_tweet)
        )

        top_dates = (
            parsed_tweets
            | 'Count tweets per date' >> beam.combiners.Count.PerKey()
            | 'Top 10 dates' >> beam.transforms.combiners.Top.Of(10, key=lambda x: x[1])
            | 'Extract top dates' >> beam.FlatMap(lambda x: x)
            | 'Create date lookup' >> beam.Map(lambda x: x[0])
        )

        result = (
            parsed_tweets
            | 'Filter top 10 dates' >> beam.Filter(
                lambda tweet, top_dates_set: tweet[0] in top_dates_set,
                beam.pvalue.AsList(top_dates)
            )
            | 'Group by date and aggregate users' >> beam.GroupByKey()
            | 'Most active user per date' >> beam.Map(lambda x: (x[0], Counter(x[1]).most_common(1)[0][0]))
            | 'Format output' >> beam.Map(lambda x: (datetime.strptime(x[0], '%Y-%m-%d').date(), x[1]))
            | 'Collect results' >> beam.combiners.ToList()
        )

        result | 'Print results' >> beam.Map(print)