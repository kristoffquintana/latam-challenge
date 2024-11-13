import apache_beam as beam
from datetime import datetime
from collections import Counter
import json

def parse_tweet(tweet_json):
    tweet = json.loads(tweet_json)
    tweet_date = tweet['date'][:10]
    username = tweet['user']['username']
    return (tweet_date, username)

def count_tweets_by_date(tweet_date):
    yield (tweet_date, 1)

def get_top_10_elements(elements):
    return sorted(elements, key=lambda x: x[1], reverse=True)[:10]

def filter_top_10_dates(tweet, top_dates):
    tweet_date, username = tweet
    if tweet_date in top_dates:
        yield (tweet_date, username)

def q1_memory(input_file):
    with beam.Pipeline() as pipeline:
        tweets = (
            pipeline
            | 'Input' >> beam.io.ReadFromText(input_file)
            | 'Columnar' >> beam.Map(parse_tweet)
        )

        top_10_dates = (
            tweets
            | 'Extraer fechas de twitter' >> beam.Map(lambda tweet: (tweet[0], 1))
            | 'Conteo por fechas' >> beam.CombinePerKey(sum)
            | 'Juntar conteos' >> beam.combiners.ToList()
            | 'Obtener el top 10' >> beam.FlatMap(lambda counts: get_top_10_elements(counts))
            | 'Key top 10' >> beam.Map(lambda x: x[0])
        )

        result = (
            tweets
            | 'Filtrar fechas' >> beam.FlatMap(
                lambda tweet, top_dates: filter_top_10_dates(tweet, top_dates),
                beam.pvalue.AsList(top_10_dates)
            )
            | 'Agrupar por fecha' >> beam.GroupByKey()
            | 'Usuario mas activo por fecha' >> beam.Map(lambda x: (x[0], Counter(x[1]).most_common(1)[0][0]))
            | 'Formateo' >> beam.Map(lambda x: (datetime.strptime(x[0], '%Y-%m-%d').date(), x[1]))
            | 'Formateo final' >> beam.combiners.ToList()
        )

        result | 'Print' >> beam.Map(print)
