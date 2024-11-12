# from typing import List, Tuple
# from datetime import datetime

# def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
#     pass


# import apache_beam as beam
# from datetime import datetime
# from collections import Counter
# import json

# def parse_tweet(tweet_json):
#     tweet = json.loads(tweet_json)
#     tweet_date = tweet['date'][:10]
#     username = tweet['user']['username']
#     return (tweet_date, username)

# def q1_time(input_file):
#     with beam.Pipeline() as pipeline:
        
#         # Primera fase: Obtener el conteo de tweets por fecha
#         top_dates = (
#             pipeline
#             | 'Read input file' >> beam.io.ReadFromText(input_file)
#             | 'Parse tweets' >> beam.Map(parse_tweet)
#             | 'Count tweets per date' >> beam.combiners.Count.PerKey()
#             | 'Top 10 dates with most tweets' >> beam.combiners.Top.Of(10, key=lambda x: x[1])  # Top 10 fechas por conteo de tweets
#             | 'Flatten top dates' >> beam.FlatMap(lambda x: x)  # Convertir la lista de top 10 fechas a elementos individuales
#             | 'Convert to dict for side input' >> beam.Map(lambda x: (x[0], x[1]))  # Convertir a pares clave-valor
#         )

#         result = (
#             pipeline
#             | 'Re-read input file' >> beam.io.ReadFromText(input_file)
#             | 'Re-parse tweets' >> beam.Map(parse_tweet)
#             | 'Filter top dates' >> beam.Filter(lambda x, top_dates_dict: x[0] in top_dates_dict, beam.pvalue.AsDict(top_dates))  # Filtrar usando el side input
#             | 'Group tweets by date' >> beam.GroupByKey()  # Agrupamos por fecha
#             | 'Find most active user per date' >> beam.Map(lambda x: (x[0], Counter(x[1]).most_common(1)[0][0]))  # Usuario más activo en cada fecha
#             | 'Format output' >> beam.Map(lambda x: (datetime.strptime(x[0], '%Y-%m-%d').date(), x[1])) 
#             | 'Recolectar en una lista' >> beam.combiners.ToList()
#         )

#         result | 'Print results' >> beam.Map(print)




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
        
        # Leer y procesar tweets una sola vez
        parsed_tweets = (
            pipeline
            | 'Read input file' >> beam.io.ReadFromText(input_file)
            | 'Parse tweets' >> beam.Map(parse_tweet)
        )

        # Obtener las 10 fechas con más tweets
        top_dates = (
            parsed_tweets
            | 'Count tweets per date' >> beam.combiners.Count.PerKey()
            | 'Top 10 dates' >> beam.transforms.combiners.Top.Of(10, key=lambda x: x[1])
            | 'Extract top dates' >> beam.FlatMap(lambda x: x)  # Desempaquetar la lista de Top.Of
            | 'Create date lookup' >> beam.Map(lambda x: x[0])  # Extraer solo las fechas
        )

        # Filtrar tweets y encontrar usuario más activo por fecha
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

        # Imprimir resultados
        result | 'Print results' >> beam.Map(print)
