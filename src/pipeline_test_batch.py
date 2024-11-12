import apache_beam as beam
import json
import emoji
from apache_beam.options.pipeline_options import PipelineOptions

# Función optimizada para extraer solo emojis válidos
def extract_emojis_from_content(line):
    def filter_lines_with_emojis(line):
        return any(char in emoji.EMOJI_DATA for char in line)
    try:
        record = json.loads(line)
        content = filter_lines_with_emojis(record.get('content', ''))
        return [char for char in content if char in emoji.EMOJI_DATA]  # Filtrar solo emojis reales

    except json.JSONDecodeError:
        return []  # Ignora líneas mal formateadas

def q2_time_optimized(input_path):
    with beam.Pipeline(options=PipelineOptions()) as p:
        (
            p
            | 'ReadInputFile' >> beam.io.ReadFromText(input_path)
            | 'ExtractEmojis' >> beam.FlatMap(extract_emojis_from_content)  # Extrae emojis reales
            | 'PairWithOne' >> beam.Map(lambda emoji: (emoji, 1))  # Prepara para conteo
            | 'CountEmojis' >> beam.CombinePerKey(sum)  # Suma ocurrencias
            | 'Top10Emojis' >> beam.combiners.Top.Of(10, key=lambda x: x[1])  # Mantiene solo el top 10
            | 'PrintResults' >> beam.FlatMap(print)  # Imprime resultados en tiempo de ejecución
        )

input_path = "../../data/farmers-protest-tweets-2021-2-4.json"
q2_time_optimized(input_path)
