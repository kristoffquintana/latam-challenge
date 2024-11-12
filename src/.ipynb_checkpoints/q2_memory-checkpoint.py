import apache_beam as beam
import json
import emoji
from apache_beam.options.pipeline_options import PipelineOptions

# Función optimizada para extraer solo emojis válidos
def extract_emojis_from_content(line):
    try:
        record = json.loads(line)
        content = record.get('content', '')
        return [char for char in content if char in emoji.EMOJI_DATA]  # Filtrar solo emojis reales
    except json.JSONDecodeError:
        return []  # Ignora líneas mal formateadas

def q2_memory(input_path):
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