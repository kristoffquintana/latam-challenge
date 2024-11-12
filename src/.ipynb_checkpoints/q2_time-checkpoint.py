# from typing import List, Tuple

# def q2_time(file_path: str) -> List[Tuple[str, int]]:
#     pass



import apache_beam as beam
import re
import emoji
from apache_beam.options.pipeline_options import PipelineOptions

# Función para extraer emojis de un texto
def extract_emojis(text):
    return [char for char in text if char in emoji.EMOJI_DATA]

# Función para contar emojis
def count_emojis(line):
    import json
    try:
        record = json.loads(line)
        content = record.get('content', '')
        return extract_emojis(content)
    except Exception as e:
        return []  # Si hay un error, retornamos una lista vacía

# Pipeline de Apache Beam
def q2_time(input_path):
    with beam.Pipeline(options=PipelineOptions()) as p:
        emoji_counts = (
            p
            | 'ReadInputFile' >> beam.io.ReadFromText(input_path)
            | 'ExtractEmojis' >> beam.FlatMap(count_emojis)
            | 'PairWithOne' >> beam.Map(lambda emoji: (emoji, 1))
            | 'CountEmojis' >> beam.CombinePerKey(sum)
            | 'Top10Emojis' >> beam.combiners.Top.Of(10, key=lambda x: x[1])
            | 'PrintResults' >> beam.FlatMap(lambda x: x)  # Desenpaquetar Top.Of
            | 'Recolectar en una lista' >> beam.combiners.ToList()
            | 'PrintOutput' >> beam.Map(print)  # Mostrar en Jupyter
        )