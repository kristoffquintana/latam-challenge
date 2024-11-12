# from typing import List, Tuple

# def q3_time(file_path: str) -> List[Tuple[str, int]]:
#     pass



# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions
# import json
# import re
# from collections import Counter

# # Función para parsear cada línea y devolver un diccionario
# def parse_json_line(line):
#     try:
#         return json.loads(line)
#     except json.JSONDecodeError:
#         return None

# # Función para extraer menciones de usuarios en el campo 'content'
# def extract_mentions(record):
#     if record and 'content' in record:
#         content = record['content']
#         mentions = re.findall(r'@(\w+)', content)  # Busca menciones en formato @usuario
#         return mentions
#     return []

# # Pipeline de Apache Beam
# def q3_time(input_path):
#     options = PipelineOptions()
#     with beam.Pipeline(options=options) as p:
#         # Leer el archivo línea por línea y procesar menciones
#         user_mentions = (
#             p
#             | "Leer Archivo" >> beam.io.ReadFromText(input_path)
#             | "Parsear JSON" >> beam.Map(parse_json_line)
#             | "Extraer Menciones" >> beam.FlatMap(extract_mentions)  # Una lista de menciones por tweet
#             | "Mapear Usuario" >> beam.Map(lambda user: (user, 1))  # Generar (usuario, 1)
#             | "Sumar Menciones" >> beam.CombinePerKey(sum)  # Combinar y sumar menciones por usuario
#             | "Top 10 Usuarios Más Mencionados" >> beam.combiners.Top.Of(10, key=lambda x: x[1])
#             | "Imprimir Resultado" >> beam.Map(print)
#         )



import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import re

# Función para parsear cada línea y devolver un diccionario
def parse_json_line(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None

# Función para extraer menciones de usuarios en el campo 'content'
def extract_mentions(record):
    if record and 'content' in record:
        content = record['content']
        for mention in re.findall(r'@(\w+)', content):  # Busca menciones en formato @usuario
            yield (mention, 1)

# Pipeline optimizado para tiempo de respuesta
def q3_time(input_path):
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Leer Archivo" >> beam.io.ReadFromText(input_path)
            | "Parsear y Extraer Menciones" >> beam.FlatMap(lambda line: extract_mentions(parse_json_line(line)))
            | "Sumar Menciones" >> beam.CombinePerKey(sum)  # Combinar y sumar menciones en paralelo
            | "Top 10 Más Mencionados" >> beam.transforms.combiners.Top.Of(10, key=lambda x: x[1])
            | "Imprimir" >> beam.Map(print)
        )