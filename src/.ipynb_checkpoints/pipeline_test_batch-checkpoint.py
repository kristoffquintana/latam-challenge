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
def run_pipeline(input_path):
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

run_pipeline("../../data/farmers-protest-tweets-2021-2-4.json")
