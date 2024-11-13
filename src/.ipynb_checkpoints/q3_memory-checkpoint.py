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
        mentions = re.findall(r'@(\w+)', content)  # Busca menciones en formato @usuario
        for mention in mentions:
            yield (mention, 1)

# Pipeline de Apache Beam
def q3_memory(input_path):
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Leer Archivo" >> beam.io.ReadFromText(input_path)
            | "Parsear JSON" >> beam.Map(parse_json_line)
            | "Extraer y Contar Menciones" >> beam.FlatMap(extract_mentions)  # Emite (usuario, 1)
            | "Sumar Menciones" >> beam.CombinePerKey(sum)  # Usar el combinador integrado 'sum'
            | "Top 10 Usuarios Más Mencionados" >> beam.combiners.Top.Of(10, key=lambda x: x[1])
            | "Imprimir Resultado" >> beam.Map(print)
        )

