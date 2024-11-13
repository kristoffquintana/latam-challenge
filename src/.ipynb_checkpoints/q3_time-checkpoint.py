import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import re

def parse_json_line(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None

def extract_mentions(record):
    if record and 'content' in record:
        content = record['content']
        for mention in re.findall(r'@(\w+)', content):
            yield (mention, 1)

def q3_time(input_path):
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Leer Archivo" >> beam.io.ReadFromText(input_path)
            | "Parsear y Extraer Menciones" >> beam.FlatMap(lambda line: extract_mentions(parse_json_line(line)))
            | "Sumar Menciones" >> beam.CombinePerKey(sum)
            | "Top 10 Más Mencionados" >> beam.transforms.combiners.Top.Of(10, key=lambda x: x[1])
            | "Imprimir" >> beam.Map(print)
        )