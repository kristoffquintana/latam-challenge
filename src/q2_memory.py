import apache_beam as beam
import emoji
import json
import heapq
from apache_beam.options.pipeline_options import PipelineOptions

def extract_emojis_optimized(text):
    return [item['emoji'] for item in emoji.emoji_list(text)]

def count_emojis_optimized(line):
    try:
        record = json.loads(line)
        content = record.get('content', '')
        return extract_emojis_optimized(content)
    except Exception as e:
        return []

def top_n_emojis(elements, n=10):
    heap = []
    for element in elements:
        heapq.heappush(heap, element)
        if len(heap) > n:
            heapq.heappop(heap)
    return sorted(heap, key=lambda x: -x[1])

def q2_memory(input_path):
    with beam.Pipeline(options=PipelineOptions()) as p:
        top_emojis = (
            p
            | 'Leer archivo' >> beam.io.ReadFromText(input_path)
            | 'Extraer emojis' >> beam.FlatMap(count_emojis_optimized)
            | 'Pair' >> beam.Map(lambda emoji: (emoji, 1))
            | 'Contar emojis' >> beam.CombinePerKey(sum)
            | 'Top 10' >> beam.combiners.Top.Of(10, key=lambda x: x[1])
            | 'Aplanar' >> beam.FlatMap(lambda x: x)
            | 'Recolectar en una lista' >> beam.combiners.ToList()
            | 'print' >> beam.Map(print)
        )