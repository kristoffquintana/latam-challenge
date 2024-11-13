import apache_beam as beam
import emoji
import json
from apache_beam.options.pipeline_options import PipelineOptions

def extract_emojis_optimized(text):
    return [item['emoji'] for item in emoji.emoji_list(text)]

def count_emojis_optimized(line):
    try:
        record = json.loads(line)
        content = record.get('content', '')
        return extract_emojis_optimized(content)
    except json.JSONDecodeError:
        return []  # Solo en caso de errores JSON específicos

def q2_time(input_path):
    with beam.Pipeline(options=PipelineOptions()) as p:
        (
            p
            | 'Leer archivo' >> beam.io.ReadFromText(input_path)
            | 'Extraer y contar emojis' >> beam.FlatMap(count_emojis_optimized)
            | 'Map a Pair' >> beam.Map(lambda emoji: (emoji, 1))
            | 'Combinar conteos' >> beam.CombinePerKey(sum)
            | 'Obtener Top 10' >> beam.transforms.combiners.Top.Of(10, key=lambda x: x[1])
            | 'Aplanar resultados' >> beam.FlatMap(lambda x: x)
            | 'Recolectar en una lista' >> beam.combiners.ToList()
            | 'Imprimir resultados' >> beam.Map(print)
        )

        
        # Imprimir directamente los resultados
        # for result in top_emojis | beam.Map(print):
        #     pass

        # Para recolectar los resultados como una lista
        # return emoji_counts | 'Recolectar en una lista' >> beam.combiners.ToList()


q2_time("../../data/farmers-protest-tweets-2021-2-4.json")
