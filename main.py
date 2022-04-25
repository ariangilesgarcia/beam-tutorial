import argparse
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions


def main():
    parser = argparse.ArgumentParser(description="Dataflow & Apache Beam & Python & Snowflake")
    parser.add_argument("--input", "-i", help="Input file")
    parser.add_argument("--output", "-o", help="Output filename")
    parser.add_argument("--number", "-n", type=int, help="Number of words")

    args, beam_args  = parser.parse_known_args()
    run_pipeline(args, beam_args)


def run_pipeline(args, beam_args):
    input_fn = args.input
    output_fn = args.output
    top_n = args.number

    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p:
        lines = p | beam.io.ReadFromText(input_fn)
        tokens = lines | beam.FlatMap(lambda l: l.split())
        word_count = tokens | beam.combiners.Count.PerElement()
        top_words = word_count | beam.combiners.Top.Of(n=top_n, key=lambda w: w[1])
        top_words_flatten = top_words | beam.FlatMap(lambda w: w)
        # top_words_formatted = top_words_flatten | beam.Map(lambda w: f"'{w[0]}': {w[1]} times")
        top_words_csv = top_words_flatten | beam.Map(lambda w: f"{w[0]},{w[1]}")
        top_words_csv | beam.Map(print)
        top_words_csv | beam.io.WriteToText(output_fn)


if __name__ == "__main__":
    main()
