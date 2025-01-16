#! /usr/bin/env python
import sys
import argparse
import polars as ps

# TODO:
# implement for manysearch, (multi)gather - detect automatically


def _load_files(gather_csvs):
    loaded = []
    for filename in gather_csvs:
        if filename.endswith('.csv'):
            loaded.append(ps.scan_csv(filename))
        elif filename.endswith('.parquet'):
            loaded.append(ps.scan_parquet(filename))
        else:
            assert 0, filename
    return ps.concat(loaded)


def concat(args):
    print(f"** loading {len(args.gather_csvs)} CSVs")
    combined = _load_files(args.gather_csvs)
    print(f"** writing parquet file '{args.output}'")
    combined.sink_parquet(args.output)


def columns(args):
    print(f"** showing columns for given CSV(s)")
    combined = _load_files(args.gather_csvs)
    columns = combined.collect_schema().names()

    if args.filter:
        print(f"** filtering on substr matches to '{args.filter}'")
        columns = [ x for x in columns if args.filter in x ]

    print(columns)


def display(args):
    print('display!', args)


def main():
    p = argparse.ArgumentParser()
    subparsers = p.add_subparsers(help="subcommand help",
                                  required=True, dest='command')

    p_concat = subparsers.add_parser('concat', help='concatenate CSVs into parquet')
    p_concat.add_argument('gather_csvs', nargs='+')
    p_concat.add_argument('-o', '--output', required=True)
    p_concat.set_defaults(func=concat)

    p_display = subparsers.add_parser('display', help='display CSVs')
    p_display.add_argument('gather_csvs', nargs='+')
    p_display.set_defaults(func=display)

    p_columns = subparsers.add_parser('columns', help='display columns')
    p_columns.add_argument('gather_csvs', nargs='+')
    p_columns.add_argument('-l', '--filter', help="filter on substring")
    p_columns.set_defaults(func=columns)
    
    args = p.parse_args()
    args.func(args)


if __name__ == '__main__':
    sys.exit(main())
