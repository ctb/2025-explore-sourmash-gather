#! /usr/bin/env python
import sys
import argparse
import polars as pl

# TODO:
# implement for manysearch, (multi)gather - detect automatically

# EVENTUALLY?
# - add explainer for columns


def _load_files(gather_csvs):
    "load CSVs and/or parquet files"
    loaded = []
    for filename in gather_csvs:
        if filename.endswith('.csv'):
            loaded.append(pl.scan_csv(filename))
        elif filename.endswith('.parquet'):
            loaded.append(pl.scan_parquet(filename))
        else:
            assert 0, filename
    return pl.concat(loaded)


def _trunc_dots(disp, *, length=40):
    if len(disp) <= 40:
        return disp

    disp = disp[:37] + '...'
    return disp


def _display_name(name, *, length=40, split=True):
    #name = " ".join(name.split(' ')[:3])
    return _trunc_dots(name)


def _percent(val):
    return f"{100*val:.1f}%"


def _format_bp(bp):
    "Pretty-print bp information."
    bp = float(bp)
    if bp < 500:
        return f"{bp:.0f} bp"
    elif bp <= 500e3:
        return f"{round(bp / 1e3, 1):.1f} kbp"
    elif bp < 500e6:
        return f"{round(bp / 1e6, 1):.1f} Mbp"
    elif bp < 500e9:
        return f"{round(bp / 1e9, 1):.1f} Gbp"
    return "???"


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
    combined = _load_files(args.gather_csvs).collect()
    columns = combined.collect_schema().names()
    if 'f_unique_weighted' in columns: # autodetect: gather
        display_gather(args, combined)


def display_gather(args, combined):
    print(f"** displaying gather results for given CSVs")

    # @CTB: catch 'name' for old-style gather results.
    queries = set(combined['query_name'])

    for q in queries:
        df = combined.filter(pl.col('query_name') == q)
        df = df.sort("f_unique_weighted", descending=True)[:args.display_num_results]
        df = df.filter(pl.col('f_unique_weighted') >= args.threshold)

        sum_sofar = 0
        total = sum(df['f_unique_weighted'])
        did_disp = False
        for rank, row in enumerate(df.iter_rows(named=True)):
            assert row['query_name'] == q, row['query_name']
            match_name = _display_name(row['match_name'])
            f_uniq_p = _percent(row['f_unique_weighted'])
            sum_sofar += row['f_unique_weighted']
            intersect_bp = _format_bp(row['intersect_bp'])

            if not args.filter or args.filter in row['match_name']:
                if not did_disp:
                    print(f"\nQuery: {_display_name(q)} ({_percent(total)} assigned in {len(df)} matches)")
                    did_disp = True
                print(f"  {rank+1}: {match_name} - {f_uniq_p} ({_percent(sum_sofar)}) - {intersect_bp}")
        if not did_disp:
            #print(f"(nothing to display for query '{q}')")
            pass
        else:
            print("")
    

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
    p_display.add_argument('-l', '--filter', help="filter matches on substring")
    p_display.add_argument('-n', '--display-num-results', help="display this many results", default=3, type=int)
    p_display.add_argument('-t', '--threshold', help='filter at this threshold', default=0.01, type=float)
    p_display.set_defaults(func=display)

    p_columns = subparsers.add_parser('columns', help='display columns')
    p_columns.add_argument('gather_csvs', nargs='+')
    p_columns.add_argument('-l', '--filter', help="filter on substring")
    p_columns.set_defaults(func=columns)
    
    args = p.parse_args()
    args.func(args)


if __name__ == '__main__':
    sys.exit(main())
