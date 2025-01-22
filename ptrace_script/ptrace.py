from calc_events_stats import get_statistics as polars_calc_event_stats
from filter_trans_id import main_polars as polars_filter_trans_id
from list_events import main_polars as polars_list_events
from list_longest_spans import main as polars_list_longest_spans
from list_root_traces import main as polars_list_root_traces
from show_overlap import main as polars_show_overlap
from show_span import main as polars_show_span
import polars as pl
from argparse import ArgumentParser, RawDescriptionHelpFormatter


import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter


class PtraceParser():
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="command")
    subparsers.cmds = []
    
    def __init__(self):
        
        self.parser.add_argument('-f', '--file', type=str, help='File to process.')
        self.parser.add_argument('-r', '--rows', type=int, default=1000, help='Number of rows to display.')
        
        l = [(f"{n} {s}",h) for n,s,h in self.subparsers.cmds]
        maxl = max(len(s) for s,_ in l)
        import textwrap
        self.parser.epilog = textwrap.dedent(
            "commands arguments: (-h for details)\n"+
            "\n".join([f"  {s:<{maxl}}   {h}" for s,h in l])
            )
        
    @classmethod
    def add_parser(cls, func, arguments=[], help=''):
        name = func.__name__
        parser = cls.subparsers.add_parser(name, description=func.__doc__)
        for t, args, kwargs in arguments:
            if t == 'a':
                parser.add_argument(*args, **kwargs)
            elif t=='m':
                g = parser.add_mutually_exclusive_group(**kwargs)
                for _,a,kw in args:
                    g.add_argument(*a, **kw)
        parser.set_defaults(func=func)
        def strip_prefix(s):
            return s[len(parser.prog)+8:].rstrip()
        cls.subparsers.cmds.append((name, strip_prefix(parser.format_usage()),
                           help))

def argument(*name_or_flags, **kwargs):
    return ('a', list(name_or_flags), kwargs)
def mutex(arguments, **kwargs):
    return ('m', arguments, kwargs)

def command(arguments=[], help=''):
    def decorator(func):
        PtraceParser.add_parser(func, arguments, help)
    return decorator

@command(arguments=[], help='Calculate event statistics.')
def calc_event_stats(args):
    """
    Calculating event stats
    """
    progress_trace = pl.scan_csv(args.file)
    print_progress_trace(polars_calc_event_stats(progress_trace))

@command(arguments=[argument('-t', '--tid', 
                                dest='transid',
                                type=int,
                                help='Transaction id to filter..'),
                       argument('-o', '--output', 
                                type=str,
                                help='File to write result to.')], 
            help='View transaction')
def filter_trans_id(args):
    """
    View a specific transactions events, based on transaction ID.
    """
    progress_trace = pl.scan_csv(args.file)
    result = polars_filter_trans_id(progress_trace, args.transid)
    if args.output:
        result.write_csv(args.output, separator=',')
    else:
        print_progress_trace(result)
    
@command(arguments=[argument('-n', '--negate', 
                                action='store_true',
                                default=False,
                                help='Negate/comment events.')], 
            help='List event occurrences.')
def list_events(args):
    """
    List all events in the trace, with the number of occurrences.
    """
    progress_trace = pl.scan_csv(args.file)
    polars_list_events(progress_trace, args.negate)

#TODO: Support show-spans, find-spans and hide-rows ?
@command(arguments=[argument('-e', '--event', 
                                type=str,
                                help='Event name'),
                       argument('--show-spans', 
                                action="store_true", 
                                default=False,
                                help='Show spans when there is an overlap'),
                       argument('--find-spans', 
                                action="store_true", 
                                default=False,
                                help='Find overlapping spans'),
                       argument('--hide-rows', 
                                action="store_true", 
                                default=False,
                                help='Hide rows')], 
            help='List event span durations.')
def list_longest_spans(args):
    """
    List event span durations, ordered by duration.
    """
    progress_trace = pl.scan_csv(args.file)
    result = polars_list_longest_spans(progress_trace, args.event)
    print_progress_trace(result)

#TODO: Support show-spans, find-spans and hide-rows ?
#TODO: List root spans, instead of traces?
@command(arguments=[argument('-e', '--event', 
                                type=str,
                                help='Event name'), 
                       argument('--show-spans', 
                                action="store_true", 
                                default=False,
                                help='Show spans when there is an overlap'),
                       argument('--find-spans', 
                                action="store_true", 
                                default=False,
                                help='Find overlapping spans'),
                       argument('--hide-rows', 
                                action="store_true", 
                                default=False,
                                help='Hide rows')], 
            help='List root traces')    
def list_root_traces(args):
    """
    List all root spans, i.e. spans without a parent.
    """
    progress_trace = pl.scan_csv(args.file)
    result = polars_list_root_traces(progress_trace, args.event)
    print_progress_trace(result)

@command(arguments=[argument('-e', '--event',
                                required=True,
                                type=str,
                                help='Event name'),
                        argument('--show-spans', 
                                action="store_true", 
                                default=False,
                                help='Show spans when there is an overlap'),
                       argument('--find-spans', 
                                action="store_true", 
                                default=False,
                                help='Find overlapping spans'),
                       argument('--hide-rows', 
                                action="store_true", 
                                default=False,
                                help='Hide rows')]
            , help='Show overlapping events.')
def show_overlap(args):
    """
    List overlapping events, with each timestamp the overlap occurs.
    """
    progress_trace = pl.scan_csv(args.file)
    polars_show_overlap(progress_trace, args.hide_rows, args.show_spans, args.find_spans, args.event)

@command(arguments=[argument('-s', '--span',
                                required=True,
                                type=str,
                                help='Span ID')], 
            help='Show span.')
def show_span(args):
    """
    Show a span and all of its child events.
    """
    progress_trace = pl.scan_csv(args.file)
    result = polars_show_span(progress_trace, args.span)
    print_progress_trace(result)
    
def print_progress_trace(pt):
    pl.Config().set_tbl_rows(n_rows)
    with pl.Config(tbl_cols=-1):
        print(pt)

def main():
    my_parser = PtraceParser()
    args = my_parser.parser.parse_args()
    global n_rows
    n_rows = args.rows
    if args.command is None:
        my_parser.parser.print_help()
    else:
        args.func(args)
    sys.exit()
    
if __name__ == '__main__':
    main()
