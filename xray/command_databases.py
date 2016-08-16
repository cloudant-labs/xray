import click
import requests
import grequests
import math
from functools import partial
from tabulate import tabulate

# number of databases
# top N databases by size, doc count, indexes


@click.command()
@click.pass_obj
@click.option('--limit', '-l', default=50, help='Limit results. Set to 0 for all.')
def databases(obj, limit):
    all_dbs_resp = requests.get(obj['URL'] + '/_all_dbs')
    all_dbs = all_dbs_resp.json()

    db_stats = []

    urls = map(partial(get_stats_url, obj['URL']), all_dbs)
    rs = (grequests.get(u) for u in urls)

    for r in grequests.imap(rs, size=1000):
        r.raise_for_status()
        db_stats.append(get_stats(r))

    table_headers = ["name",
                     "doc count",
                     "doc count (friendly)",
                     "db size (bytes)",
                     "db size"]

    ## sort and limit db_stats
    sorted_db_stats = sorted(db_stats, key=lambda x: x[1], reverse=True)

    if limit > 0 and len(db_stats) > limit:
        click.echo('Showing {0} of {1} databases, '.format(limit, len(db_stats)) +
                   'sorted by document count descending.')
        sorted_db_stats = sorted_db_stats[:limit]
    else:
        click.echo('Showing all {0} databases, '.format(len(db_stats)) +
                   'sorted by document count descending.')

    click.echo('\n')
    click.echo(tabulate(sorted_db_stats, headers=table_headers))


def millify(n):
    if n <= 0:
        return 0

    millnames = ['', 'k', 'M', 'B', 'T']
    millidx = max(0, min(len(millnames)-1,
                      int(math.floor(math.log10(abs(n))/3.0))))
    return '%.0f%s' % (n/10**(3*millidx), millnames[millidx])


def sizeof_fmt(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def get_stats_url(url, db):
    return url + '/' + db


def format_table(db_stats):
    for db in db_stats:
        click.echo('{0}: ')


def get_stats(db_stats_resp):
    db_stats = db_stats_resp.json()

    doc_count = db_stats["doc_count"]
    doc_del_count = db_stats["doc_del_count"]
    size_bytes = db_stats["other"]["data_size"]

    return [db_stats["db_name"],
            doc_count + doc_del_count,
            millify(doc_count),
            size_bytes,
            sizeof_fmt(size_bytes)]
