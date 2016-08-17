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
@click.option('--connections', '-con', default=100, help='Number of parallel connections to make to the server.')
def databases(obj, limit, connections):
    all_dbs_resp = requests.get(obj['URL'] + '/_all_dbs')
    all_dbs = all_dbs_resp.json()

    db_stats = []

    urls = map(partial(get_stats_url, obj['URL']), all_dbs)
    session = requests.session()
    rs = (grequests.get(u, session=session) for u in urls)
    errors = 0

    with click.progressbar(grequests.imap(rs, size=connections),
                       length=len(all_dbs)) as bar:
        for r in bar:
            if r.status_code is requests.codes.ok:
                db_stats.append(get_stats(r))
            elif r.status_code is 404:
                # indicates database was deleted before we queried it
                continue
            elif r.status_code is 500:
                errors = errors + 1
                click.echo('500 error processing {0}. Continuing...' + r.url, err=True)
            else:
                r.raise_for_status()

    table_headers = ["name",
                     "doc count",
                     "doc count (friendly)",
                     "db size (bytes)",
                     "db size"]

    ## sort and limit db_stats
    sorted_db_stats = sorted(db_stats, key=lambda x: x[1], reverse=True)

    click.echo('Failed to get data for {0} databases due to server errors'.format(errors))

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
