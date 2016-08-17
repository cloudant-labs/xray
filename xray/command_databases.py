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
@click.option('--pretty-print', '-pp', is_flag=True, default=False)
@click.option('--shard-docs', '-qd', default=10000000, type=float, help='Recommended docs per shard.')
@click.option('--shard-size', '-qs', default=10, type=float, help='Recommended GB per shard.')
@click.option('--connections', '-con', default=100, help='Number of parallel connections to make to the server.')
def databases(obj, limit, pretty_print, shard_docs, shard_size, connections):
    all_dbs_resp = requests.get(obj['URL'] + '/_all_dbs')
    all_dbs = all_dbs_resp.json()
    shard_bytes = shard_size * 1073741824

    db_stats = []
    base_url = obj['URL']

    session = requests.session()

    db_stats = get_db_info(session, connections, base_url, all_dbs)

    ## sort and limit db_stats
    sorted_db_stats = sorted(db_stats, key=lambda x: x['doc_count'], reverse=True)
    sorted_db_stats = sorted_db_stats[:limit]

    sorted_db_stats = get_shard_data(session, connections, base_url, sorted_db_stats)
    add_recommended_q(sorted_db_stats, shard_docs, shard_bytes)
    click.echo('Recommended docs/shard: {0}'.format(millify(shard_docs)))
    click.echo('Recommended shard size: {0}GB'.format(shard_size))


    if limit > 0 and len(db_stats) > limit:
        click.echo('Showing {0} of {1} databases, '.format(limit, len(db_stats)) +
                   'sorted by document count descending.')
    else:
        click.echo('Showing all {0} databases, '.format(len(db_stats)) +
                   'sorted by document count descending.')

    table_headers = ['name',
                     'doc count',
                     'deleted doc count',
                     'db size',
                     'q',
                     'recommended q (count)',
                     'recommended q (size)']
    table = map(partial(format_stats, pretty_print), sorted_db_stats)
    click.echo('\n')
    click.echo(tabulate(table, headers=table_headers))


def process_requests(rs, connections, count, process_fun, ordered=False):
    errors = 0

    if ordered:
        request_iterator = grequests.map(rs, size=connections)
    else:
        request_iterator = grequests.imap(rs, size=connections)

    with click.progressbar(request_iterator, length=count) as bar:
        for index, r in enumerate(bar):
            if r.status_code is requests.codes.ok:
                process_fun(index, r)
            elif r.status_code is 404:
                # indicates database was deleted before we queried it
                continue
            elif r.status_code is 500:
                errors = errors + 1
                click.echo('500 error processing {0}. Continuing...' + r.url, err=True)
            else:
                r.raise_for_status()

    if errors > 0:
        click.echo('Failed to get data for {0} databases due to server errors'.format(errors))


def get_db_info(session, connections, base_url, all_dbs):
    db_stats = []
    urls = map(partial(get_stats_url, base_url), all_dbs)
    rs = (grequests.get(u, session=session) for u in urls)
    url_count = len(urls)

    click.echo('Fetching db info for {0} databases...'.format(url_count))

    def process_response(index, response):
        db_stats.append(response.json())

    process_requests(rs, connections, url_count, process_response, ordered=True)

    return db_stats


def get_shard_data(session, connections, base_url, db_stats):
    db_names = [db['db_name'] for db in db_stats]
    urls = map(partial(get_shards_url, base_url), db_names)
    rs = (grequests.get(u, session=session) for u in urls)
    url_count = len(urls)

    click.echo('Fetching shard counts for {0} databases...'.format(url_count))

    def process_response(index, response):
        q = len(response.json()['shards'])
        db_stats[index]['q'] = q

    process_requests(rs, connections, url_count, process_response)

    return db_stats


def millify(n):
    if n <= 0:
        return 0

    millnames = ['', 'k', 'M', 'B', 'T']
    millidx = max(0, min(len(millnames)-1,
                      int(math.floor(math.log10(abs(n))/3.0))))
    return '%.0f%s' % (n/10**(3*millidx), millnames[millidx])


def sizeof_fmt(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if abs(num) < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def get_shards_url(url, db):
    return url + '/' + db + '/_shards'


def get_stats_url(url, db):
    return url + '/' + db


def add_recommended_q(db_stats, recommended_shard_docs, recommended_shard_bytes):
    for s in db_stats:
        if 'q' in s:
            total_docs = s['doc_count'] + s['doc_del_count']
            size_bytes = s['other']['data_size']
            s['q_docs'] = math.ceil((total_docs + 1.0) / float(recommended_shard_docs))
            s['q_bytes'] = math.ceil((size_bytes + 1.0) / float(recommended_shard_bytes))


def format_stats(pretty_print, db_stats):
    doc_count = db_stats['doc_count']
    doc_del_count = db_stats['doc_del_count']
    size = db_stats['other']['data_size']

    if pretty_print:
        doc_count = millify(doc_count)
        doc_del_count = millify(doc_del_count)
        size = sizeof_fmt(size)

    return [db_stats['db_name'],
            doc_count,
            doc_del_count,
            size,
            db_stats['q'],
            db_stats['q_docs'],
            db_stats['q_bytes']]
