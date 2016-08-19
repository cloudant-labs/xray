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
@click.option('--ddocs', '-dd', is_flag=True, default=False, help='Show ddoc stats / index counts per db. Results are listed as views, view_groups, search, geo, query views, query view groups, query search, validate_doc_update, update handlers')
@click.option('--shards', '-s', is_flag=True, default=False, help='Show shard counts per db.')
@click.option('--shard-docs', '-qd', default=10000000, type=float, help='Recommended docs per shard.')
@click.option('--shard-size', '-qs', default=10, type=float, help='Recommended GB per shard.')
@click.option('--connections', '-con', default=100, help='Number of parallel connections to make to the server.')
def databases(obj, limit, pretty_print, ddocs, shards, shard_docs, shard_size, connections):
    ctx = obj
    ctx['shards'] = shards
    ctx['pretty_print'] = pretty_print
    ctx['connections'] = connections

    all_dbs_resp = requests.get(obj['URL'] + '/_all_dbs')
    all_dbs = all_dbs_resp.json()

    ctx['shard_docs'] = float(shard_docs)
    ctx['shard_bytes'] = float(shard_size * 1073741824)

    db_stats = []
    ctx['session'] = requests.session()

    db_stats = get_db_info(ctx, all_dbs)

    ## sort and limit db_stats
    sorted_db_stats = sorted(db_stats, key=lambda x: x['doc_count'] + x['doc_del_count'], reverse=True)
    sorted_db_stats = sorted_db_stats[:limit]

    table_headers = ['name',
                     'docs (total/active/deleted)',
                     'db size']

    # get sharding info for each database
    if shards:
        click.echo('Recommended docs/shard: {0}'.format(millify(shard_docs)))
        click.echo('Recommended shard size: {0}GB'.format(shard_size))

        table_headers.extend(['q', 'recommended q (by count/size)'])
        sorted_db_stats = get_shard_data(ctx, sorted_db_stats)
        add_recommended_q(ctx, sorted_db_stats)

    if ddocs:
        table_headers.extend(['v/vg/s/g/qv/qvg/qs', 'vdu/uh'])
        sorted_db_stats = get_index_data(ctx, sorted_db_stats)

    if limit > 0 and len(db_stats) > limit:
        click.echo('Showing {0} of {1} databases, '.format(limit, len(db_stats)) +
                   'sorted by document count descending.')
    else:
        click.echo('Showing all {0} databases, '.format(len(db_stats)) +
                   'sorted by document count descending.')

    table = map(partial(format_stats, ctx), sorted_db_stats)
    click.echo('\n')
    click.echo(tabulate(table, headers=table_headers))


def process_requests(ctx, rs, count, process_fun, ordered=False):
    errors = 0
    index = -1

    if ordered:
        request_iterator = grequests.map(rs, size=ctx['connections'])
    else:
        request_iterator = grequests.imap(rs, size=ctx['connections'])

    with click.progressbar(request_iterator, length=count) as bar:
        for r in bar:
            index = index + 1
            if r.status_code is requests.codes.ok:
                process_fun(index, r)
            elif r.status_code is 404:
                # indicates database was deleted before we queried it
                continue
            elif r.status_code is 500:
                errors = errors + 1
                click.echo('500 error processing {0}. Continuing...' + r.url, err=True)
            else:
                click.echo(r.status_code)
                r.raise_for_status()

    if errors > 0:
        click.echo('Failed to get data for {0} databases due to server errors'.format(errors))


def get_db_info_url(url, db):
    return '{0}/{1}'.format(url, db)


def get_db_info(ctx, all_dbs):
    db_stats = []
    urls = map(partial(get_db_info_url, ctx['URL']), all_dbs)
    rs = (grequests.get(u, session=ctx['session']) for u in urls)
    url_count = len(urls)

    click.echo('Fetching db info for {0} databases...'.format(url_count))

    def process_response(index, response):
        db_stats.append(response.json())

    process_requests(ctx, rs, url_count, process_response)

    return db_stats


def get_shards_url(url, db):
    return '{0}/{1}/_shards'.format(url, db)


def get_shard_data(ctx, db_stats):
    db_names = [db['db_name'] for db in db_stats]
    urls = map(partial(get_shards_url, ctx['URL']), db_names)
    rs = (grequests.get(u, session=ctx['session']) for u in urls)
    url_count = len(urls)

    click.echo('Fetching shard counts for {0} databases...'.format(url_count))

    def process_response(index, response):
        q = len(response.json()['shards'])
        db_stats[index]['q'] = q

    process_requests(ctx, rs, url_count, process_response, ordered=True)

    return db_stats


def get_ddocs_url(url, db):
    return '{0}/{1}/_all_docs?startkey=%22_design%252F%22&endkey=%22_design0%22&include_docs=true'.format(url, db)


def get_index_data(ctx, db_stats):
    db_names = [db['db_name'] for db in db_stats]
    urls = map(partial(get_ddocs_url, ctx['URL']), db_names)
    rs = (grequests.get(u, session=ctx['session']) for u in urls)
    url_count = len(urls)

    click.echo('Fetching index stats for {0} databases...'.format(url_count))

    def process_response(index, response):
        design_docs = response.json()['rows']
        views = 0
        view_groups = 0
        search = 0
        geo = 0
        query_views = 0
        query_view_groups = 0
        query_search = 0
        vdu = 0
        uh = 0

        for row in design_docs:
            doc = row['doc']
            is_query = False

            if 'language' in doc and doc['language'] is 'query':
                is_query = True

            if 'views' in doc:
                if is_query:
                    query_views = query_views + len(doc['views'])
                    query_view_groups = query_view_groups + 1
                else:
                    views = views + len(doc['views'])
                    view_groups = view_groups + 1

            if 'indexes' in doc:
                if is_query:
                    query_search = query_search + len(doc['indexes'])
                else:
                    search = search + len(doc['indexes'])

            if 'st_indexes' in doc:
                geo = geo + len(doc['st_indexes'])

            if 'updates' in doc:
                uh = uh + len(doc['updates'])

            if 'validate_doc_update' in doc:
                vdu = vdu + 1

        db_stats[index]['indexes'] = {
            'views': views,
            'view_groups': view_groups,
            'search': search,
            'geo': geo,
            'query_views': query_views,
            'query_view_groups': query_view_groups,
            'query_search': query_search,
            'validate_doc_updates': vdu,
            'update_handlers': uh
        }

    process_requests(ctx, rs, url_count, process_response, ordered=True)

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
            return '%3.1f %s' % (num, x)
        num /= 1024.0


def add_recommended_q(ctx, db_stats):
    for s in db_stats:
        if 'q' in s:
            total_docs = s['doc_count'] + s['doc_del_count']
            size_bytes = s['other']['data_size']
            s['q_docs'] = math.ceil((total_docs + 1.0) / ctx['shard_docs'])
            s['q_bytes'] = math.ceil((size_bytes + 1.0) / ctx['shard_bytes'])


def format_stats(ctx, db_stats):
    doc_count = db_stats['doc_count']
    doc_del_count = db_stats['doc_del_count']
    doc_count_total = doc_count + doc_del_count
    size = db_stats['other']['data_size']

    if ctx['pretty_print']:
        doc_count_total = millify(doc_count_total)
        doc_count = millify(doc_count)
        doc_del_count = millify(doc_del_count)
        size = sizeof_fmt(size)

    result = [db_stats['db_name'],
              '{0} / {1} / {2}'.format(doc_count_total, doc_count, doc_del_count),
              size]

    if ctx['shards']:
        result.extend([
            db_stats['q'],
            '{0}/{1}'.format(int(db_stats['q_docs']), int(db_stats['q_bytes']))])

    if 'indexes' in db_stats:
        indexes = db_stats['indexes']
        result.append('{0}/{1}/{2}/{3}/{4}/{5}/{6}'.format(indexes['views'],
            indexes['view_groups'],
            indexes['search'],
            indexes['geo'],
            indexes['query_views'],
            indexes['query_view_groups'],
            indexes['query_search']))
        result.append('{0}/{1}'.format(
            indexes['validate_doc_updates'],
            indexes['update_handlers']))

    return result
