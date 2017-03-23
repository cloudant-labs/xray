import click
import requests
import grequests
import math
from urlparse import urlparse
from functools import partial
from tabulate import tabulate
import csv


@click.command()
@click.pass_obj
@click.option('--limit', '-l', default=50, help='Limit results. Set to 0 for all.')
@click.option('--pretty-print', '-pp', is_flag=True, default=False)
@click.option('--ddocs', '-dd', is_flag=True, default=False, help='Show ddoc stats / index counts per db. Results are listed as views, view_groups, search, geo, query views, query view groups, query search, validate_doc_update, update handlers')
@click.option('--shards', '-s', is_flag=True, default=False, help='Show shard counts per db.')
@click.option('--shard-docs', '-qd', default=10000000, type=float, help='Recommended docs per shard.')
@click.option('--shard-size', '-qs', default=10, type=float, help='Recommended GB per shard.')
@click.option('--connections', '-con', default=100, help='Number of parallel connections to make to the server.')
@click.option('--output', '-o', type=click.Path(), default=None, help='Output to the specified csv')
def databases(obj, limit, pretty_print, ddocs, shards, shard_docs, shard_size, connections, output):
    ctx = obj
    ctx['shards'] = shards
    ctx['pretty_print'] = pretty_print
    ctx['connections'] = connections

    all_dbs = []
    for root_url in obj['URLs']:
        all_dbs = all_dbs + get_database_list(root_url)

    ctx['shard_docs'] = float(shard_docs)
    ctx['shard_bytes'] = float(shard_size * 1073741824)

    db_stats = []
    ctx['session'] = requests.session()

    db_stats = get_db_info(ctx, all_dbs)

    ## sort and limit db_stats
    sorted_db_stats = sorted(db_stats, key=lambda x: x['doc_count'] + x['doc_del_count'], reverse=True)

    if limit > 0:
        sorted_db_stats = sorted_db_stats[:limit]

    short_headers = ['host','cloudant backend','db',
                     'docs (total/active/deleted)',
                     'db size']
    expanded_headers = ['host','cloudant backend','db',
                     'total docs', 'active docs', 'deleted docs',
                     'db size (bytes)', 'db size']

    # get sharding info for each database
    if shards:
        click.echo('Recommended docs/shard: {0}'.format(millify(shard_docs)))
        click.echo('Recommended shard size: {0}GB'.format(shard_size))

        short_headers.extend(['q', 'recommended q (by count/size)'])
        expanded_headers.extend(['q', 'recommended q by count', 'recommended q by size)'])
        sorted_db_stats = get_shard_data(ctx, sorted_db_stats)
        add_recommended_q(ctx, sorted_db_stats)

    if ddocs:
        short_headers.extend(['v/vg/s/g/qv/qvg/qs', 'vdu/uh'])
        expanded_headers.extend(['views', 'view groups', 'search', 'geo', 'CQ json' ,'CQ view groups', 'CQ text', 'VDUs', 'update handlers'])
        sorted_db_stats = get_index_data(ctx, sorted_db_stats)

    if output is None:
        if limit > 0 and len(db_stats) > limit:
            click.echo('Showing {0} of {1} databases, '.format(limit, len(db_stats)) +
                       'sorted by document count descending.')
        else:
            click.echo('Showing all {0} databases, '.format(len(db_stats)) +
                       'sorted by document count descending.')

        table = map(partial(format_stats, ctx), sorted_db_stats)
        click.echo('\n')
        click.echo(tabulate(table, headers=short_headers))
    else:
        table = map(partial(format_stats_expanded, ctx), sorted_db_stats)
        with open(output, 'wb') as csvfile:
            writer = csv.writer(csvfile, dialect='excel')
            writer.writerow(expanded_headers)
            writer.writerows(table)


def get_database_list(root_url):
    all_dbs_resp = requests.get(root_url + '/_all_dbs')
    all_dbs = all_dbs_resp.json()
    host = urlparse(root_url).hostname
    return map(lambda x: {'url': root_url + '/' + x,
                          'name': x,
                          'host': host}, all_dbs)


def process_requests(ctx, rs, count, process_fun):
    errors = 0

    request_iterator = grequests.imap(rs, size=ctx['connections'])

    with click.progressbar(request_iterator, length=count) as bar:
        for r in bar:
            if r.status_code is requests.codes.ok:
                process_fun(r)
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


def find_by_db(dbs, name):
    for index, item in enumerate(dbs):
        if item['name'] == name:
            return item


def get_db_info(ctx, all_dbs):
    db_stats = all_dbs[:]
    rs = (grequests.get(d['url'],
                        session=ctx['session'],
                        headers=dict(db_name=d['name'])) for d in db_stats)
    url_count = len(db_stats)

    click.echo('Fetching db info for {0} databases...'.format(url_count))

    def process_response(response):
        metadata = response.json()
        if 'X-Cloudant-Backend' in response.headers:
            metadata['backend'] = response.headers['X-Cloudant-Backend']
        else:
            metadata['backend'] = None

        # hack to associate response with a db
        db_name = response.request.headers['db_name']
        find_by_db(db_stats, db_name).update(metadata)

    process_requests(ctx, rs, url_count, process_response)

    return db_stats


def get_shards_url(db):
    return '{0}/_shards'.format(db['url'])


def get_shard_data(ctx, db_stats):
    rs = (grequests.get(get_shards_url(d),
                        session=ctx['session'],
                        headers=dict(db_name=d['name'])) for d in db_stats)

    url_count = len(db_stats)

    click.echo('Fetching shard counts for {0} databases...'.format(url_count))

    def process_response(response):
        q = len(response.json()['shards'])

        # hack to associate response with a db
        db_name = response.request.headers['db_name']
        find_by_db(db_stats, db_name)['q'] = q

    process_requests(ctx, rs, url_count, process_response)

    return db_stats


def get_ddocs_url(db):
    return '{0}/_all_docs?startkey=%22_design%252F%22&endkey=%22_design0%22&include_docs=true'.format(db['url'])


def get_index_data(ctx, db_stats):
    rs = (grequests.get(get_ddocs_url(d),
                        session=ctx['session'],
                        headers=dict(db_name=d['name'])) for d in db_stats)
    url_count = len(db_stats)

    click.echo('Fetching index stats for {0} databases...'.format(url_count))

    def process_response(response):
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

            if 'language' in doc and doc['language'] == 'query':
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

        # hack to associate response with a db
        db_name = response.request.headers['db_name']
        find_by_db(db_stats, db_name)['indexes'] = {
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

    process_requests(ctx, rs, url_count, process_response)

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

    result = [db_stats['host'], db_stats['backend'], db_stats['name'],
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


def format_stats_expanded(ctx, db_stats):
    doc_count = db_stats['doc_count']
    doc_del_count = db_stats['doc_del_count']
    doc_count_total = doc_count + doc_del_count
    size = db_stats['other']['data_size']

    result = [db_stats['host'], db_stats['backend'], db_stats['name'],
              doc_count_total, doc_count, doc_del_count,
              size, sizeof_fmt(size)]

    if ctx['shards']:
        result.extend([
            db_stats['q'],
            int(db_stats['q_docs']),
            int(db_stats['q_bytes'])])

    if 'indexes' in db_stats:
        indexes = db_stats['indexes']
        result.extend([indexes['views'],
            indexes['view_groups'],
            indexes['search'],
            indexes['geo'],
            indexes['query_views'],
            indexes['query_view_groups'],
            indexes['query_search'],
            indexes['validate_doc_updates'],
            indexes['update_handlers']])

    return result
