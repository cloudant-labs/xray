import click
import requests
import grequests
import math
from functools import partial
from tabulate import tabulate


@click.command()
@click.pass_obj
@click.option('--limit', '-l', default=50, help='Limit results. Set to 0 for all.')
@click.option('--pretty-print', '-pp', is_flag=True, default=False)
@click.option('--connections', '-con', default=100, help='Number of parallel connections to make to the server.')
def indexes(obj, limit, pretty_print, connections):
    ctx = obj
    ctx['pretty_print'] = pretty_print
    ctx['connections'] = connections

    # are we hitting one db or all?
    r = requests.get(obj['URL'])
    r.raise_for_status()

    is_db = 'db_name' in r.json()
    if is_db:
        all_dbs = [r.json()['db_name']]
    else:
        all_dbs_resp = requests.get(obj['URL'] + '/_all_dbs')
        all_dbs = all_dbs_resp.json()

    ctx['session'] = requests.session()

    index_stats = get_index_data(ctx, all_dbs)
    sorted_index_stats = index_stats[:limit]

    table_headers = (['db name', 'ddoc', 'type', 'index name'])

    if limit > 0 and len(index_stats) > limit:
        click.echo('Showing {0} of {1} indexes, '.format(limit, len(index_stats)))
    else:
        click.echo('Showing all {0} indexes, '.format(len(index_stats)))

    table = map(partial(format_stats, ctx), sorted_index_stats)
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
        click.echo('Failed to get data for {0} requests due to server errors'.format(errors))


def get_ddocs_url(url, db):
    return '{0}/{1}/_all_docs?startkey=%22_design%252F%22&endkey=%22_design0%22&include_docs=true'.format(url, db)


def get_index_data(ctx, db_names):
    urls = map(partial(get_ddocs_url, ctx['URL']), db_names)
    rs = (grequests.get(u, session=ctx['session']) for u in urls)
    url_count = len(urls)
    result = []

    click.echo('Fetching index stats for {0} databases...'.format(url_count))

    # ['db name', 'ddoc', 'type', 'index name']
    def process_response(index, response):
        design_docs = response.json()['rows']

        for row in design_docs:
            doc = row['doc']
            is_query = False
            indexes = []

            if 'language' in doc and doc['language'] == 'query':
                is_query = True


            if 'views' in doc:
                for view in doc['views']:
                    result.append({
                        'db_name': db_names[index],
                        'ddoc': doc['_id'],
                        'type': 'CQ JSON' if is_query else 'view',
                        'name': view
                    })

                # if is_query:
                #     query_views = query_views + len(doc['views'])
                #     query_view_groups = query_view_groups + 1
                # else:
                #     views = views + len(doc['views'])
                #     view_groups = view_groups + 1

            if 'indexes' in doc:
                for i in doc['indexes']:
                    result.append({
                        'db_name': db_names[index],
                        'ddoc': doc['_id'],
                        'type': 'CQ Text' if is_query else 'search',
                        'name': i
                    })

                # if is_query:
                #     query_search = query_search + len(doc['indexes'])
                # else:
                #     search = search + len(doc['indexes'])

            if 'st_indexes' in doc:
                for g in doc['st_indexes']:
                    result.append({
                        'db_name': db_names[index],
                        'ddoc': doc['_id'],
                        'type': 'geo',
                        'name': g
                    })

        return result

    process_requests(ctx, rs, url_count, process_response, ordered=True)

    return result


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


def format_stats(ctx, index_stats):

    result = [index_stats['db_name'],
              index_stats['ddoc'],
              index_stats['type'],
              index_stats['name']]

    return result
