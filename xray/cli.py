import click
from command_databases import databases
from command_indexes import indexes


@click.group()
@click.option('--url', required=False)
@click.option('--source', type=click.File('r'), default=None, help='Use source URLs from the specified input file. Assumes one URL per line.')
@click.pass_context
def main(ctx, url, source):
    """Tool to investigate Cloudant/CouchDB cluster usage.

    \b
    example:
    $ xray --url https://user:password@user.cloudant.com databases
    """
    if source is None and url is None:
        raise click.UsageError('No URL specified')

    if source is None:
        ctx.obj = {'URLs': [url]}
    else:
        ctx.obj = {'URLs': map(lambda x: x.strip(), source.readlines())}

main.add_command(databases)
main.add_command(indexes)
