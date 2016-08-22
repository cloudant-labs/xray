import click
from command_databases import databases
from command_indexes import indexes


@click.group()
@click.argument('url', required=True)
@click.pass_context
def main(ctx, url):
    """Tool to investigate Cloudant/CouchDB cluster usage.

    \b
    example:
    $ xray https://user:password@user.cloudant.com --databases
    """
    ctx.obj = {'URL': url}

main.add_command(databases)
main.add_command(indexes)