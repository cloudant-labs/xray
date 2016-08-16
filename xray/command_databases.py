import click

@click.command()
@click.pass_obj
@click.option('--limit', '-l', default=50, help='Limit results. Set to 0 for all.')
def databases(obj, limit):
    click.echo('{0}, {1}.'.format(obj['URL'], limit))
