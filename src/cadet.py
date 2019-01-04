import click

@click.version_option('1.0.0')
@click.group()
def cli():
	pass

if __name__ == '__main__':
	cli()