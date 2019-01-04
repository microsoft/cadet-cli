import click

@click.version_option('1.0.0')
@click.group()
def cadet():
	pass

if __name__ == '__main__':
	cadet()