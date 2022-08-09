import click


@click.group()
def root():
    pass


@click.command()
def from_submissions():
    click.echo("would parse submissions")


@click.command()
def from_tsv():
    click.echo("would parse tsv")


@click.command()
def from_sqlite():
    click.echo("would parse sqlite")


root.add_command(from_submissions)
root.add_command(from_tsv)
root.add_command(from_sqlite)

if __name__ == "__main__":
    root()
