import click
from click_aliases import ClickAliasedGroup


@click.group(cls=ClickAliasedGroup)
def oda():
    pass

@oda.command(aliases=["i","in","info"])
def info():
    click.echo("oda evaluate")

@oda.command(aliases=["ev","eva","eval"])
def evaluate():
    click.echo("oda evaluate repl")

@oda.command()
def oda_list(aliases=[""]):
    click.echo("oda evaluate")


if __name__ == "__main__":
    oda()
