from . import sparql
import click
from click_aliases import ClickAliasedGroup
import logging

logger = logging.getLogger("oda")

@click.group(cls=ClickAliasedGroup)
@click.option("--log-level", default="INFO")
@click.option("-d", "--debug", is_flag=True)
def oda(log_level, debug):
    if debug:
        log_level = "DEBUG"

    log_level = log_level.upper()

    logging.basicConfig(level=log_level)
    logger.setLevel(log_level)
    logger.debug("starting oda client log level %s", log_level) 


@oda.command(aliases=["i","in","info"])
def info():
    logger.debug("oda info")


@oda.command(aliases=["ev","eva","eval"])
def evaluate():
    logger.debug("oda evaluate")


@oda.command("list")
def oda_list(aliases=[""]):
    logger.debug("oda list")

    r = sparql.select("?workflow a oda:workflow; ?p ?o; oda:domain ?domain .", "?workflow ?p ?o", tojdict=True)

    for workflow, d in r.items():
        logger.info("\033[32m%s\033[0m", workflow)
        for k, v in d.items():
            logger.info("   %s : %s", k, v)

oda.add_command(sparql.cli, 'sparql')

if __name__ == "__main__":
    oda()
