import click

from dynaconf import Dynaconf, Validator
from os import path, environ

settings = Dynaconf(
    settings_files=[   
        path.join(environ["HOME"], ".odakb/settings.toml"),
        path.join(environ["HOME"], ".odakb/private.toml"),
    ],

    environments=False,
    load_dotenv=False,

    envvar_prefix="ODA",             # variables exported as `ODAKB_FOO=bar` becomes `settings.FOO == "bar"`
)


# -- Lets add some Validation and Defaults
settings.validators.register(
    #Validator("ODA_SPARQL_ROOT", default=""),
    #Validator("DB.PORT",gte=8000, lte=9000, env="production"),


    # Defaults can also be used to define computed values if default=a_callable
    #Validator("DB.TIMEOUT", default=lambda _settings, _value: 24 * 60 * _settings.factor),

    # You can compound validators for better meaning
    #Validator("DB.USER", ne="pgadmin") & Validator("DB.USER", ne="master"),
)

settings.validators.validate()

@click.group()
def cli():
    pass

@cli.command()
def inspect():
    click.echo(settings)
    click.echo(settings.sparql_root)
    click.echo(settings.jena_password)

if __name__ == "__main__":
    cli()