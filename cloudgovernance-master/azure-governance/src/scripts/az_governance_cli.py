import click
import common.utils

@click.group()
def cli():
    pass

@cli.command(name="run",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.option('--servicename', required=False,default=None,help='job name to test. If blank and test.ini file exists, will be taken from ini file')
#@click.option('--stop-on-failure/--no-stop-on-failure', required=False,default=False,help="whether or not to keep going if a test fails. default is to run all tests")
#@click.pass_context

def run(servicename):
    if servicename is not None:
       print("==========test started==========")
       common.utils.execute_service(servicename)
       # UserMgmt.get_user_mgmt()
    else:
        print("servicename is not given")

if __name__ == '__main__':
   cli()

