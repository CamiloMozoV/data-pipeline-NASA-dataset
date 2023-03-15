#! /usr/bin/env python
from urllib import request, error
import click

@click.command()
@click.argument("url", type=str)
@click.option(
    "--filename",
    default="/tmp/temp-file.csv",
    type=click.Path(dir_okay=False, writable=True),
    help="Optional file to write output to."
)
def fetch_data_nasa(url: str, filename: str):
    """CLI application for fetching weather forecasts from NASA
    CSV format.
    
    Parameter:
    
    - `url` (str)
    """
    try:
        request.urlretrieve(url=url, filename=f"/tmp/{filename}")
    except error.HTTPError:
        print(f"{error.HTTPError.reason}")

if __name__=="__main__":
    fetch_data_nasa()