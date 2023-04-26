import asyncio
from ssl import SSLCertVerificationError, SSLError

import click
import httpx
import logging
from aiofile import AIOFile, Writer
from httpx import ReadTimeout, ConnectError, ConnectTimeout, ReadError, TooManyRedirects, RemoteProtocolError


async def _prepare_url(url: str) -> str:
    if url.startswith("ark:"):
        return f"https://n2t.net/{url}"
    else:
        return url


async def _main(output_file: str, conn_count: int, link_set: set[str], timeout: int):
    async with AIOFile(output_file, "w") as aiodf:
        writer = Writer(aiodf)
        header = """link,status,notes\n"""
        await writer(header)
        await aiodf.fsync()

        count = 0
        headers = {"User-Agent": "iSamples-Link-Checker/0.0.1"}
        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=conn_count), follow_redirects=True, timeout=timeout, headers=headers) as client:
            for url in link_set:
                count += 1
                if count % 10 == 0:
                    logging.debug(f"Processed {count} records")
                try:
                    req = client.build_request("GET", await _prepare_url(url))
                    response = await client.send(req, stream=True)
                    if 200 <= response.status_code < 300:
                        line = f"{url},{response.status_code},SUCCESS"
                    else:
                        text = await response.aread()
                        line = f"{url},{response.status_code},{text[:100]}"
                    await response.aclose()
                except SSLCertVerificationError as e:
                    line = f"{url},-1,{e.reason}"
                except ConnectTimeout:
                    line = f"{url},-1,Connect timed out after {timeout} seconds"
                except ReadTimeout:
                    line = f"{url},-1,Read timed out after {timeout} seconds"
                except ConnectError as c:
                    line = f"{url},-1,{c.__cause__}"
                except ValueError as v:
                    line = f"{url},-1,{v.__cause__}"
                except ReadError as r:
                    line = f"{url},-1,{r.__cause__}"
                except TooManyRedirects as r:
                    line = f"{url},-1,{r.__cause__}"
                except RemoteProtocolError as rpe:
                    line = f"{url},-1,{rpe.__cause__}"
                except SSLError as ssle:
                    line = f"{url},-1,{ssle.__cause__}"
                await writer(line)
                await writer("\n")
                await aiodf.fsync()


@click.command()
@click.option("-i", "--input_file", help="Input file, one link per line.")
@click.option("-o", "--output_file", help="Output file, CSV formatted.")
@click.option("-c", "--concurrent_requests", default=50, help="The max number of concurrent requests to allow.")
@click.option("-t", "--timeout", default=5, help="The number of seconds to wait before timing out.")
def main(input_file: str, output_file: str, concurrent_requests: int, timeout: int):
    """Program that takes a list of links in a file as input and writes a CSV file with the result of checking
    those links."""
    link_set = set()
    with open(input_file, "r") as input_file:
        for line in input_file:
            stripped = line.strip()
            if len(stripped) > 0:
                link_set.add(stripped)
    asyncio.run(_main(output_file, concurrent_requests, link_set, timeout))


if __name__ == '__main__':
    main()
