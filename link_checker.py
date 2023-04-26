import asyncio
import click
import httpx
from aiofile import AIOFile, Writer


async def _prepare_url(url: str) -> str:
    if url.startswith("ark:"):
        return f"https://n2t.net/{url}"
    else:
        return url


async def _main(output_file: str, conn_count: int, link_list: list[str]):
    async with AIOFile(output_file, "w") as aiodf:
        writer = Writer(aiodf)
        header = """link,status,notes\n"""
        await writer(header)
        await aiodf.fsync()

        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=conn_count), follow_redirects=True) as client:
            for url in link_list:
                req = client.build_request("GET", await _prepare_url(url))
                response = await client.send(req, stream=True)
                if 200 <= response.status_code < 300:
                    line = f"{url},{response.status_code},SUCCESS"
                else:
                    text = await response.aread()
                    line = f"{url},{response.status_code},{text}"
                await response.aclose()
                await writer(line)
                await writer("\n")
                await aiodf.fsync()


@click.command()
@click.option("-i", "--input_file", help="Input file, one link per line.")
@click.option("-o", "--output_file", help="Output file")
@click.option("-c", "--concurrent_requests", default=50, help="The max number of concurrent requests to allow")
def main(input_file: str, output_file: str, concurrent_requests: int):
    """Program that takes a list of links in a file as input and writes a CSV file with the result of checking
    those links."""
    link_list = []
    with open(input_file, "r") as input_file:
        for line in input_file:
            stripped = line.strip()
            if len(stripped) > 0:
                link_list.append(stripped)
    asyncio.run(_main(output_file, concurrent_requests, link_list))


if __name__ == '__main__':
    main()
