import asyncio
import click
import httpx
from aiofile import AIOFile, Writer
from httpx import Response, AsyncClient
from bs4 import BeautifulSoup


async def _prepare_url(url: str) -> str:
    if url.startswith("ark:"):
        return f"https://n2t.net/{url}"
    else:
        return url


async def _parse_redirect_link(redirect_text: str) -> str:
    soup = BeautifulSoup(redirect_text, "html.parser")
    first_link = soup.find("a")["href"]
    return first_link




# async def _follow_link(url: str, client: AsyncClient) -> Response:
#     """Follows a link until it returns successfully (in the 200s) or errors out"""
#     req = client.build_request("GET", await _prepare_url(url))
#     response = await client.send(req, stream=True)
#     if 200 <= response.status_code < 300:
#         return response
#     elif response.status_code == 302:
#         response.text

async def _main(output_file: str, conn_count: int, link_list: list[str]):
    async with AIOFile(output_file, "w") as aiodf:
        writer = Writer(aiodf)
        header = """link,status,notes\n"""
        await writer(header)
        await aiodf.fsync()

        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=conn_count)) as client:
            for url in link_list:
                req = client.build_request("GET", await _prepare_url(url))
                response = await client.send(req, stream=True)
                line = ""
                if 200 <= response.status_code < 300:
                    line = f"{url},{response.status_code},"
                elif response.status_code == 302:
                    text = await response.aread()
                    text_str = str(text)
                    redirect_link = await _parse_redirect_link(text_str)
                    print(f"response status is {response.status_code}, redirect link is {redirect_link}")
                else:
                    text = await response.aread()
                    line = f"{url},{response.status_code},{text}"
                await response.aclose()
                await writer(line)
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
            if len(line) > 0:
                link_list.append(line.strip())
    asyncio.run(_main(output_file, concurrent_requests, link_list))


if __name__ == '__main__':
    main()
