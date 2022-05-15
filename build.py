import asyncio
import logging
import re
import os

from bs4 import BeautifulSoup

from financial_modeling import utils

import httpx
from xbrlassembler import XBRLType

from financial_modeling import sec_rate_limit
from financial_modeling.settings import BASE_DATA_DIRECTORY

logger = logging.getLogger()
httpx_client = httpx.AsyncClient(timeout=5)

CRAWLER_IDX_URL = 'https://www.sec.gov/Archives/edgar/full-index/{}/QTR{}/crawler.idx'


async def GET(url):
    async with sec_rate_limit:
        try:
            response = await httpx_client.get(url, headers={'User-Agent': 'Company Name myname@company.com'})
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            logger.error(f'{url} timeout')


async def scrape_quarter(year, qtr):
    logger.debug(f'Scraping {year} QTR{qtr}')
    crawler_data = await GET(CRAWLER_IDX_URL.format(year, qtr))
    if not crawler_data:
        # logger.error(CRAWLER_IDX_URL.format(year, qtr))
        return

    starting_index = next(i for i, line in enumerate(crawler_data.text.split('\n')) if all(c == '-' for c in line))

    indexs = []

    for line in crawler_data.text.split('\n')[starting_index+1:]:
        if not any(file_type in line for file_type in ('10-K', '10-Q')):
            continue

        info = re.split(r'\s{2,}', line)
        path = (
            utils.char_only(info[0]),
            info[3],
            info[1],
        )

        dir_path = os.path.join(BASE_DATA_DIRECTORY, *path)
        if os.path.isdir(dir_path) and len(os.listdir(dir_path)) != 0:
            continue
        os.makedirs(dir_path, exist_ok=True)

        # logger.debug(info)
        indexs.append((info[4], path))

    await asyncio.gather(
        *[scrape_index(url, path) for (url, path) in indexs],
    )


async def scrape_index(url, path):
    index_response = await GET(url)
    if not index_response:
        return

    index_soup = BeautifulSoup(index_response.content, 'lxml')
    data_files_table = index_soup.find('table', {'summary': 'Data Files'})
    if not data_files_table:
        return

    file_map = {}

    for row in data_files_table('tr')[1:]:
        link = "https://www.sec.gov" + row.find_all('td')[2].find('a')['href']
        file_name = link.rsplit('/', 1)[1]
        file_path = os.path.join(BASE_DATA_DIRECTORY, *path, file_name)
        if os.path.isfile(file_path):
            continue

        xbrl_type = XBRLType.get(file_name)
        if xbrl_type:
            file_map[xbrl_type] = link, file_path

    async def get_save(url, path):
        file = await GET(url)
        if not file:
            return

        if os.path.isfile(path):
            return
        open(path, 'wb+').write(file.content)

    await asyncio.gather(
        *[get_save(url, file_path)
        for xbrl_type, (url, file_path) in file_map.items()]
    )


if __name__ == '__main__':
    