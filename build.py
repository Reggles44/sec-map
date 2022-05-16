import asyncio
import json
import logging
import logging.config
import re
import os
import datetime
import math
import sys

import httpx
from aiolimiter import AsyncLimiter

from bs4 import BeautifulSoup

logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger()
logger.info('TEST')

HTTPX_CLIENT = httpx.AsyncClient(timeout=5)
SEC_RATE_LIMITER = AsyncLimiter(9, 1)

CRAWLER_IDX_URL = 'https://www.sec.gov/Archives/edgar/full-index/{}/QTR{}/crawler.idx'
CRAWLER_LINE_REGEX = re.compile('(.+)\s+([\dA-Z\-\/]+)\s+(\d+)\s+(\d{4}-\d{2}-\d{2}).*\/([\d\-]+)-index.htm\s*$')

INDEX_URL = 'https://www.sec.gov/Archives/edgar/data/{}/{}-index.htm'
SCHEMA_TICKET_REGEX = re.compile('(\w+)-\d+.xsd')

START_DATE = datetime.date(year=2020, month=1, day=1)
END_DATE = datetime.date.today()

INDEX_MAPPING = {}
INDEX_JSON = 'index.json'

"""
Index schema

{
    # CIK is primary index
    "0000001": {
        "ticker": "ABC",
        "company_name": "ABC Inc.",
        "forms": {
            "10-K": {
                "2022-02-09": "0001127602-22-004061"
            }
        }
    }
}
"""


async def get(url):
    async with SEC_RATE_LIMITER:
        try:
            response = await HTTPX_CLIENT.get(url, headers={'User-Agent': 'Company Name myname@company.com'})
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            pass


async def scrape_quarter(date):
    year, qtr = date.year, math.ceil(date.month / 3)
    logger.info(f'Scraping {year} QTR{qtr}')
    crawler_data = await get(CRAWLER_IDX_URL.format(year, qtr))
    if not crawler_data:
        return

    starting_index = next(i for i, line in enumerate(crawler_data.text.split('\n')) if all(c == '-' for c in line))

    for line in crawler_data.text.split('\n')[starting_index+1:]:
        line = line.strip()
        if not line:
            continue

        company_name, form_type, cik, date_filed, index_id = CRAWLER_LINE_REGEX.search(line).groups()

        company_data = INDEX_MAPPING.setdefault(
            cik,
            {
                'company_name': company_name,
                'ticker': None,
                'forms': {}
            }
        )

        company_data['forms'].setdefault(form_type, {})[date_filed] = index_id


async def scrape_index(url):
    index_response = await get(url)
    if not index_response:
        return

    index_soup = BeautifulSoup(index_response.content, 'lxml')
    data_files_table = index_soup.find('table', {'summary': 'Data Files'})
    if not data_files_table:
        return

    schema_file = data_files_table.find_all(text=SCHEMA_TICKET_REGEX)[0]
    ticker = SCHEMA_TICKET_REGEX.search(schema_file.text).group(1)
    return ticker.upper()


async def find_ticker(cik, data):
    ticker = data['ticker']
    if ticker:
        return

    index_ids = []
    for date_index_mapping in data['forms'].values():
        index_ids.extend(date_index_mapping.values())

    while not ticker and index_ids:
        url = INDEX_URL.format(cik, index_ids.pop())
        tck = await scrape_index(url)
        if tck:
            ticker = tck

    data['ticker'] = ticker


async def build():
    quarters = int(((END_DATE.year - START_DATE.year) * 4 + (END_DATE.month - START_DATE.month) + 1) / 3)

    await asyncio.gather(
        *(scrape_quarter(START_DATE + datetime.timedelta(weeks=12 * i)) for i in range(quarters)),
    )

    await asyncio.gather(
        *(find_ticker(cik, data) for cik, data in INDEX_MAPPING.items())
    )

    json.dump(INDEX_MAPPING, open(INDEX_JSON, 'w+'), indent=4)


if __name__ == '__main__':
    if os.path.isfile(INDEX_JSON) and '--force' not in sys.argv:
        INDEX_MAPPING = json.load(open(INDEX_JSON))
    asyncio.get_event_loop().run_until_complete(build())
