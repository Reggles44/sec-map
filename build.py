import asyncio
import json
import logging
import re
import os
import datetime
import math
import sys

from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

from sec_map.utils import get

logger = logging.getLogger()

CRAWLER_IDX_URL = 'https://www.sec.gov/Archives/edgar/full-index/{}/QTR{}/crawler.idx'
CRAWLER_LINE_REGEX = re.compile('(.+)\s+([\dA-Z\-\/]+)\s+(\d+)\s+(\d{4}-\d{2}-\d{2}).*\/([\d\-]+)-index.htm\s*$')

INDEX_URL = 'https://www.sec.gov/Archives/edgar/data/{}/{}-index.htm'
SCHEMA_TICKET_REGEX = re.compile('(\w+)-\d+.xsd')

START_DATE = datetime.date(year=2015, month=1, day=1)
END_DATE = datetime.date.today()

META_JSON = 'meta.json'
META_MAPPING = json.load(open(META_JSON)) if os.path.isfile(META_JSON) else {}

TICKER_JSON = 'ticker.json'
TICKER_MAPPING = json.load(open(TICKER_JSON)) if os.path.isfile(TICKER_JSON) else {}

INDEX_JSON = 'index.json'
INDEX_MAPPING = json.load(open(INDEX_JSON)) if os.path.isfile(INDEX_JSON) else {}

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


async def scrape_quarter(date):
    year, qtr = date.year, math.ceil(date.month / 3)
    key = f'{year}-{qtr}'
    if META_MAPPING.get(key):
        return

    logger.info(f'Scraping {year} QTR{qtr}')
    crawler_data = await get(CRAWLER_IDX_URL.format(year, qtr))
    if not crawler_data:
        return

    starting_index = next(i for i, line in enumerate(crawler_data.text.split('\n')) if all(c == '-' for c in line))

    for line in crawler_data.text.split('\n')[starting_index+1:]:
        line = line.strip()
        if not line:
            continue

        try:
            company_name, form_type, cik, date_filed, index_id = CRAWLER_LINE_REGEX.search(line).groups()
        except AttributeError:
            logger.debug(f'Invalid line ({line})')
            continue

        company_data = INDEX_MAPPING.setdefault(
            cik,
            {
                'company_name': company_name.strip(),
                'ticker': None,
                'forms': {}
            }
        )

        company_data['forms'].setdefault(form_type, {})[date_filed] = index_id

    META_MAPPING[key] = True


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
    ticker = TICKER_MAPPING.get(cik)
    if not ticker:
        index_ids = list(data['forms'].get('10-Q', {}).values())

        while index_ids:
            url = INDEX_URL.format(cik, index_ids.pop())
            ticker = await scrape_index(url)
            if ticker:
                logger.debug(f'Found Ticker {ticker} for {data["company_name"]}')
                break

        TICKER_MAPPING[cik] = ticker

    data['ticker'] = ticker


async def build():
    quarters = int((END_DATE - START_DATE).days / (365/4)) + 1

    await asyncio.gather(
        *(scrape_quarter(START_DATE + relativedelta(months=3 * i)) for i in range(quarters)),
    )

    json.dump(INDEX_MAPPING, open(INDEX_JSON, 'w+'), indent=4)
    json.dump(META_MAPPING, open(META_JSON, 'w+'), indent=4)

    await asyncio.gather(
        *(find_ticker(cik, data) for cik, data in INDEX_MAPPING.items())
    )

    json.dump(TICKER_MAPPING, open(TICKER_JSON, 'w+'), indent=4)
    json.dump(INDEX_MAPPING, open(INDEX_JSON, 'w+'), indent=4)
    json.dump(META_MAPPING, open(META_JSON, 'w+'), indent=4)


if __name__ == '__main__':
    if '--clear' in sys.argv:
        META_MAPPING = {}
        INDEX_MAPPING = {}

    asyncio.get_event_loop().run_until_complete(build())
