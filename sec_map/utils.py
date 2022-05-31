import datetime
import typing

import httpx
from aiolimiter import AsyncLimiter

from sec_map import INDEX_MAPPING

HTTPX_CLIENT = httpx.AsyncClient(timeout=5)
SEC_RATE_LIMITER = AsyncLimiter(5, 1)


async def get(url):
    async with SEC_RATE_LIMITER:
        try:
            response = await HTTPX_CLIENT.get(url, headers={'User-Agent': 'Company Name myname@company.com'})
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            return


def data_lookup(kwargs) -> typing.Union[typing.Tuple[str, dict], typing.Tuple[None, None]]:
    for cik, company in INDEX_MAPPING.items():
        if kwargs.get('cik') == cik or \
                kwargs.get('ticker') == company['ticker'] or \
                kwargs.get('company_name') == company['company_name']:

            data = company

            form_type = kwargs.get('form_type')
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')

            if form_type:
                doc_ids = data['forms'].get(form_type, {})

                if start_date:
                    doc_ids = {date: v for date, v in doc_ids.items() if
                               datetime.datetime.strptime(date, '%Y-%m-%d') > start_date}

                if end_date:
                    doc_ids = {date: v for date, v in doc_ids.items() if
                               datetime.datetime.strptime(date, '%Y-%m-%d') < end_date}

                data['forms'] = {form_type: doc_ids}

            return cik, data
    return None, None