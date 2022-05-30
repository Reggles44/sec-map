import asyncio
import datetime
import logging

import httpx
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from flask import Flask
from flask import request
from marshmallow import ValidationError
from xbrlassembler import XBRLType, XBRLAssembler

from build import INDEX_MAPPING
from sec_map.serializers import LookupSchema, AssembleSchema

app = Flask(__name__)
logger = logging.getLogger()

lookup_schema = LookupSchema()
assemble_schema = AssembleSchema()

HTTPX_CLIENT = httpx.AsyncClient(timeout=5)
SEC_RATE_LIMITER = AsyncLimiter(5, 1)


async def get(url):
    async with SEC_RATE_LIMITER:
        try:
            response = await HTTPX_CLIENT.get(url, headers={'User-Agent': 'Company Name myname@company.com'})
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            pass


def _lookup(kwargs):
    for cik, company in INDEX_MAPPING.items():
        if kwargs.get('cik') == cik or \
                kwargs.get('ticker') == company['ticker'] or \
                kwargs.get('company_name') == company['company_name']:

            data = company

            form_type = kwargs.get('form_type')
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')

            if form_type:
                data = data['forms'][form_type]

                if start_date:
                    data = {date: v for date, v in data.items() if
                            datetime.datetime.strptime(date, '%Y-%m-%d') > start_date}

                if end_date:
                    data = {date: v for date, v in data.items() if
                            datetime.datetime.strptime(date, '%Y-%m-%d') < end_date}

            return cik, data
    return None, None


@app.route('/', methods=['GET'])
def index():
    return INDEX_MAPPING


@app.route('/lookup', methods=['GET'])
def lookup():
    try:
        validated_data = lookup_schema.load(request.args, many=False)
    except ValidationError as e:
        return e.__str__(), 400
    cik, data = _lookup(validated_data)
    if cik is None and data is None:
        return 'Could not find company from cik or ticker', 404
    return data


SEC_INDEX_URL = 'https://www.sec.gov/Archives/edgar/data/{}/{}-index.htm'


async def make_assembler(cik, index_id) -> XBRLAssembler:
    index_response = await get(SEC_INDEX_URL.format(cik, index_id))
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
        xbrl_type = XBRLType(file_name)
        if xbrl_type:
            file_map[xbrl_type] = BeautifulSoup(await get(link), 'lxml')

    return XBRLAssembler.parse(file_map, ref_doc=XBRLType.PRE)


@app.route('/assemble', methods=['GET'])
async def assemble():
    try:
        validated_data = assemble_schema.load(request.args, many=False)
    except ValidationError as e:
        return e.__str__(), 400

    cik, data = _lookup(validated_data)
    if cik is None and data is None:
        return 'Could not find company from cik or ticker', 404

    assemblers = await asyncio.gather(
        *(make_assembler(cik, index_id) for index_id in data.values())
    )

    if len(assemblers) == 1:
        return assemblers[0].to_json()
    else:
        assembler = assemblers[0]
        assembler.merge(*assemblers[1:])
        return assembler.to_json()
