from flask import Blueprint, request
from marshmallow import ValidationError
from werkzeug.exceptions import BadRequest

from sec_map.serializers import LookupSchema

import asyncio
import datetime
import logging

from bs4 import BeautifulSoup
from flask import Flask
from flask import request
from marshmallow import ValidationError
from xbrlassembler import XBRLType, XBRLAssembler

from sec_map.serializers import LookupSchema, AssembleLookupSchema
from sec_map.utils import get, data_lookup

bp = Blueprint('assemble', __name__, url_prefix='/assemble')

lookup_schema = LookupSchema()
assemble_lookup_schema = AssembleLookupSchema()


SEC_INDEX_URL = 'https://www.sec.gov/Archives/edgar/data/{}/{}-index.htm'


async def make_assembler(cik, index_id) -> (XBRLAssembler, None):
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


@bp.route('/', methods=['GET'])
async def assemble():
    try:
        data = request.get_json()
    except BadRequest:
        data = request.get_data(parse_form_data=True)

    try:
        validated_data = assemble_lookup_schema.load(data, many=False)
    except ValidationError as e:
        return e.__str__(), 400

    cik, data = data_lookup(validated_data)
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
