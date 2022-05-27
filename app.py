import asyncio
import datetime
import logging

import werkzeug
from flask import Flask
from flask import request
from marshmallow import ValidationError

from build import INDEX_MAPPING
from sec_map.scrape import make_assembler
from sec_map.serializers import LookupSchema, AssembleSchema

app = Flask(__name__)
logger = logging.getLogger()


lookup_schema = LookupSchema()
assemble_schema = AssembleSchema()


@app.route('/', methods=['GET'])
def index():
    return INDEX_MAPPING


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
