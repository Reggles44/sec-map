import asyncio
import datetime
import logging

from flask import Flask
from flask import request

from build import INDEX_MAPPING
from sec_map.scrape import make_assembler
from sec_map.serializers import LookupSchema, AssembleSchema

app = Flask(__name__)
logger = logging.getLogger()


@app.route('/', methods=['GET'])
def index():
    return INDEX_MAPPING


@app.route('/lookup', methods=['GET'])
def lookup():
    validated_data = LookupSchema().load(request.args)

    data = None
    for cik, company in INDEX_MAPPING.items():
        if validated_data.get('cik') == cik or \
                validated_data.get('ticker') == company['ticker'] or \
                validated_data.get('company_name') == company['company_name']:
            data = company
            break

    form_type = validated_data.get('form_type')
    start_date = validated_data.get('start_date')
    end_date = validated_data.get('end_date')

    if form_type:
        data = data['forms'][form_type]

        if start_date:
            data = {date: v for date, v in data.items() if datetime.datetime.strptime(date, '%Y-%m-%d') > start_date}

        if end_date:
            data = {date: v for date, v in data.items() if datetime.datetime.strptime(date, '%Y-%m-%d') < end_date}

    return data


@app.route('/assemble', methods=['GET'])
async def assemble():
    validated_data = AssembleSchema().load(request.args)

    data = None
    cik = None
    for c, company in INDEX_MAPPING.items():
        if validated_data.get('cik') == c or \
                validated_data.get('ticker') == company['ticker'] or \
                validated_data.get('company_name') == company['company_name']:
            data = company
            cik = c
            break

    start_date = validated_data.get('start_date')
    end_date = validated_data.get('end_date')

    data = data['forms'][validated_data.get('form_type')]

    if not (start_date and end_date):
        ids = data.values()
    else:
        if start_date:
            ids = (v for date, v in data.items() if datetime.datetime.strptime(date, '%Y-%m-%d') > start_date)
        if end_date:
            ids = (v for date, v in data.items() if datetime.datetime.strptime(date, '%Y-%m-%d') < end_date)

    assemblers = await asyncio.gather(
        *(make_assembler(cik, index_id) for index_id in ids)
    )

    if len(assemblers) == 1:
        return assemblers[0].to_json()
    else:
        assembler = assemblers[0]
        assembler.merge(assemblers[1:])
        return assembler.to_json()
