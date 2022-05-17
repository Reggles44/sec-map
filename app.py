import datetime
import logging
import re

from flask import Flask
from flask import request

from build import INDEX_MAPPING
from sec_map.serializers import lookup_schema

app = Flask(__name__)
logger = logging.getLogger()


@app.route('/', methods=['GET'])
def index():
    return INDEX_MAPPING


@app.route('/lookup', methods=['GET'])
def lookup():
    validated_data = lookup_schema.load(request.args)

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
def assemble():
    validated_data = lookup_schema.load(request.args)

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
