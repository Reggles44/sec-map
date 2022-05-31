import json
import logging
import re

import pytest
from marshmallow import Schema, fields, validate

from sec_map import INDEX_MAPPING


class ResponseSchema(Schema):
    ticker = fields.String(required=True, allow_none=True)
    company_name = fields.String(required=True)
    forms = fields.Dict(
        keys=fields.String(),
        values=fields.Dict(
            keys=fields.String(required=True, validate=validate.Regexp(r'\d{4}-\d{2}-\d{2}')),
            values=fields.String(required=True, validate=validate.Regexp(r'\d{10}-\d{2}-\d{6}')),
            allow_none=True,
        )
    )


def test_index_file():
    assert INDEX_MAPPING


def test_index(client):
    response = client.get('/')
    assert response.status_code == 200

    index= response.get_json()

    assert isinstance(index, dict)
    assert len(index) > 0

    response_schema = ResponseSchema()
    for cik, company_data in index.items():
        assert re.search(r'^\d*$', cik)
        response_schema.load(company_data)


ID_LOOK_QUERY = (
    {'cik': '1403161'},
    {'ticker': 'V'},
    {'company_name': 'VISA INC.'},
)

FORM_QUERIES = (
    {},
    {'form_type': '10-Q'},
    {'form_type': '10-Q', 'start_date': '2018-04-17'},
    {'form_type': '10-Q', 'end_date': '2021-09-09'},
    {'form_type': '10-Q', 'start_date': '2018-04-17', 'end_date': '2021-09-09'},
)


@pytest.mark.parametrize('id_query', ID_LOOK_QUERY)
@pytest.mark.parametrize('form_query', FORM_QUERIES)
def test_lookup(client, id_query, form_query):
    response = client.get('/lookup/company', json=id_query | form_query)
    print(response.data)
    assert response.status_code == 200

    response_schema = ResponseSchema()
    response_schema.load(response.get_json())