import json

from flask import Blueprint, request
from marshmallow import ValidationError
from werkzeug.exceptions import BadRequest

from sec_map.serializers import LookupSchema
from sec_map.utils import data_lookup

bp = Blueprint('lookup', __name__, url_prefix='/lookup')

lookup_schema = LookupSchema()


# localhost:5000/lookup/company?ticker=V
@bp.route('/company', methods=['GET'])
def company_lookup():
    try:
        data = request.get_json()
    except BadRequest:
        data = request.get_data(parse_form_data=True)

    try:
        validated_data = lookup_schema.load(data, many=False)
    except ValidationError as e:
        return BadRequest(e), 400
    cik, data = data_lookup(validated_data)
    if cik is None and data is None:
        return f'Could not find company from cik or ticker from {validated_data}', 404
    return data