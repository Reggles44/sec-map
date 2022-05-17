from marshmallow import Schema, fields, validates_schema, ValidationError


class LookupSchema(Schema):
    cik = fields.Str(required=False)
    company_name = fields.Str(required=False)
    ticker = fields.Str(required=False)
    form_type = fields.Str(required=False)
    start_date = fields.DateTime(required=False)
    end_date = fields.DateTime(required=False)

    class Meta:
        datetimeformat = '%Y-%m-%d'

    @validates_schema
    def validate_schema(self, data, **kwargs):
        if not (data.get('cik') or data.get('company_name') or data.get('ticker')):
            raise ValidationError('lookup must provide a company_name or ticket')

        if not data.get('form_type') and (data.get('start_date') or data.get('end_date')):
            raise ValidationError('form_type required for start_date and end_date to be used')


class AssembleSchema(Schema):
    cik = fields.Str(required=False)
    company_name = fields.Str(required=False)
    ticker = fields.Str(required=False)
    form_type = fields.Str(required=True)
    start_date = fields.DateTime(required=False)
    end_date = fields.DateTime(required=False)

    class Meta:
        datetimeformat = '%Y-%m-%d'

    @validates_schema
    def validate_schema(self, data, **kwargs):
        if not (data.get('cik') or data.get('company_name') or data.get('ticker')):
            raise ValidationError('lookup must provide a company_name or ticket')

        if not data.get('form_type') and (data.get('start_date') or data.get('end_date')):
            raise ValidationError('form_type required for start_date and end_date to be used')


lookup_schema = LookupSchema()
