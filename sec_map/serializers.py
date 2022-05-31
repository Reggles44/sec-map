from marshmallow import Schema, fields, validates_schema, ValidationError


class CompanyLookupSchema(Schema):
    cik = fields.Str(required=False)
    ticker = fields.Str(required=False)
    company_name = fields.Str(required=False)

    class Meta:
        datetimeformat = '%Y-%m-%d'

    @validates_schema()
    def validate_schema(self, data, **kwargs):
        if not (data.get('cik') or data.get('ticker') or data.get('company_name')):
            raise ValidationError({
                'cik': ['Required if ticker or company_name is not present'],
                'ticker': ['Required if cik or company_name is not present'],
                'company_name': ['Required if cik or ticker is not present'],
            })


class LookupSchema(CompanyLookupSchema):
    form_type = fields.Str(required=False)
    start_date = fields.DateTime(required=False)
    end_date = fields.DateTime(required=False)

    @validates_schema()
    def validate_schema(self, data, **kwargs):
        field_errors = {}

        if not (data.get('cik') or data.get('ticker') or data.get('company_name')):
            field_errors.update({
                'cik': ['Required if ticker is not present'],
                'ticker': ['Required if cik is not present'],
                'company_name': ['Required if cik or ticker is not present'],
            })

        if not data.get('form_type') and (data.get('start_date') or data.get('end_date')):
            field_errors.update({
                'form_type': ['Required if either start_date or end_date is present'],
            })

        if field_errors:
            raise ValidationError(field_errors)


class AssembleLookupSchema(CompanyLookupSchema):
    form_type = fields.Str(required=True)
    start_date = fields.DateTime(required=False)
    end_date = fields.DateTime(required=False)
