from typing import Dict

from fastavro import parse_schema

API_ENDPOINT: str = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'

SCHEMA: Dict = {
    'doc': 'Sales Data',
    'name': 'Sales',
    'namespace': 'sales',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'int'},
    ],
}

PARSED_SCHEMA = parse_schema(SCHEMA)
