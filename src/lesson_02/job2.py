import shutil
import os
import json
import logging

from fastavro import writer, parse_schema
from flask import Flask, request, jsonify

from lesson_02.defs import SCHEMA

app = Flask(__name__)

app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)

PARSED_SCHEMA = parse_schema(SCHEMA)


@app.route('/', methods=['POST'])
def job():
    app.logger.info('Received request')
    app.logger.info('Start parsing data')
    app.logger.info(request.get_json())

    raw_dir = request.get_json()['raw_dir']
    stg_dir = request.get_json()['stg_dir']

    if not os.path.exists(raw_dir):
        return jsonify({'message': f'JSON data is not available in {raw_dir}'}), 404

    if os.path.exists(stg_dir):
        shutil.rmtree(stg_dir)

    os.makedirs(stg_dir)

    file_locations = {
        os.path.join(raw_dir, f): f
        for f in os.listdir(raw_dir)
        if os.path.isfile(os.path.join(raw_dir, f))
    }

    for file_location, file_name in file_locations.items():
        with open(file_location, "r") as f:
            data = json.load(f)

        with open(os.path.join(stg_dir, file_name.replace('.json', '.avro')), 'wb') as out:
            writer(out, PARSED_SCHEMA, data)

    app.logger.info("Request has been processed")
    return jsonify({'message': 'Request processed successfully'}), 201


if __name__ == '__main__':
    app.run(debug=True)
