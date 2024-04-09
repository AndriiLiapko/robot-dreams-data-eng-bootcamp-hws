import shutil
import os
import json
import logging
from http import HTTPStatus

from fastavro import writer
from flask import Flask, request, jsonify

from src.lesson_02.settings import PARSED_SCHEMA

app = Flask(__name__)

app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)


@app.route('/', methods=['POST'])
def job():
    app.logger.info('Received request')
    app.logger.info('Start parsing data')
    app.logger.info(request.get_json())

    raw_dir = request.get_json()['raw_dir']
    stg_dir = request.get_json()['stg_dir']

    if not os.path.exists(raw_dir):
        return jsonify({'message': f'JSON data is not available in {raw_dir}'}), HTTPStatus.NOT_FOUND

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
    return jsonify({'message': 'Request processed successfully'}), HTTPStatus.CREATED


if __name__ == '__main__':
    app.run(debug=True)
