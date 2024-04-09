import os
import logging
from http import HTTPStatus
from flask import Flask, request, jsonify

from lesson_02.utils import create_path, map_files_to_location, get_json_file, write_avro_file

app = Flask(__name__)


@app.route('/', methods=['POST'])
def job():
    app.logger.info(request.get_json())

    raw_dir = request.get_json()['raw_dir']
    stg_dir = request.get_json()['stg_dir']

    if not os.path.exists(raw_dir):
        return jsonify({'message': f'JSON data is not available in {raw_dir}'}), HTTPStatus.NOT_FOUND

    create_path(stg_dir)

    file_locations = map_files_to_location(raw_dir)

    for file_location, file_name in file_locations.items():
        data = get_json_file(file_location)
        out_file_path = os.path.join(stg_dir, file_name.replace('.json', '.avro'))
        write_avro_file(out_file_path, data)

    return jsonify({'message': 'Request processed successfully'}), HTTPStatus.CREATED


if __name__ == '__main__':
    app.run(debug=True)
