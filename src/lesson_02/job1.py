import os
import logging

from http import HTTPStatus

from flask import Flask, request, jsonify

from lesson_02.utils import fetch_data, create_path, save_json_file, get_and_format_path
from lesson_02.settings import API_ENDPOINT

app = Flask(__name__)

app.logger.setLevel(logging.INFO)
handler = logging.FileHandler('app.log')
app.logger.addHandler(handler)


@app.before_request
def log_request_info():
    app.logger.info('Headers: %s', request.headers)
    app.logger.info('Body: %s', request.data.decode('utf-8'))


@app.route('/', methods=['POST'])
def job():
    app.logger.info('Received POST request')
    target_path, target_date = get_and_format_path(request.data)

    app.logger.info(f'raw_dir: {target_path}')
    app.logger.info(f'date: {target_date}')
    create_path(target_path)

    page: int = 1
    while (response := fetch_data(API_ENDPOINT, target_date, page)).status_code != HTTPStatus.NOT_FOUND:
        content = response.json()
        file_path = os.path.join(target_path, f"sales_{target_date}_{page}.json")
        save_json_file(file_path, content)
        page += 1

    app.logger.info('Finished execution')
    return jsonify({'message': 'Request processed successfully'}), HTTPStatus.CREATED


if __name__ == '__main__':
    app.run(debug=True)
