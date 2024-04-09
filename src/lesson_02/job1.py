import os
import logging

from http import HTTPStatus
from flask import Flask, request, jsonify

from lesson_02.utils import fetch_data, create_path, save_json_file
from src.lesson_02.settings import API_ENDPOINT

app = Flask(__name__)


@app.route('/', methods=['POST'])
def job():

    target_path = request.get_json()['raw_dir']
    target_date = request.get_json()['date']

    create_path(target_path)

    page: int = 1
    while (response := fetch_data(API_ENDPOINT, target_date, page)).status_code != HTTPStatus.NOT_FOUND:
        content = response.json()
        file_path = os.path.join(target_path, f"sales_{target_date}_{page}.json")
        save_json_file(file_path, content)
        page += 1

    return jsonify({'message': 'Request processed successfully'}), HTTPStatus.CREATED


if __name__ == '__main__':
    app.run(debug=True)
