import shutil
import os
import requests
import json
import logging

from flask import Flask, request, jsonify, Response

app = Flask(__name__)

app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)

API_ENDPOINT = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'


def fetch_data(endpoint: str, date: str, page: int) -> Response:
    return requests.get(
        url=endpoint,
        params={'date': date, 'page': page},
        headers={'Authorization': os.environ['AUTH_TOKEN']},
    )

@app.route('/', methods=['POST'])
def job():
    app.logger.info('Received request')
    app.logger.info('Start parsing data')

    target_path = request.get_json()['raw_dir']
    target_date = request.get_json()['date']

    app.logger.info(f'Data parsed: path={target_path}, date={target_date}')

    if os.path.exists(target_path):
        shutil.rmtree(target_path)

    os.makedirs(target_path)

    page: int = 1
    while True:
        response = fetch_data(API_ENDPOINT, target_date, page)

        if response.status_code == 404:
            app.logger.info(f'Fetch date={target_date}, page={page} Response code: {response.status_code}')
            break

        app.logger.info(f'Fetch date={target_date}, page={page} Response code: {response.status_code}')

        content = response.json()
        file_path = os.path.join(target_path, f"sales_{target_date}_{page}.json")
        with open(file_path, 'w') as f:
            json.dump(content, f)

        app.logger.info(f"Saving file to {file_path}")

        page += 1

    app.logger.info("Request has been processed")
    return jsonify({'message': 'Request processed successfully'}), 201


if __name__ == '__main__':
    app.run(debug=True)
