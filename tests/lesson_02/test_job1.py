import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import json
from lesson_02.job1 import app


class TestJob1(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.temp_dir = tempfile.mkdtemp(dir=os.getcwd())

        os.environ['AUTH_TOKEN'] = '123'
        os.environ['BASE_DIR'] = os.getcwd()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    @patch('src.lesson_02.job1.requests.get')
    def test_job_e2e_happy_path(self, mock_get):
        expected_content_file_1 = [{'data': 'some_data'}]
        expected_content_file_2 = [{'data': 'some_data'}, {'data': 'some_more_data'}]

        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: expected_content_file_1),
            MagicMock(status_code=200, json=lambda: expected_content_file_2),
            MagicMock(status_code=404)
        ]
        client = app.test_client()
        response = client.post('/', json={'raw_dir': '/tmp', 'date': '2024-04-07'})
        self.assertEqual(response.status_code, 201)

        # Assert files were created
        file1_path = '/tmp/sales_2024-04-07_1.json'
        file2_path = '/tmp/sales_2024-04-07_2.json'
        non_existent_file_path = '/tmp/sales_2024-04-07_3.json'

        self.assertTrue(os.path.exists(file1_path))
        self.assertTrue(os.path.exists(file2_path))
        self.assertFalse(os.path.exists(non_existent_file_path))

        # Asserting content of the files
        with open(file1_path, 'r') as file1:
            file1_content = json.load(file1)
            self.assertEqual(file1_content, expected_content_file_1)

        with open(file2_path, 'r') as file2:
            file2_content = json.load(file2)
            self.assertEqual(file2_content, expected_content_file_2)

    @patch('src.lesson_02.job1.requests.get')
    def test_job_e2e_no_data_and_no_errors_path(self, mock_get):

        mock_get.side_effect = [
            MagicMock(status_code=404)
        ]
        client = app.test_client()
        response = client.post('/', json={'raw_dir': '/tmp', 'date': '2024-04-07'})
        self.assertEqual(response.status_code, 201)

        # Assert files were created
        file1_path = '/tmp/sales_2024-04-07_1.json'
        file2_path = '/tmp/sales_2024-04-07_2.json'

        self.assertFalse(os.path.exists(file1_path))
        self.assertFalse(os.path.exists(file2_path))


if __name__ == '__main__':
    unittest.main()
