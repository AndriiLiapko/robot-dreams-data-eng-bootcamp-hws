import os
import shutil
import tempfile
import unittest
import json
from src.lesson_02.job2 import app


class TestJob2(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.temp_dir = tempfile.mkdtemp(dir=os.getcwd())

        os.environ['BASE_DIR'] = os.getcwd()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_job_e2e_happy_path(self):
        file1_path = os.path.join(self.temp_dir, 'sales_2024-04-07_1.json')
        file2_path = os.path.join(self.temp_dir, 'sales_2024-04-07_2.json')

        expected_content_file_1 = [
            {'client': "John Doe", 'purchase_date': "2024-04-07", 'product': "Sun Glasses", 'price': 100},
            {'client': "Kate Conor", 'purchase_date': "2024-04-07", 'product': "Photo Camera", 'price': 500}
        ]
        expected_content_file_2 = [
            {'client': "Mark Spencer", 'purchase_date': "2024-04-07", 'product': "Game Pad", 'price': 50},
            {'client': "Jack Scott", 'purchase_date': "2024-04-07", 'product': "Keyboard", 'price': 120}
        ]

        with open(file1_path, 'w') as file:
            json.dump(expected_content_file_1, file)

        with open(file2_path, 'w') as file:
            json.dump(expected_content_file_2, file)

        response = self.app.post(
            '/',
            json={
                'raw_dir': self.temp_dir,
                'stg_dir': os.path.join(self.temp_dir, 'tmp_stg')}
        )

        self.assertEqual(response.status_code, 201)

        # Assert files were created
        file1_path = os.path.join(self.temp_dir, 'tmp_stg', 'sales_2024-04-07_1.avro')
        file2_path = os.path.join(self.temp_dir, 'tmp_stg', 'sales_2024-04-07_2.avro')
        non_existent_file_path = os.path.join(self.temp_dir, 'tmp_stg', 'sales_2024-04-07_3.avro')

        self.assertTrue(os.path.exists(file1_path))
        self.assertTrue(os.path.exists(file2_path))
        self.assertFalse(os.path.exists(non_existent_file_path))

    def test_job_e2e_no_files_after_first_job_run_path(self):
        response = self.app.post(
            '/',
            json={
                'raw_dir': self.temp_dir,
                'stg_dir': os.path.join(self.temp_dir, 'tmp_stg')}
        )

        self.assertEqual(response.status_code, 201)

        # Assert directory exists
        stg_dir = os.path.join(self.temp_dir, 'tmp_stg')
        self.assertTrue(os.path.exists(stg_dir))
        self.assertTrue(os.path.isdir(stg_dir))

        # Assert directory is empty
        files_in_stg_dir = os.listdir(stg_dir)
        self.assertEqual(len(files_in_stg_dir), 0)



if __name__ == '__main__':
    unittest.main()
