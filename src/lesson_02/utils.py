import json
import os
import shutil
from typing import Dict, Tuple

import requests
from fastavro import writer
from flask import Response

from lesson_02.settings import PARSED_SCHEMA


def fetch_data(endpoint: str, date: str, page: int) -> Response:
    """
    Fetch data from a specified endpoint with the provided date and page number.

    Parameters:
        endpoint (str): The URL endpoint from which to fetch data.
        date (str): The date for which data is to be fetched.
        page (int): The page number of the data to fetch.

    Returns:
        Response: The response object containing the fetched data.

    Dependencies:
        - os: Provides access to environment variables to retrieve authentication token.
        - requests: HTTP library for sending requests.

    Example:
        response = fetch_data('https://api.example.com/data', '2024-04-09', 1)

    Function Details:
        - Fetches data from the specified endpoint using requests.get() method.
        - The 'date' and 'page' parameters are passed as query parameters to the endpoint.
        - The 'Authorization' header is set using the 'AUTH_TOKEN' environment variable stored in os.environ.
        - Returns the Response object containing the fetched data.

    Error Handling:
        - If the 'AUTH_TOKEN' environment variable is not set or inaccessible, a KeyError will be raised.
        - If there's an error during the HTTP request (e.g., network issues, invalid endpoint),
          requests library may raise corresponding exceptions.

    """
    return requests.get(
        url=endpoint,
        params={'date': date, 'page': page},
        headers={'Authorization': os.environ['AUTH_TOKEN']},
    )


def create_path(target_path: str) -> None:
    """
    Ensure the existence of a specified directory path.

    Parameters:
        target_path (str): The path of the directory to be created.

    Returns:
        None

    Dependencies:
        - os: Provides functions for interacting with the operating system, including file and directory manipulation.
        - shutil: Provides high-level file operations such as copying and removal.

    Example:
        create_path('/path/to/directory')

    Function Details:
        1. Check Existing Directory:
           - Checks if the specified directory path exists using os.path.exists(target_path).
        2. Remove Existing Directory (if applicable):
           - If the directory already exists, removes the entire directory
             and its contents using shutil.rmtree(target_path).
        3. Create Directory:
           - Creates a new directory at the specified path using os.makedirs(target_path),
             creating any necessary parent directories if they do not exist.
        4. No Return Value:
           - This function does not return any value.
             It performs the operations to ensure the specified directory path exists.

    """
    if os.path.exists(target_path):
        shutil.rmtree(target_path)

    os.makedirs(target_path)


def save_json_file(file_path: str, content: dict) -> None:
    """
    Save a dictionary as JSON to a file.

    Parameters:
        file_path (str): The path to the file where JSON data will be saved.
        content (dict): The dictionary to be saved as JSON.

    Returns:
        None

    Dependencies:
        - json: Provides functions for encoding and decoding JSON data.

    Example:
        save_json_file('data.json', {'name': 'John', 'age': 30})

    Function Details:
        - Opens the specified file in 'write' mode ('w') using a context manager.
        - Serializes the content dictionary into JSON format and writes it to the file.
        - Closes the file after writing.

    Error Handling:
        - If there's an error during file I/O (e.g., permission issues, invalid file path),
          Python's built-in File I/O functions may raise corresponding exceptions.

    Caution:
        - Ensure that the file_path provided is a valid path where the JSON data should be saved.
        - Handle file I/O exceptions gracefully to manage errors during the saving process.
    """
    with open(file_path, 'w') as f:
        json.dump(content, f)


def map_files_to_location(path: str) -> Dict[str, str]:
    """
    Map filenames to their corresponding absolute file paths within a directory.

    Parameters:
        path (str): The path to the directory containing the files.

    Returns:
        Dict[str, str]: A dictionary mapping filenames to their absolute file paths.

    Dependencies:
        - os: Provides functions for interacting with the operating system, including file and directory manipulation.

    Example:
        mapping = map_files_to_location('/path/to/directory')

    Function Details:
        - Iterates through each file in the specified directory.
        - Constructs the absolute file path for each file using os.path.join().
        - Checks if the path corresponds to a file using os.path.isfile().
        - Constructs a dictionary where keys are absolute file paths and values are corresponding filenames.

    Error Handling:
        - If the specified directory path does not exist or is inaccessible, os.listdir() may raise FileNotFoundError or OSError.
        - If an error occurs during the construction of absolute file paths or checking
          if a path corresponds to a file, corresponding exceptions may be raised.

    Caution:
        - Ensure that the specified directory path is valid and accessible.
        - Handle exceptions gracefully to manage errors that may occur during directory listing or file mapping.
    """
    return {
        os.path.join(path, f): f
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f))
    }


def get_json_file(path: str) -> dict:
    """
    Retrieve data from a JSON file and return it as a dictionary.

    Parameters:
        path (str): The path to the JSON file.

    Returns:
        dict: The dictionary containing data retrieved from the JSON file.

    Dependencies:
        - json: Provides functions for encoding and decoding JSON data.

    Example:
        data = get_json_file('data.json')

    Function Details:
        - Opens the specified JSON file in 'read' mode ('r') using a context manager.
        - Loads the JSON data from the file into a Python dictionary using json.load().
        - Closes the file after loading the data.

    Error Handling:
        - If the specified file path is invalid or inaccessible,
          Python's built-in File I/O functions may raise corresponding exceptions.
        - If the JSON data in the file is malformed or cannot be decoded, json.load() may raise JSONDecodeError.

    Caution:
        - Ensure that the specified file path points to a valid JSON file.
        - Handle file I/O and JSON decoding errors gracefully to manage potential issues with reading the file.
    """
    with open(path, "r") as f:
        data = json.load(f)
    return data


def write_avro_file(save_to_file: str, data: dict) -> None:
    """
    Write data to an Avro file using a specified schema.

    Parameters:
        save_to_file (str): The path to the Avro file to write the data to.
        data (dict): The dictionary containing the data to be written to the Avro file.

    Returns:
        None

    Example:
        write_avro_file('data.avro', {'field1': 'value1', 'field2': 'value2'})

    Function Details:
        - Opens the specified Avro file in 'write binary' mode ('wb') using a context manager.
        - Writes the data to the Avro file using a specified schema and the Avro Python library's writer() function.
        - Closes the file after writing.

    Error Handling:
        - If the specified file path is invalid or inaccessible, Python's built-in File I/O functions may raise corresponding exceptions.
        - If there's an error during the Avro writing process, such as schema mismatch or incompatible data, it may raise exceptions specific to the Avro library.

    Caution:
        - Ensure that the specified file path points to a valid location where the Avro file should be saved.
        - Ensure that the data provided conforms to the expected schema to avoid writing errors.
    """
    with open(save_to_file, 'wb') as out:
        writer(out, PARSED_SCHEMA, data)


def get_and_format_path(request_body: bytes) -> Tuple[str, str]:
    """
    Get and format path based on request body data.

    Args:
        request_body (bytes): The byte string representing the request body.

    Returns:
        Tuple[str, str]: A tuple containing the raw directory and formatted date or staging directory.

    This function decodes and cleans up the `request_body`, then checks if the 'date' key exists
    in the decoded data. If present, it means that it is the body for the job 1 it then returns the 'raw_dir'
    and the formatted date extracted from the 'date' key. Otherwise, it returns the 'raw_dir' and the 'stg_dir', which
    is expected body for the job 2, from the decoded data.
    """
    data = decode_and_cleanup_request_body(request_body)
    if 'date' in data.keys():
        return data['raw_dir'], data['date'].split(" ")[0]
    return data['raw_dir'], data['stg_dir']


def decode_and_cleanup_request_body(request_body: bytes) -> Dict[str, str]:
    """
    Decode and clean up a request body.

    Args:
        request_body (bytes): The byte string representing the request body.

    Returns:
        Dict[str, str]: A dictionary containing decoded and cleaned up key-value pairs.

    This function takes a byte string `request_body`, decodes it as UTF-8, and then replaces
    any backslashes with double backslashes. It then loads the cleaned up JSON string into
    a dictionary and returns it.
    """
    return json.loads(request_body.decode('utf-8').replace('\\', '\\\\'))
