# Flask Application

This is a Flask application designed to [provide a brief description of what your application does].

## Setup Environment

To set up the environment for this Flask application, follow these steps:

1. Clone the repository:
    ```
    git clone git@github.com:AndriiLiapko/robot-dreams-data-eng-bootcamp-hws.git
    ```

2. Navigate to the project directory:
    ```
    cd robot-dreams-data-eng-bootcamp-hws
    ```

3. Create a virtual environment:
    ```
    python -m venv venv
    ```

4. Activate the virtual environment:
    - On Windows:
        ```
        venv\Scripts\activate
        ```
    - On Unix or MacOS:
        ```
        source venv/bin/activate
        ```

5. Install the required dependencies:
    ```
    pip install -r requirements.txt
    ```

## Environment Variables

To run this Flask application from PyCharm it is required the following environment variables to be set:

#### For JOB 1
- `AUTH_TOKEN`: Your personal token to the https://fake-api-vycpfa6oca-uc.a.run.app/sales endpoint.
- `BASE_DIR`: Base directory of this repository.
- `FLASK_RUN_PORT`: Both jobs are running on the same instance so for each Flask app (job) different port has to be specified. For job 1 it is recommended to use 8081


#### For JOB 2
- `BASE_DIR`: Base directory of this repository.
- `FLASK_RUN_PORT`: Both jobs are running on the same instance so for each Flask app (job) different port has to be specified. For job 2 it is recommended to use 8082


Ensure these variables are set in your environment before running the application. Create different run configurations.
One run configuration for Job 1 and another one for job 2. Set there required environment variables. Run both Flask servers.
You can check the work of the job by running the check_jobs.py, or you can send the POST request (through Postman for example) since jobs are waiting for the HTTP POST request.

Example json body to use as a body for Job 1:
```
   {
       "date": "2022-08-10",
       "raw_dir": "some-path\\raw\\sales\\2022-08-10"
   }
```

Example json body to use as a body for Job 2:
```
   {
       "raw_dir": "some-path\\raw\\sales\\2022-08-10"
       "stg_dir": "some-path\\stg_dir\\sales\\2022-08-10"
   }
```



