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
## Set up and run Airflow
Folder lesson_07 contains the docker-compose.yaml file with the configuration to run Airflow in Docker with local
executor. The config done in such a way that allows to do post requests to Job 1 and Job 2 which are running outside the docker.
In order to run the Airflow and jobs:

#### Initialize Airflow db
Go to the lesson_07, and then from command line run:
 ```
 docker-compose up airflow-init
 ```
#### Run docker-compose
One initialization of Airflow is completed, run the docker-compose:
 ```
 docker-compose up
 ```
#### Create http connections in Airflow admin
2 out of 3 tasks in the dag are making POST requests to Job 1 and Job 2. For doing this,
go to Connection on Airflow UI and create 2 HTTP connection. 

First connection ID:  ```flask_job_1_fetch```

Port for flask_job_1_fetch: ```8081```

Second connection ID:  ```flask_job_2_convert_to_avro```

Port for flask_job_1_fetch: ```8082```

For both the host is:  ```http://host.docker.internal```

#### Set the BASE_DIR env variable

The BASE_DIR env variable is specified in docker-compose file for Airflow to fetch

```BASE_DIR: 'D:\learning\robot-dreams-data-eng-bootcamp-hws\'```

#### Create .env file
Create the .env file in the lesson_07 folder. Then open terminal and type:
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Once above steps are done, you can trigger the DAG.