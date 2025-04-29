# Interactive web application using Databricks Connect and Flask
This is a simple example of how to create an interactive web application using Databricks Connect and Flask.
TODO: Add more details about the project.

## Prerequisites

Python version:
```python
python 3.12.7
```

Install the required libraries:
```bash
pip install -r requirements.txt
```

### Backend Connectivity to Databricks
#### DB Connect ####  
The application uses [Databricks Connect](https://docs.databricks.com/en/dev-tools/databricks-connect/index.html) to connect the backend API services to Databricks compute services.

#### Cluster Configuration ####
A shared Databricks cluster is required for providing backend Databricks data and compute services.  
*ToDo* Serverless Compute support has not been added to this app yet (WIP)

#### M2M OAuth Service Principal (SP) #### 
Authentication to Databricks uses OAuth M2M  [Machine-to-Machine OAuth](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html)  
A Personal Access Token (PAT) can be used for development and testing. 

#### Cluster Privileges 
Grant access on the Cluster to the SP `Application ID`.  
*Compute* -> *More* (top right Web GUI) -> *Permissions* -> add the Service Principal.

#### Set Databricks DB Connect Environment Variables
To connect to a shared cluster using a SP set the following environment variables
+ `DATABRICKS_HOST`  - This is the workspace URL, EG `https://<my-workspace>.databricks.com/`  
+ `DATABRICKS_CLUSTER_ID` - This is the name of the shared cluster to connect to.  
+ `DATABRICKS_CLIENT_ID` - This is the M2M OAuth SP *Application ID* to authenticate with.    
+ `DATABRICKS_CLIENT_SECRET` - This is the M2M OAuth SP *Secret* that is shown when a new SP Secret is created.  
Alternatively, for local development and testing, 
+ `DATABRICKS_TOKEN` can be set *instead of* `DATABRICKS_CLIENT_ID` & `DATABRICKS_CLIENT_SECRET`

Using environment variables to set the access parameters for the backend makes it easy to deploy the app and securely integrate it into an enterprise environment.  
A wrapper for extracting connect details and secrets from a secret store can be incorporated into the deployment to avoid storing secrets in local files.   

#### Connecting via `backend.DataSource()` object

`backend/DataSource.py` allows a backend `DataSource()` instance to be initialised in Python code which has a Spark session connection as an object-instance attribute.

EG 

```
from .DataSource import DataSource

datasource = DataSource()
df = datasource.session.sql(query_string)
```

See the example `./backend/examples/backend_data_example.py` for more detail.  

DB Connect Session timeouts can be handled by handling exceptions and making a single retry after calling the `Datasource.reset()` method:

```python
for attempt in range(2):  # Try twice at most
try:
    # Execute query using your datasource
    df = datasource.session.sql(query_string)
    
except Exception as e:
    if attempt == 0:  # only runs once; after the first failure try to re-initialise the datasource
        datasource.reset()
    else:
        raise HTTPException(status_code=500, detail={
                    "error": "Unexpected Error",
                    "message": str(e),
                    "query_json": query_string
                })        

```

#  DEVELOPMENT SETUP

This project is structured into two components: the **frontend** and **backend**, each with its own development setup and `run.sh` script for easier execution.

---

## Project Structure

```bash
DB-CONNECT-WEBAPP
├── backend
│   ├── app/                # Backend application code
│   ├── .venv               # Backend virtual environment
│   ├── requirements.txt    # Backend dependencies
│   ├── run.sh              # Script to set up and run the backend
│   └── README.md           # Backend-specific README
├── frontend
│   ├── app/                # Frontend application code
│   ├── .venv               # Frontend virtual environment
│   ├── requirements.txt    # Frontend dependencies
│   ├── run.py              # Frontend entry point
│   ├── run.sh              # Script to set up and run the frontend
│   └── README.md           # Frontend-specific README
└── README.md               # This file
```

---

## Running the Backend

Further details in [./backend/README.md](./backend/Readme.md)

Navigate to the `backend` directory and execute the `run.sh` script:

```bash
cd backend
./run.sh
```

`run.sh` creates a local venv, installs any Python dependancies and then runs the Backend FastAPI in a Uvicorn web server.

---

## Running the Frontend

Navigate to the `frontend` directory and execute the `run.sh` script:

```bash
cd frontend
./setup.sh
./run.sh
```
---

## Running Both Services Together

You can set up and run both the frontend and backend by running their respective `run.sh` scripts in separate terminal windows:

```bash
# Terminal 1: Run backend
cd backend
./run.sh

# Terminal 2: Run frontend
cd frontend
./setup.sh
./run.sh
```

---      

## Running Both services in docker


```bash
 docker-compose up --build
```

---  