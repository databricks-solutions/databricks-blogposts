""" Backend Interface to the Databricks Lakehouse """
import os
import logging
import datetime
import gc

from databricks.connect import DatabricksSession

logger = logging.getLogger('datasource_logger')
logger.setLevel(logging.INFO)

# Create a formatter to define the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')




class DataSource:
    """Initialise a DataSource instance

    Initialise a connection based on env vars
    DATABRICKS_HOST must be set.

    If DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are set, try to connect that way.
    Otherwise, try to connect with PAT Token

    """
    def __init__(self, log_root=None):

        # Create a file handler to write logs to a file
        if log_root:
            log_file = './' + log_root + '/logs/datasource.log'
        else:
            log_file = './logs/datasource.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Create a stream handler to print logs to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        self.session = None
        logger.info(f"Connecting to Databricks")

        # always need to specify the workspace URL
        if os.environ.get("DATABRICKS_HOST"):
            self.databricks_host = os.environ["DATABRICKS_HOST"]
        else:
            logger.error('DATABRICKS_HOST environment variable not set')
            raise Exception('DATABRICKS_HOST environment variable not set')

        # this can be None for Serverless
        if os.environ.get("DATABRICKS_CLUSTER_ID"):
            self.databricks_cluster_id = os.environ["DATABRICKS_CLUSTER_ID"]
        else:
            self.databricks_cluster_id = None
            #Todo - implement Serveless Connect functionality
            logger.error('Serverless DB Connect not implemented yet in this utility')
            raise Exception("Serverless not implemented yet")

        # Service Principal ID, can be None for a PAT connection
        if os.environ.get("DATABRICKS_CLIENT_ID"):
            self.databricks_client_id = os.environ["DATABRICKS_CLIENT_ID"]
        else:
            self.databricks_client_id = None

        # Service Principal Secret, can be None for a PAT connection
        if os.environ.get("DATABRICKS_CLIENT_SECRET"):
            self.databricks_client_secret = True
        else:
            self.databricks_client_secret = False

        # PAT tokn - can be None is using SP
        if os.environ.get("DATABRICKS_TOKEN"):
            self.databricks_token = True
        else:
            self.databricks_token = False

        self.__connect()

    def __connect(self):
        if self.databricks_client_id and self.databricks_client_secret:
            # connect to Service Principal
            os.environ.pop('DATABRICKS_TOKEN', None)
            logger.info(f"Connecting using OAUTH to Service Principal")
            self.session = DatabricksSession.builder.serverless().validateSession(False).getOrCreate()

        elif self.databricks_token:
            # connect using PAT
            logger.info(f"{datetime.datetime.now().strftime('%FT%X')} Connecting using PAT Token")
            self.session = DatabricksSession.builder.serverless().validateSession(False).getOrCreate()
        else:
            logger.error('DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET need to be set *or* DATABRICKS_TOKEN')
            raise Exception('DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET need to be set *or* DATABRICKS_TOKEN')

    def reset(self):
        logger.info(f"Re-Connecting to Databricks")
        self.session = None
        gc.collect()
        self.__connect()



