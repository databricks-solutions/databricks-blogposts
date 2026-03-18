class Config:
    """
    Configuration class for application settings.
    """

    def __init__(self):
        """
        Initializes the configuration with default values.
        """

        # Kafka configuration for streaming data ingestion
        # Ensure these secrets are added to the workspace before accessing them

        # Example: How to add these secrets to Databricks using CLI (not via the notebook)
        # databricks secrets create-scope --scope KAFKA_SERVER_SCOPE
        # databricks secrets put-secret KAFKA_SERVER_SCOPE KAFKA_SERVER --string-value "<KAFKA ENDPOINT>"
        # databricks secrets put-secret KAFKA_SERVER_SCOPE KAFKA_USERNAME --string-value "<KAFKA USERNAME>"
        # databricks secrets put-secret KAFKA_SERVER_SCOPE KAFKA_SECRET --string-value "<KAFKA SECRET>"
        self.kafka_config = {
            "kafka_topic": "fraud_feature_eng_example",
            "kafka_credentials_secrets": {
                "scope": "KAFKA_SERVER_SCOPE", # Replace with appropriate scope name. 
                "server": "KAFKA_SERVER",
                "username": "KAFKA_USERNAME",
                "secret": "KAFKA_SECRET",
            },
            "checkpoint_base_path" : "/Volumes/main/fraud_feature_eng_demo/default/checkpoints/"
        }
        
        # Data generation configuration for synthetic data
        self.data_gen_config = {
            "num_users": 10000,
            "num_merchants": 1000,
            "rows_per_second": 1000
        }
        
        # Lakebase configuration for data storage
        self.lakebase_config = {
            "instance_name": "rtm-lakebase-demo",  # Replace with appropriate lakebase instance name
            "database": "databricks_postgres"  # This is the default value. 
        }


# Example usage
if __name__ == "__main__":
    print("Testing config reader...")
    config = Config()
    print(config.kafka_config)
    print(config.data_gen_config)
    print(config.lakebase_config)
    print("\nTest complete!")