#!/bin/bash

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    export $(grep -v '^#' .env | xargs)
fi

# Function to show usage
show_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  run      Run the FastAPI application (default)"
    echo "  test     Run the tests"
    echo "  test-v   Run the tests in verbose mode"
    echo "  help     Show this help message"
}

# Function to check and prompt for environment variables
check_env_var() {
    local var_name=$1
    local var_value=$(printenv "$var_name")

    if [ -z "$var_value" ]; then
        echo "$var_name is not set. Please provide a value:"
        read -r input_value
        echo "$var_name=$input_value" >> .env
        export "$var_name=$input_value"
    else
        echo "$var_name is already set."
    fi
}

# Check for required environment variables
check_required_env_vars() {
    echo "Checking required environment variables..."
    check_env_var "DATABRICKS_HOST"
    # check_env_var "DATABRICKS_CLUSTER_ID" # not needed when using serverless

    # Check for Service Principal credentials
    if [ -n "$(printenv DATABRICKS_CLIENT_ID)" ] && [ -n "$(printenv DATABRICKS_CLIENT_SECRET)" ]; then
        echo "Using Service Principal for authentication."
    else
        # If Service Principal is not set, check for PAT token
        check_env_var "DATABRICKS_TOKEN"
    fi
}

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt
mkdir -p logs

# Install development requirements if they exist
if [[ "$1" == "test" || "$1" == "test-v" ]]; then
    echo "Installing testing packages..."
    pip install pytest pytest-mock httpx
fi

# Check for required environment variables
check_required_env_vars

# Default action is to run the application
action=${1:-run}

case "$action" in
    run)
        # Run the FastAPI application
        echo "Starting FastAPI application..."
        uvicorn app.main:app --reload --log-config logging_config.json
        ;;
    test)
        # Run the tests
        echo "Running tests..."
        pytest app/tests/
        ;;
    test-v)
        # Run the tests with verbose output
        echo "Running tests with verbose output..."
        pytest app/tests/ -v
        ;;
    help)
        show_usage
        ;;
    *)
        echo "Unknown option: $action"
        show_usage
        exit 1
        ;;
esac