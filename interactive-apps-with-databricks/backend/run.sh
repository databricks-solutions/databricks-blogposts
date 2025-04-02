#!/bin/bash

# Function to show usage
show_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  run      Run the FastAPI application (default)"
    echo "  test     Run the tests"
    echo "  test-v   Run the tests in verbose mode"
    echo "  help     Show this help message"
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

# Install development requirements if they exist
if [[ "$1" == "test" || "$1" == "test-v" ]]; then
        echo "Installing testing packages..."
        pip install pytest pytest-mock httpx
fi

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
