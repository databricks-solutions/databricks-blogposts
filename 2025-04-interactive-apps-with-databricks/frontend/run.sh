#!/bin/bash

# Exit on error
set -e

# Default to development mode unless specified
MODE=${1:-dev}

echo "Setting up Next.js frontend application..."

# Load environment variables from .env.local or .env if they exist
if [ -f .env.local ]; then
    echo "Loading environment variables from .env.local..."
    export $(grep -v '^#' .env.local | xargs)
elif [ -f .env ]; then
    echo "Loading environment variables from .env..."
    export $(grep -v '^#' .env | xargs)
else
    echo "No .env.local or .env file found. A new .env.local file will be created if needed."
fi

# Function to check and prompt for environment variables
check_env_var() {
    local var_name=$1
    local var_value=$(printenv "$var_name")

    if [ -z "$var_value" ]; then
        echo "$var_name is not set. Please provide a value:"
        read -r input_value
        if [ ! -f .env.local ]; then
            echo "Creating .env.local file..."
            touch .env.local
        fi
        echo "$var_name=$input_value" >> .env.local
        export "$var_name=$input_value"
    else
        echo "$var_name is already set."
    fi
}

# Check for required environment variables
check_required_env_vars() {
    echo "Checking required environment variables..."
    check_env_var "NEXT_PUBLIC_API_BASE_URL"
    check_env_var "NEXT_PUBLIC_CATALOG"
    check_env_var "NEXT_PUBLIC_DATABASE"
}

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "Node.js is not installed. Please install Node.js 18 or later."
    exit 1
fi

# Check Node.js version (should be 18 or higher)
NODE_VERSION=$(node -v | cut -d 'v' -f 2 | cut -d '.' -f 1)
if [ "$NODE_VERSION" -lt 18 ]; then
    echo "Your Node.js version is too old. Please upgrade to Node.js 18 or later."
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
npm ci

# Check for required environment variables
check_required_env_vars

# Print the backend URL
echo "Connecting to backend at: $NEXT_PUBLIC_API_BASE_URL"

if [ "$MODE" = "dev" ]; then
    # Start in development mode
    echo "Starting the application in development mode on port 3000..."
    npm run dev
else
    # Build the Next.js application
    echo "Building the application for production..."
    npm run build
    
    # Set production environment
    export NODE_ENV=production
    
    # Start the application using serve for static exports
    echo "Starting the application in production mode on port 3000..."
    npx serve@latest out -p 3000
fi

echo "Application is running on http://localhost:3000"