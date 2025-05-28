#!/bin/bash

# Script to build and deploy a Databricks app
# Usage: ./deploy_databricks_app.sh <workspace_path> <app_name>

# Check for required parameters
if [ $# -lt 2 ]; then
  echo "Error: Missing required parameters"
  echo "Usage: $0 <workspace_path> <app_name>"
  echo "Example: $0 /Workspace/Apps/data_explorer data_explorer"
  exit 1
fi

# Accept parameters
WORKSPACE_PATH=$1
APP_NAME=$2

echo "==== Databricks App Deployment ===="
echo "Workspace path: $WORKSPACE_PATH"
echo "App name: $APP_NAME"

# Create build directory
BUILD_DIR="databricks_app_build"
echo "Creating build directory: $BUILD_DIR"
rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR
mkdir -p $BUILD_DIR/static
mkdir -p $BUILD_DIR/logs
touch $BUILD_DIR/logs/datasource.log

# Build frontend
echo "Building frontend..."
cd frontend
# Set environment variable for integrated mode
export NEXT_PUBLIC_DEPLOYMENT_MODE=integrated
echo "Set NEXT_PUBLIC_DEPLOYMENT_MODE=integrated for build"
npm run build
# Unset the environment variable
unset NEXT_PUBLIC_DEPLOYMENT_MODE
echo "Unset NEXT_PUBLIC_DEPLOYMENT_MODE"
echo "Copying frontend build to $BUILD_DIR/static..."
cp -r out/* ../$BUILD_DIR/static/
cd ..

# Copy backend files
echo "Copying backend files..."
cd backend
cp app/main.py ../$BUILD_DIR/
cp app/DataSource.py ../$BUILD_DIR/
cd ..

# Create app.yml in build directory
echo "Creating app.yml with cluster ID: $CLUSTER_ID"
cat > $BUILD_DIR/app.yml << EOL
command: ["uvicorn", "main:app", "--workers", "4"]
env:
- name: NEXT_PUBLIC_DEPLOYMENT_MODE
  value: "integrated"
- name: STATIC_FILES_DIR
  value: "static"
EOL

# Copy environment files and requirements
echo "Copying environment and requirements files..."
cp frontend/.env $BUILD_DIR/ 2>/dev/null || echo "No .env file found in frontend"
cp backend/requirements.txt $BUILD_DIR/ 2>/dev/null || echo "No requirements.txt found in backend"

# Import to Databricks workspace
echo "Importing to Databricks workspace: $WORKSPACE_PATH"
databricks workspace import-dir $BUILD_DIR $WORKSPACE_PATH --overwrite

#cleanup build directroy
echo "Cleaning up build dir : $BUILD_DIR"
rm -rf $BUILD_DIR

# Deploy the app
echo "Deploying app: $APP_NAME"
databricks apps deploy $APP_NAME --source-code-path $WORKSPACE_PATH

# # Get URL (works if logged in to Databricks CLI)
# WORKSPACE_URL=$(databricks configure get-value host 2>/dev/null)
# if [ -n "$WORKSPACE_URL" ]; then
#   echo "App deployed! Access at: $WORKSPACE_URL/apps/$APP_NAME"
# else
#   echo "App deployed! Access it from your Databricks workspace Apps section."
# fi

echo "Deployment complete!"