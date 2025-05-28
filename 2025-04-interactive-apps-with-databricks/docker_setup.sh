#!/bin/bash

# Exit on error
set -e

# Default values
MODE="separate"
FRONTEND_DOCKERFILE="./frontend/Dockerfile"
BACKEND_DOCKERFILE="./backend/Dockerfile"
COMPOSE_FILE="./docker-compose.yml"

# Function to display help information
show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -m, --mode MODE       Mode to run services (separate or compose, default: separate)"
    echo "  -f, --frontend PATH   Path to frontend Dockerfile (default: ./frontend/Dockerfile)"
    echo "  -b, --backend PATH    Path to backend Dockerfile (default: ./backend/Dockerfile)"
    echo "  -c, --compose PATH    Path to compose file (default: ./podman-compose.yml)"
    echo "  -h, --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    Run frontend and backend separately using default Dockerfiles"
    echo "  $0 -m compose         Run using podman-compose"
    echo "  $0 -f ./custom/Dockerfile.frontend -b ./custom/Dockerfile.backend"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        -f|--frontend)
            FRONTEND_DOCKERFILE="$2"
            shift 2
            ;;
        -b|--backend)
            BACKEND_DOCKERFILE="$2"
            shift 2
            ;;
        -c|--compose)
            COMPOSE_FILE="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Function to ensure podman machine is running
ensure_podman_machine() {
    # Check if podman machine exists and is running
    if podman machine list 2>/dev/null | grep -q "Currently running"; then
        echo "Podman machine is already running."
    else
        echo "Starting podman machine..."
        # Try to start existing machine, if it fails, create a new one
        podman machine start 2>/dev/null || podman machine init && podman machine start
        echo "Podman machine started successfully."
    fi
}

# Function to clean up images
cleanup_images() {
    echo "Cleaning up containers and images..."
    
    # Stop any running containers (ignore errors)
    podman ps -q | xargs podman stop 2>/dev/null || true
    
    # Remove containers with specific names if they exist
    podman container exists frontend 2>/dev/null && podman rm frontend
    podman container exists backend 2>/dev/null && podman rm backend
    
    # Prune all images forcefully
    podman image prune -af
    
    echo "Cleanup completed."
}

# Function to run services separately
run_separately() {
    echo "Running frontend and backend as separate services..."
    
    # Build and run backend
    echo "Building backend..."
    podman build -t backend:latest --no-cache -f "$BACKEND_DOCKERFILE" $(dirname "$BACKEND_DOCKERFILE")
    
    echo "Running backend..."
    podman run -d --name backend -p 8000:8000 backend:latest
    
    # Build and run frontend
    echo "Building frontend..."
    podman build -t frontend:latest --no-cache -f "$FRONTEND_DOCKERFILE" $(dirname "$FRONTEND_DOCKERFILE")
    
    echo "Running frontend..."
    podman run -d --name frontend -p 3000:3000 frontend:latest
    
    echo "Services started:"
    echo "- Frontend: http://localhost:3000"
    echo "- Backend: http://localhost:8000"
}

# Function to run services using podman-compose
run_compose() {
    echo "Running services using podman-compose..."
    
    # Check if podman-compose is installed
    if ! command -v podman-compose &> /dev/null; then
        echo "Error: podman-compose is not installed. Please install it first."
        exit 1
    fi
    
    # Check if compose file exists
    if [ ! -f "$COMPOSE_FILE" ]; then
        echo "Error: Compose file not found at $COMPOSE_FILE"
        exit 1
    fi
    
    # Stop any running compose services
    podman-compose -f "$COMPOSE_FILE" down || true
    
    # Build and run with compose
    echo "Building and running services with podman-compose..."
    podman-compose -f "$COMPOSE_FILE" build --no-cache
    podman-compose -f "$COMPOSE_FILE" up -d
    
    echo "Services started using podman-compose."
}

# Main execution
echo "Service Runner Script"
echo "--------------------"

# Ensure podman machine is running
ensure_podman_machine

# Clean up images
cleanup_images

# Run based on selected mode
if [ "$MODE" = "compose" ]; then
    run_compose
else
    run_separately
fi

echo "Done!"