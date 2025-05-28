# Frontend - DB Connect WebApp

## Overview

The frontend of the **DB Connect WebApp** is a user interface built to interact with datasets, perform analysis, and visualize data. It provides features like dataset selection, filtering, grouping, and querying, making it easier for users to explore and analyze data.

## Technologies Used

The frontend is built using the following technologies:

- **TypeScript**: For type-safe JavaScript development.
- **React**: As the core library for building the user interface.
- **Next.js**: For server-side rendering and routing.
- **Tailwind CSS**: For styling the application.
- **Flask (API Integration)**: The frontend communicates with a Flask-based backend API for fetching datasets and performing operations.

## Features

- Dataset browsing and selection.
- Query building with filters, group-by, and aggregations.
- Visualization of query results.

## Getting Started

Follow these steps to set up and run the frontend locally:

### Prerequisites

Ensure you have the following installed on your system:

- **Node.js** (v16 or later)
- **npm** or **yarn**
- **Python** (for backend API if needed)

### Installation


1. Install dependencies:

   ```bash
   npm install
   ```

   Or, if you prefer `yarn`:

   ```bash
   yarn install
   ```

### Running the Development Server

To start the development server, run:
   
   ```bash
   # Run in development mode (default)
   ./run.sh

   # OR explicitly specify development mode
   ./run.sh dev

   # Run in production mode
   ./run.sh prod
   ```

This script will start the frontend server. By default, the application will be available at [http://localhost:3000](http://localhost:3000).

### Environment Variables

The frontend relies on environment variables for API endpoints. Create a `.env.local` file in the `frontend` directory and add the following variables:

```env
NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000/api/v1
```

Replace the URL with the actual backend API base URL.
