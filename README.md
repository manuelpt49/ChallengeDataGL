# FastAPI Application with SQLite

This is a FastAPI application that uses SQLite as its database. The application provides endpoints for creating a SQLite database, uploading CSV files to create or replace tables, inserting data into tables, and performing specific queries.

## Prerequisites

Before running the application, ensure that you have the following installed:

- Python 3.7 or higher
- [Docker](https://www.docker.com/) (optional)

## Installation

1. **Clone the Repository:**

    ```bash
    git clone https://github.com/manuelpt49/ChallengeDataGL.git
    cd your-repository

2. **Install Dependencies:**

    ```bash
    pip install -r requirements.txt

## Running the Application
### Option 1: Run Locally

    ```bash
    uvicorn main:app --reload

### Option 2: Run with Docker
1. **Build the image**
    
    ```bash
    docker build -t myimage .

2. **Run the image**
    
    ```bash
    docker run -d --name mycontainer -p 8000:8000 myimage

## Endpoints
1. GET/createdb: Create SQLite database and tables.
2. POST/upload: Upload CSV file to create or replace tables. This endpoint receives a parameter *Table*. To upload a file in Employee table, you must to set "employees". For Job - "jobs" and for Department - "departments". Otherwise, loading the data won't be successful.
3. 