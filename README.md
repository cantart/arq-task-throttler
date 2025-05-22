# ARQ Task Throttler

This project demonstrates a simple API and worker service setup using Docker. The API receives requests and dispatches tasks to the worker service, which processes them asynchronously, including throttling dispatching feature.

## Prerequisites

- Docker
- Docker Compose

## Getting Started

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```

2. **Build and run the services using Docker Compose:**
   ```bash
   docker-compose up --build -d
   ```
   This command will build the Docker images for the API and worker services and start them in detached mode.

3. **Verify the services are running:**
   You can check the logs of the services using:
   ```bash
   docker-compose logs api
   docker-compose logs worker
   ```

## Running the tests

Currently, there are currently no automated tests in this project.

## Project Structure

- **`api/`**: Contains the FastAPI application.
    - `Dockerfile`: Defines the Docker image for the API service.
    - `main.py`: The main FastAPI application file, defining API endpoints.
    - `arq_dispatcher.py`: Handles dispatching tasks to the ARQ queue.
    - `arq_result_collector.py`: Collects results from completed ARQ tasks.
    - `requirements.txt`: Python dependencies for the API service.
- **`worker/`**: Contains the ARQ worker.
    - `Dockerfile`: Defines the Docker image for the worker service.
    - `main.py`: The main ARQ worker file, defining task functions.
    - `requirements.txt`: Python dependencies for the worker service.
    - **`taskkit/`**: Contains base classes and wrappers for tasks.
    - **`tasks/`**: Contains specific task implementations.
- **`docker-compose.yml`**: Defines the services, networks, and volumes for Docker Compose.
- **`README.md`**: This file.

## API Endpoints

The API service exposes the following endpoints (details can be found in `api/main.py`):

- **POST `/api/v1/task/greeting`**: Dispatches a greeting task.
    - Request Body: `{"name": "string"}`
    - Response: Information about the dispatched task.
- **POST `/api/v1/task/download`**: Dispatches a content download task.
    - Request Body: `{"url": "string"}`
    - Response: Information about the dispatched task.
- **POST `/api/v1/task/blocking_long_running_task`**: Dispatches a blocking long-running task.
    - Request Body: `{"duration": "int"}` (duration in seconds)
    - Response: Information about the dispatched task.
- **POST `/api/v1/task/non_blocking_long_running_task`**: Dispatches a non-blocking long-running task.
     - Request Body: `{"duration": "int"}` (duration in seconds)
     - Response: Information about the dispatched task.
- **GET `/api/v1/task/result/{task_id}`**: Retrieves the result of a completed task.
    - Path Parameter: `task_id` (ID of the task)
    - Response: The result of the task.

## Worker Tasks

The worker service processes the following tasks (defined in `worker/tasks/`):

- **`greeting_task`**: A simple task that returns a greeting message.
- **`download_content_task`**: Downloads content from a given URL.
- **`blocking_long_running_task`**: Simulates a long-running task that blocks the worker.
- **`non_blocking_long_running_task`**: Simulates a long-running task that does not block the worker, allowing it to pick up other tasks.
