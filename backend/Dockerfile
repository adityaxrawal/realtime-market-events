      # backend/Dockerfile
      # Defines the Docker image for the FastAPI backend service

      # Use an official Python runtime as a parent image
      FROM python:3.10-slim

      # Set the working directory in the container
      WORKDIR /app

      # Prevent python from writing pyc files to disc (optional)
      ENV PYTHONDONTWRITEBYTECODE 1
      # Ensure python output is sent straight to terminal (useful for logs)
      ENV PYTHONUNBUFFERED 1

      # Install system dependencies if needed (e.g., for psycopg2)
      # RUN apt-get update && apt-get install -y --no-install-recommends gcc build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

      # Install Python dependencies
      # Copy only requirements first to leverage Docker cache
      COPY requirements.txt requirements.txt
      RUN pip install --no-cache-dir --upgrade pip && \
          pip install --no-cache-dir -r requirements.txt

      # Copy the rest of the application code into the container
      COPY . .

      # Expose the port the app runs on (should match the port Uvicorn runs on)
      EXPOSE 8000

      # Command to run the application using Uvicorn
      # Use 0.0.0.0 to allow connections from outside the container
      CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
      