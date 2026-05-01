FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for psycopg2 and building wheels
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy local code into the container
COPY . /app

# Install dependencies for both generic workers and API
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements_api.txt

# Start command will be overridden by docker-compose for each specific worker
CMD ["python", "api.py"]
