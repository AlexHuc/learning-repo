# Start with slim Python 3.13 image for smaller size
FROM python:3.13.11-slim

# Copy uv binary from official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory inside container
WORKDIR /app

# Add virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install all dependencies (pandas, sqlalchemy, psycopg2)
RUN uv sync --locked

# Copy ingestion script
COPY ingest_data.py ingest_data.py 

# Set entry point to run the ingestion script
ENTRYPOINT [ "python", "ingest_data.py" ]


# Example command to run a Postgres container for testing (uncomment and modify as needed)
# docker run -it \
#   -e POSTGRES_USER="root" \
#   -e POSTGRES_PASSWORD="root" \
#   -e POSTGRES_DB="ny_taxi" \
#   -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql \
#   -p 5432:5432 \
#   --network=pg-network \
#   --name pgdatabase \
#   postgres:18

# In another terminal, run pgAdmin on the same network
# docker run -it \
#   -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
#   -e PGADMIN_DEFAULT_PASSWORD="root" \
#   -v pgadmin_data:/var/lib/pgadmin \
#   -p 8085:80 \
#   --network=pg-network \
#   --name pgadmin \
#   dpage/pgadmin4

# Example command to run the container (uncomment and modify as needed)
# docker run -it \
#   --network=pg-network \
#   taxi_ingest:v001 \
#     --pg-user=root \
#     --pg-pass=root \
#     --pg-host=pgdatabase \
#     --pg-port=5432 \
#     --pg-db=ny_taxi \
#     --target-table=yellow_taxi_trips_2021_2 \
#     --year=2021 \
#     --month=2 \
#     --chunksize=100000