# Use an official Python runtime as a parent image
FROM python:3.13-alpine

# Set the working directory in the container
WORKDIR /app

RUN apk update
RUN apk add --no-cache \
    build-base \
    libffi-dev \
    openssl-dev \
    python3-dev \
    py3-pip \
    zlib-dev

# Install Poetry
ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN pip install poetry

# Copy the requirements file into the container
COPY pyproject.toml .
COPY poetry.lock .
COPY server.py .
COPY server-async.py .

# Install any needed packages specified in requirements.txt
RUN poetry config virtualenvs.create false && \
    poetry install --no-root

# Define the command to run your application
# CMD ["python", "server.py"]
# CMD ["python", "server-async.py"]
