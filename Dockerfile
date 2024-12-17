FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies from pyproject.toml
COPY pyproject.toml /app

# Install core and development dependencies
RUN pip install --no-cache-dir ".[dev]"

# Copy the source code
COPY . /app

# Default command for running pytest
CMD ["pytest", "tests/"]
