FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install build tools and dependencies
RUN pip install --upgrade pip setuptools wheel

# Copy the pyproject.toml and install the project (including dev dependencies)
COPY pyproject.toml /app
RUN pip install --no-cache-dir ".[dev]"

# Copy the entire project
COPY . /app

# Install the package in editable mode (if needed for testing)
RUN pip install --no-cache-dir -e .

# Set the default command to run tests
CMD ["pytest", "tests/"]
