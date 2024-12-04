FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements file first (improves build caching)
COPY requirements.txt /app

# Install dependencies using pip with no-cache-dir for a smaller image
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . /app

# Use CMD to specify the entry point for the container
CMD ["python", "-m", "neso_solar_consumer.app"]
