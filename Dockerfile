# Use a slim Python image for efficiency
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Create the logs and exports directories
RUN mkdir -p logs exports

# U Dockerfile dodaj pre CMD-a ili Run-a
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Command to run the application
CMD ["python", "src/main.py"]
