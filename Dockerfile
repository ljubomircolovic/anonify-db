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

# Command to run the application
CMD ["python", "src/main.py"]
