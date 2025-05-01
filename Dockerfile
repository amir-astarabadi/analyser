# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install OS-level dependencies (optional, good for dev tools like curl, netcat)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies file
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip --version
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code
COPY . .

# Expose port
EXPOSE 8001

# Run FastAPI app with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
# CMD uvicorn main:app --port 8005 --reload