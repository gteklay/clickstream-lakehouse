# ~/clickstream-lakehouse/generator/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# ⚡ BUILD OPTIMIZATION: Copy dependencies separately to maximize Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your modular, pattern-matching data stream script into the container filesystem
COPY producer.py .

#  THE REAL-TIME UNBUFFERED FIREWALL: 
# The "-u" flag forces Python to flush its console logs to stdout instantly.
# This lets you run "docker logs -f lakehouse_generator" to monitor events with zero lag!
CMD ["python", "-u", "producer.py"]
