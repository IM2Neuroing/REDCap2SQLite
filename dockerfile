# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Install cron
RUN apt-get update && apt-get install -y cron

# Set the working directory
WORKDIR /app

# Copy some files to the working directory
COPY requirements.txt /app/requirements.txt
COPY workflow.py /app/etl.py
COPY PyUtilities /app/PyUtilities
COPY ETL /app/ETL

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the crontab file to the cron.d directory
COPY crontab /etc/cron.d/etl-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/etl-cron

# Apply cron job
RUN crontab /etc/cron.d/etl-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log