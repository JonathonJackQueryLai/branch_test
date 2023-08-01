FROM python:3.10

LABEL maintainer="Jonathon"

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Create a virtual environment and activate it
RUN python -m venv venv
ENV PATH="/app/venv/bin:$PATH"
RUN /bin/bash -c "source venv/bin/activate"

# Upgrade pip and install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn

# Copy the application code
COPY . .

# Expose port 5000
EXPOSE 23379

# Create a directory for logs
RUN mkdir -p /app/logs

# Redirect Flask logs to the log file
CMD ["gunicorn", "-w", "1", "demo:app", "-b", "0.0.0.0:23379", "--access-logfile", "/app/logs/flask.log", "--error-logfile", "/app/logs/error.log"]

#CMD ["gunicorn", "-w", "4", "api_server:app", "-b", "0.0.0.0:23372"]

