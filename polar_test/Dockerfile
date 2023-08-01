FROM python:3.10

LABEL maintainer="Jonathon"

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# 添加这个变量可以避免Python输出缓冲
ENV PYTHONUNBUFFERED=1

#RUN . venv/bin/activate && \
RUN    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt \

COPY . .
EXPOSE 9999
CMD ["python", "week_report_task_test.py"]