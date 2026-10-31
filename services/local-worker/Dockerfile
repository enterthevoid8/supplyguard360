FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY worker.py ./
RUN useradd --create-home worker
USER worker
CMD ["python", "-u", "worker.py"]
