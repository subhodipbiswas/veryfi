FROM python:3.9
WORKDIR /app
ADD requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY data.py app.py
CMD ["python", "-u", "app.py"]
