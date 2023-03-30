FROM python:3.9
WORKDIR /app/
ADD requirements.txt /app/
RUN pip install -r requirements.txt
ADD . /app/
EXPOSE 8005
CMD ["hypercorn", "main:app", "-b", "0.0.0.0:8000", "--reload"]
