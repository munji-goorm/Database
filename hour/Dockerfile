FROM python:3.11.0-slim-buster
workdir /usr/src/app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install cryptography
CMD ["python", "rt_air_info2.py"]
