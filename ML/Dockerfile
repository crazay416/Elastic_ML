FROM python:3.9.9-bullseye

WORKDIR /src

#COPY requirements.txt .

COPY . /src

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "ML_backend.py"]