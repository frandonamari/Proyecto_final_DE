FROM python:3.9.1

WORKDIR /app

COPY requirements.txt .
COPY entregafinal.py .


RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "entregafinal:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]