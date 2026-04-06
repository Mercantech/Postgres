FROM python:3.12-slim

WORKDIR /app

COPY webapp/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY webapp/app /app

# Bake pensum + demo SQL ind i imagen, så app'en ikke er afhængig af bind-mounts
COPY docs /syllabus
COPY docker/initdb /demo-sql

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]

