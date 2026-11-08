FROM python:3.11-slim
RUN pip install --no-cache-dir pg8000==1.31.2 scramp==1.4.5 google-auth==2.35.0 requests==2.32.3
WORKDIR /app
COPY gw.py handler_probe.py ./
ENTRYPOINT ["python", "gw.py"]
CMD ["serve"]
