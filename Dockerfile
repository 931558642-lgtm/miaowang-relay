FROM public-cn-beijing.cr.volces.com/public/python:3.12-slim@sha256:89a015f8966466ca875badd24503fcf940fe4f0ebc800a5eb47302efe059f4e8
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY service.py .
USER 65532:65532
EXPOSE 8000
CMD ["python", "-u", "service.py"]
