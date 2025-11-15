FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
COPY ./entrypoint.sh .
RUN mkdir ./src/
RUN pip install --no-cache-dir -r requirements.txt
COPY ./src/  ./src/
# CMD ["python", "./src/init.py", "&&", "python", "./src/app.py"]
# CMD ["python", "./src/app.py"]
CMD ["/bin/bash", "./entrypoint.sh"]