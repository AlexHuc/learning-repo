FROM python:3.9.6-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy your model and prediction service code
COPY *.py .
COPY *.bin ./

EXPOSE 9696

# Replace "predict.py" with your actual Python script that runs the service
ENTRYPOINT ["gunicorn", "--bind=0.0.0.0:9696", "predict:app"]

#  docker build -t ride-duration-prediction-model:v1 .
# docker run --name ride-duration-prediction-service -p 9696:9696 ride-duration-prediction-model:v1