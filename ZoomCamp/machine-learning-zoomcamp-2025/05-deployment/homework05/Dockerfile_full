FROM agrigorev/zoomcamp-model:2025

WORKDIR /app

COPY ["pyproject.toml", "uv.lock", "./"]
COPY ["predict_api.py", "pipeline_v1.bin", "./"]

# Install uv and your dependencies
RUN pip install uv
RUN uv sync --frozen

EXPOSE 9696

CMD ["uv", "run", "uvicorn", "predict_api:app", "--host=0.0.0.0", "--port=9696"]
