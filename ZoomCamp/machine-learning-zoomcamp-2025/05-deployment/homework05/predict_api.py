from fastapi import FastAPI
import pickle

app = FastAPI()

# Load the trained pipeline
with open("pipeline_v1.bin", "rb") as f_in:
    model = pickle.load(f_in)

@app.get("/")
def home():
    return {"message": "Lead scoring API is running 🚀"}

@app.post("/predict")
def predict(client: dict):
    proba = model.predict_proba([client])[0, 1]
    return {"probability": round(float(proba), 3)}
