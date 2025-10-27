import pickle

# Step 1: Load the model
with open("pipeline_v1.bin", "rb") as f_in:
    model = pickle.load(f_in)

# Step 2: Define the record
record = {
    "lead_source": "paid_ads",
    "number_of_courses_viewed": 2,
    "annual_income": 79276.0
}

# Step 3: Make prediction
proba = model.predict_proba([record])[0, 1]
print(f"Probability of conversion: {proba:.3f}")
