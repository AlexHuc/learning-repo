import pickle

# Load the model
with open("pipeline_v1.bin", "rb") as f_in:
    model = pickle.load(f_in)

# Example record (must include the same features used in training)
example = {
    'lead_source': 'google',
    'number_of_courses_viewed': 5,
    'annual_income': 75000
}

# Make a prediction
prediction = model.predict([example])
probability = model.predict_proba([example])[0, 1]

print("Prediction:", prediction[0])
print("Probability of conversion:", probability)
