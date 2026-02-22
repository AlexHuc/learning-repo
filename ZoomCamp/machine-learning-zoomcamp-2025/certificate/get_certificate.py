import os
from hashlib import sha1
from dotenv import load_dotenv

# load .env file
load_dotenv()

def compute_hash(email):
    return sha1(email.encode('utf-8')).hexdigest()

def compute_certificate_id(email):
    email_clean = email.lower().strip()
    return compute_hash(email_clean + '_')

# read from env
email = os.getenv("EMAIL")
cohort = int(os.getenv("COHORT"))
course = "ml-zoomcamp"

your_id = compute_certificate_id(email)
url = f"https://certificate.datatalks.club/{course}/{cohort}/{your_id}.pdf"

print("certificate id:", your_id)
print("certificate url:", url)