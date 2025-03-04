import os
import json
import requests
import psycopg2
import re
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

# 1. Load environment variables from .env
load_dotenv()

# 2. Read config.json for patient IDs
with open("config.json", "r") as f:
    config = json.load(f)

PATIENT_IDS = config.get("patient_ids", [])  # This is now an array of patient IDs

# Environment variables for the Adracare API
API_BASE_URL = os.getenv("ADRA_BASE_URL")
USERNAME = os.getenv("ADRA_USERNAME")
PASSWORD = os.getenv("ADRA_PASSWORD")

# Local DB references (if these are static for all notes)
LOCAL_DB_PATIENT_ID = 5
LOCAL_DB_AUTHOR_ID = 1

# Database configuration from environment
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "database": os.getenv("DB_DATABASE", "rocketdoctor_development"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "")
}

def get_auth_token():
    """Get authentication token from Adracare API."""
    url = f"{API_BASE_URL}/account_token"
    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }
    
    response = requests.post(url, json=payload)
    if response.status_code not in [200, 201]:
        raise Exception(f"Authentication failed: {response.status_code} - {response.text}")
    
    data = response.json()
    return data["jwt"]

def get_encounter_notes(auth_token, patient_id):
    """Get encounter notes for a specific patient."""
    url = f"{API_BASE_URL}/patients/{patient_id}/encounter_notes"
    headers = {
        "Authorization": f"Bearer {auth_token}"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to get encounter notes: {response.status_code} - {response.text}")
    
    return response.json()

def extract_text_from_html(html_content):
    """Extract text from HTML content and preserve paragraphs."""
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    paragraphs = []
    for p in soup.find_all(['p', 'div']):
        text = p.get_text(strip=True)
        if text:
            paragraphs.append(text)
    
    # Join paragraphs with double newlines for better readability
    text = "\n\n".join(paragraphs)
    
    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    
    return text.strip()

def insert_notes_to_db(notes_data):
    """Insert processed notes into the local PostgreSQL database.
    
    Returns:
        A list of created_at timestamps for each inserted note.
    """
    conn = None
    inserted_timestamps = []
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO patient_notes (notes, patient_id, author_user_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s);
        """
        
        for note in notes_data:
            note_text = extract_text_from_html(note.get("notes", ""))
            created_at = note.get("created_at")
            updated_at = note.get("updated_at")

            cursor.execute(sql, (
                note_text,
                LOCAL_DB_PATIENT_ID,  # or dynamic if needed
                LOCAL_DB_AUTHOR_ID,   # or dynamic if needed
                created_at,
                updated_at
            ))
            
            inserted_timestamps.append(created_at)
        
        conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    
    return inserted_timestamps

def main():
    # Structure to hold logs/results for all patients
    results = {
        "timestamp": datetime.now().isoformat(),
        "patients": []
    }

    try:
        # Get authentication token once
        print("Getting authentication token...")
        auth_token = get_auth_token()
        print("Authentication successful!")
        
        # Loop over each patient in PATIENT_IDS
        for patient_id in PATIENT_IDS:
            print(f"\n=== Processing patient {patient_id} ===")
            
            # Initialize a patient-specific result entry
            patient_result = {
                "patient_id": patient_id,
                "messages": [],
                "notes_found": 0,
                "inserted_notes": []  # we'll store the created_at timestamps here
            }
            
            # 1. Fetch encounter notes for this patient
            msg = f"Fetching encounter notes for patient {patient_id}..."
            print(msg)
            patient_result["messages"].append(msg)

            encounter_notes_response = get_encounter_notes(auth_token, patient_id)
            
            # 2. Extract relevant data
            notes_data = []
            for item in encounter_notes_response.get("data", []):
                attributes = item.get("attributes", {})
                note_data = {
                    "notes": attributes.get("notes", ""),
                    "created_at": attributes.get("created_at"),
                    "updated_at": attributes.get("updated_at")
                }
                notes_data.append(note_data)
            
            msg = f"Found {len(notes_data)} encounter notes for {patient_id}."
            print(msg)
            patient_result["messages"].append(msg)
            patient_result["notes_found"] = len(notes_data)
            
            # 3. Insert into the database
            if notes_data:
                insert_msg = "Inserting notes into local database..."
                print(insert_msg)
                patient_result["messages"].append(insert_msg)

                inserted_timestamps = insert_notes_to_db(notes_data)
                
                # For each inserted note, log or store the created_at
                for ts in inserted_timestamps:
                    note_msg = f"Inserted note created on {ts}"
                    print(note_msg)
                    patient_result["messages"].append(note_msg)
                
                patient_result["inserted_notes"] = inserted_timestamps
                
                success_msg = f"Successfully inserted {len(inserted_timestamps)} notes into the database."
                print(success_msg)
                patient_result["messages"].append(success_msg)
            else:
                no_notes_msg = "No notes to insert."
                print(no_notes_msg)
                patient_result["messages"].append(no_notes_msg)
            
            # Add this patient's results to the overall results
            results["patients"].append(patient_result)
    
    except Exception as e:
        print(f"Error: {e}")
        # In case of a global error, you could store it in the JSON as well
        results["error"] = str(e)

    # Write the results to a JSON file
    with open("results.json", "w") as outfile:
        json.dump(results, outfile, indent=2)

    print("\nAll processing complete. See 'results.json' for detailed logs.")

if __name__ == "__main__":
    main()