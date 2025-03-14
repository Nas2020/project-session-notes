#!/usr/bin/env python3
"""
Adracare Encounter Notes Import Tool

This script fetches encounter notes from the Adracare API for one or more patients
and imports them into a local PostgreSQL database.
"""

import json
from datetime import datetime
from config.settings import load_config
from api.adracare import get_auth_token, get_encounter_notes, extract_notes_data
from db.database import Database

def process_patient(db, api_base_url, auth_token, patient_id, default_author_id):
    """
    Process encounter notes for a single patient.
    
    Args:
        db (Database): Database connection handler
        api_base_url (str): Adracare API base URL
        auth_token (str): Authentication token for Adracare API
        patient_id (str): External patient ID from Adracare
        default_author_id (int): Default author user ID (used only when created_by_account_id is None)
        
    Returns:
        dict: Results of patient processing
    """
    # Initialize result structure
    patient_result = {
        "patient_id": patient_id,
        "messages": [],
        "notes_found": 0,
        "processed_notes": []
    }
    
    msg = f"Fetching encounter notes for patient {patient_id}..."
    print(msg)
    patient_result["messages"].append(msg)
    
    try:
        encounter_notes_response = get_encounter_notes(api_base_url, auth_token, patient_id)
        notes_data = extract_notes_data(encounter_notes_response)
        
        msg = f"Found {len(notes_data)} encounter notes for {patient_id}."
        print(msg)
        patient_result["messages"].append(msg)
        patient_result["notes_found"] = len(notes_data)
        
        if notes_data:
            sql_gen_msg = "Generating SQL statements for notes..."
            print(sql_gen_msg)
            patient_result["messages"].append(sql_gen_msg)
            
            # Always use append mode for individual patient processing to maintain all notes
            processed_records = db.generate_notes_sql(notes_data, patient_id, default_author_id, "output.sql", "a")
            
            for created_at, _ in processed_records:
                note_msg = f"Generated SQL for note created on {created_at}"
                print(note_msg)
                patient_result["messages"].append(note_msg)
            
            patient_result["processed_notes"] = processed_records
            
            success_msg = f"Successfully generated SQL for {len(processed_records)} notes."
            print(success_msg)
            patient_result["messages"].append(success_msg)
        else:
            no_notes_msg = "No notes to process."
            print(no_notes_msg)
            patient_result["messages"].append(no_notes_msg)
    
    except Exception as e:
        error_msg = f"Error processing patient {patient_id}: {str(e)}"
        print(error_msg)
        patient_result["messages"].append(error_msg)
        patient_result["error"] = str(e)
    
    return patient_result

def main():
    """Main execution function for the import script."""
    # Load configuration
    config = load_config()
    
    # Initialize results structure - or load existing one if it exists
    results_file = "results.json"
    try:
        with open(results_file, "r") as infile:
            results = json.load(infile)
            # Add timestamp for this run
            results["last_run"] = datetime.now().isoformat()
    except (FileNotFoundError, json.JSONDecodeError):
        # Create new results if file doesn't exist or is invalid
        results = {
            "first_run": datetime.now().isoformat(),
            "last_run": datetime.now().isoformat(),
            "patients": {},
            "processed_notes": {}  # Track by note ID
        }
    
    # Initialize database connection
    db = Database(config["db_config"])
    
    try:
        # Connect to database (for lookups only, not for insertions)
        if not db.connect():
            raise Exception("Failed to connect to the database")
        
        # Load configuration with dynamic patient ID fetching
        print("Fetching patient IDs from providers...")
        config = load_config(fetch_patient_ids=True, db=db)
        
        # Get authentication token
        print("Getting authentication token...")
        auth_token = get_auth_token(
            config["api_base_url"],
            config["username"],
            config["password"]
        )
        print("Authentication successful!")
        
        # Process each patient
        for patient_id in config["patient_ids"]:
            print(f"\n=== Processing patient {patient_id} ===")
            
            # Initialize patient in results if not present
            if patient_id not in results["patients"]:
                results["patients"][patient_id] = []
            
            # Get encounter notes
            encounter_notes_response = get_encounter_notes(
                config["api_base_url"], 
                auth_token, 
                patient_id
            )
            
            notes_data = extract_notes_data(encounter_notes_response)
            print(f"Found {len(notes_data)} encounter notes for {patient_id}.")
            
            # Filter out already processed notes
            new_notes = []
            for note in notes_data:
                note_id = note.get("id")
                if note_id not in results["processed_notes"]:
                    new_notes.append(note)
                else:
                    print(f"Note {note_id} already processed, skipping.")
            
            print(f"Found {len(new_notes)} new notes to process.")
            
            # Process new notes
            if new_notes:
                # Use write mode for first patient, append mode for subsequent patients
                file_mode = "w" if patient_id == config["patient_ids"][0] else "a"
                processed_records = db.generate_notes_sql(
                    new_notes, 
                    patient_id, 
                    config["default_author_id"],
                    "output.sql",
                    file_mode,
                    limit=1000 
                )
                
                # Record processed notes
                for i, note in enumerate(new_notes):
                    note_id = note.get("id")
                    if i < len(processed_records):  # Ensure we have records
                        created_at, _ = processed_records[i]
                        results["processed_notes"][note_id] = {
                            "patient_id": patient_id,
                            "created_at": note.get("created_at"),
                            "processed_at": datetime.now().isoformat(),
                            "sql_generated": True
                        }
                        # Add to this run's patient results
                        results["patients"][patient_id].append({
                            "note_id": note_id,
                            "created_at": note.get("created_at"),
                            "processed_at": datetime.now().isoformat(),
                            "sql_generated": True
                        })
                
                print(f"Successfully generated SQL for {len(processed_records)} notes.")
            else:
                print("No new notes to process.")
    
    except Exception as e:
        print(f"Error: {e}")
        if "errors" not in results:
            results["errors"] = []
        results["errors"].append({
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        })
    
    finally:
        db.close()
    
    # Write results to JSON file
    with open(results_file, "w") as outfile:
        json.dump(results, outfile, indent=2)
    
    print(f"\nAll processing complete. See '{results_file}' for detailed logs.")

if __name__ == "__main__":
    main()