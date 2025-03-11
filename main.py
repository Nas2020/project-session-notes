# #!/usr/bin/env python3
# """
# Adracare Encounter Notes Import Tool

# This script fetches encounter notes from the Adracare API for one or more patients
# and imports them into a local PostgreSQL database.
# """
# import json
# from datetime import datetime

# from config.settings import load_config
# from api.adracare import get_auth_token, get_encounter_notes, extract_notes_data
# from db.database import Database


# def process_patient(db, api_base_url, auth_token, patient_id, default_author_id):
#     """
#     Process a single patient's encounter notes.
    
#     Args:
#         db (Database): Database connection handler
#         api_base_url (str): Adracare API base URL
#         auth_token (str): Authentication token
#         patient_id (str): Adracare patient ID
#         default_author_id (int): Default author user ID
        
#     Returns:
#         dict: Patient processing result information
#     """
#     # Initialize patient result structure
#     patient_result = {
#         "patient_id": patient_id,
#         "messages": [],
#         "notes_found": 0,
#         "inserted_notes": []
#     }
    
#     # Add initial message
#     msg = f"Fetching encounter notes for patient {patient_id}..."
#     print(msg)
#     patient_result["messages"].append(msg)
    
#     try:
#         # Get encounter notes from API
#         encounter_notes_response = get_encounter_notes(api_base_url, auth_token, patient_id)
        
#         # Extract note data from response
#         notes_data = extract_notes_data(encounter_notes_response)
        
#         # Log the notes found
#         msg = f"Found {len(notes_data)} encounter notes for {patient_id}."
#         print(msg)
#         patient_result["messages"].append(msg)
#         patient_result["notes_found"] = len(notes_data)
        
#         # Insert notes into database if any were found
#         if notes_data:
#             insert_msg = "Inserting notes into local database..."
#             print(insert_msg)
#             patient_result["messages"].append(insert_msg)
            
#             # Insert notes and get timestamps of inserted notes
#             inserted_timestamps = db.insert_notes(notes_data, patient_id, default_author_id)
            
#             # Log each inserted note
#             for ts in inserted_timestamps:
#                 note_msg = f"Inserted note created on {ts}"
#                 print(note_msg)
#                 patient_result["messages"].append(note_msg)
            
#             # Update result with inserted notes info
#             patient_result["inserted_notes"] = inserted_timestamps
            
#             # Log success message
#             success_msg = f"Successfully inserted {len(inserted_timestamps)} notes into the database."
#             print(success_msg)
#             patient_result["messages"].append(success_msg)
#         else:
#             # Log if no notes were found
#             no_notes_msg = "No notes to insert."
#             print(no_notes_msg)
#             patient_result["messages"].append(no_notes_msg)
    
#     except Exception as e:
#         # Handle errors with specific patient processing
#         error_msg = f"Error processing patient {patient_id}: {str(e)}"
#         print(error_msg)
#         patient_result["messages"].append(error_msg)
#         patient_result["error"] = str(e)
    
#     return patient_result


# def main():
#     """Main execution function for the import script."""
#     # Load configuration
#     config = load_config()
    
#     # Initialize results structure
#     results = {
#         "timestamp": datetime.now().isoformat(),
#         "patients": []
#     }
    
#     # Initialize database connection
#     db = Database(config["db_config"])
    
#     try:
#         # Connect to database
#         if not db.connect():
#             raise Exception("Failed to connect to the database")
        
#         # Get authentication token
#         print("Getting authentication token...")
#         auth_token = get_auth_token(
#             config["api_base_url"],
#             config["username"],
#             config["password"]
#         )
#         print("Authentication successful!")
        
#         # Process each patient
#         for patient_id in config["patient_ids"]:
#             print(f"\n=== Processing patient {patient_id} ===")
            
#             # Process this patient
#             patient_result = process_patient(
#                 db,
#                 config["api_base_url"],
#                 auth_token,
#                 patient_id,
#                 config["default_author_id"]
#             )
            
#             # Add patient results to overall results
#             results["patients"].append(patient_result)
    
#     except Exception as e:
#         # Handle global errors
#         print(f"Error: {e}")
#         results["error"] = str(e)
    
#     finally:
#         # Always close the database connection
#         db.close()
    
#     # Write results to JSON file
#     with open("results.json", "w") as outfile:
#         json.dump(results, outfile, indent=2)
    
#     print("\nAll processing complete. See 'results.json' for detailed logs.")


# if __name__ == "__main__":
#     main()

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
    # [Unchanged from your version]
    patient_result = {
        "patient_id": patient_id,
        "messages": [],
        "notes_found": 0,
        "inserted_notes": []
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
            insert_msg = "Inserting notes into local database..."
            print(insert_msg)
            patient_result["messages"].append(insert_msg)
            
            inserted_timestamps = db.insert_notes(notes_data, patient_id, default_author_id)
            
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
            "imported_notes": {}  # Track by note ID
        }
    
    # Initialize database connection
    db = Database(config["db_config"])
    
    try:
        # Connect to database
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
            
            # Filter out already imported notes
            new_notes = []
            for note in notes_data:
                note_id = note.get("id")
                if note_id not in results["imported_notes"]:
                    new_notes.append(note)
                else:
                    print(f"Note {note_id} already imported, skipping.")
            
            print(f"Found {len(new_notes)} new notes to import.")
            
            # Process new notes
            if new_notes:
                inserted_records = db.insert_notes(
                    new_notes, 
                    patient_id, 
                    config["default_author_id"]
                )
                
                # Record imported notes
                for i, note in enumerate(new_notes):
                    note_id = note.get("id")
                    if i < len(inserted_records):  # Ensure we have records
                        created_at, db_id = inserted_records[i]
                        results["imported_notes"][note_id] = {
                            "patient_id": patient_id,
                            "created_at": note.get("created_at"),
                            "imported_at": datetime.now().isoformat(),
                            "db_id": db_id  # Store the database ID
                        }
                        # Add to this run's patient results
                        results["patients"][patient_id].append({
                            "note_id": note_id,
                            "created_at": note.get("created_at"),
                            "imported_at": datetime.now().isoformat(),
                            "db_id": db_id  # Store the database ID
                        })
                
                print(f"Successfully inserted {len(inserted_records)} notes into the database.")
            else:
                print("No new notes to insert.")
    
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