"""
Functions for interacting with the Adracare API.
"""
import requests
import json
import time


def get_auth_token(api_base_url, username, password):
    """
    Get authentication token from Adracare API.
    
    Args:
        api_base_url: Base URL for the Adracare API
        username: Adracare API username 
        password: Adracare API password
        
    Returns:
        str: JWT authentication token
        
    Raises:
        Exception: If authentication fails
    """
    url = f"{api_base_url}/account_token"
    payload = {
        "username": username,
        "password": password
    }
    
    response = requests.post(url, json=payload)
    if response.status_code not in [200, 201]:
        raise Exception(f"Authentication failed: {response.status_code} - {response.text}")
    
    data = response.json()
    return data["jwt"]


def get_encounter_notes(api_base_url, auth_token, patient_id):
    """
    Get encounter notes for a specific patient from the Adracare API.
    
    Args:
        api_base_url: Base URL for the Adracare API
        auth_token: JWT authentication token
        patient_id: Adracare patient ID
        
    Returns:
        dict: JSON response containing encounter notes
        
    Raises:
        Exception: If the API request fails
    """
    url = f"{api_base_url}/patients/{patient_id}/encounter_notes"
    headers = {
        "Authorization": f"Bearer {auth_token}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            error_message = f"Failed to get encounter notes: {response.status_code} - {response.text}"
            return {"error": error_message, "data": []}
        
        return response.json()
    except Exception as e:
        return {"error": f"Exception occurred: {str(e)}", "data": []}


def extract_notes_data(encounter_notes_response):
    """
    Extract relevant note data from the API response.
    
    Args:
        encounter_notes_response: JSON response from the Adracare API
        
    Returns:
        list: List of dictionaries containing note data
    """
    notes_data = []
    
    # Check if the response is a dictionary
    if not isinstance(encounter_notes_response, dict):
        print(f"Warning: Unexpected response type: {type(encounter_notes_response)}")
        return notes_data
    
    # Get the data array, which should contain the notes
    data_array = encounter_notes_response.get("data", [])
    
    # Check if data_array is actually a list
    if not isinstance(data_array, list):
        print(f"Warning: Expected 'data' to be a list, got: {type(data_array)}")
        return notes_data
    
    # Process each item in the data array
    for item in data_array:
        try:
            # Check if item is a dictionary
            if not isinstance(item, dict):
                print(f"Warning: Expected note item to be a dict, got: {type(item)}")
                continue
            
            # Get the note ID with validation
            note_id = item.get("id", "")
            
            # Get attributes with validation
            attributes = item.get("attributes", {})
            if not isinstance(attributes, dict):
                print(f"Warning: Expected 'attributes' to be a dict, got: {type(attributes)}")
                attributes = {}
            
            # Create note data dictionary with safe gets ; TO TEST TIME ERROR ISSUE modified creaetd_at to updated_at
            note_data = {
                "id": note_id,
                "notes": attributes.get("notes", ""),
                "created_at": attributes.get("updated_at", ""),
                "updated_at": attributes.get("updated_at", ""),
                "patient_id": attributes.get("patient_id", ""),
                "created_by_account_id": attributes.get("created_by_account_id", "")
            }
            
            notes_data.append(note_data)
        except Exception as e:
            print(f"Error processing note: {str(e)}")
            # Continue to the next note instead of failing
            continue
    
    return notes_data

def process_all_patients(api_base_url, auth_token, patient_ids, max_retries=3, retry_delay=2):
    """
    Process all patients and collect their encounter notes with retry logic.
    
    Args:
        api_base_url: Base URL for the Adracare API
        auth_token: JWT authentication token
        patient_ids: List of Adracare patient IDs
        max_retries: Maximum number of retry attempts for API calls
        retry_delay: Delay in seconds between retries
        
    Returns:
        dict: Results containing patient data and any errors
    """
    results = {
        "successful_patients": 0,
        "failed_patients": 0,
        "total_notes_processed": 0,
        "patient_data": {},
        "errors": []
    }
    
    for index, patient_id in enumerate(patient_ids):
        print(f"=== Processing patient {patient_id} ({index+1}/{len(patient_ids)}) ===")
        
        # Add retry logic for API calls
        retry_count = 0
        success = False
        patient_result = None
        
        while retry_count < max_retries and not success:
            if retry_count > 0:
                print(f"  Retry attempt {retry_count}/{max_retries}...")
                time.sleep(retry_delay)
                
            try:
                # Get encounter notes with error handling
                encounter_notes = get_encounter_notes(api_base_url, auth_token, patient_id)
                
                # Check if there was an error getting encounter notes
                if "error" in encounter_notes:
                    error_msg = encounter_notes["error"]
                    print(f"  Error: {error_msg}")
                    
                    # Only count as failed after all retries
                    if retry_count == max_retries - 1:
                        results["failed_patients"] += 1
                        results["errors"].append({
                            "patient_id": patient_id,
                            "error": error_msg
                        })
                    
                    retry_count += 1
                    continue
                
                # Extract notes data with error handling
                patient_result = extract_notes_data(encounter_notes)
                
                if "error" in patient_result:
                    error_msg = patient_result["error"]
                    print(f"  Error: {error_msg}")
                    
                    # Only count as failed after all retries
                    if retry_count == max_retries - 1:
                        results["failed_patients"] += 1
                        results["errors"].append({
                            "patient_id": patient_id,
                            "error": error_msg
                        })
                    
                    retry_count += 1
                    continue
                
                # If we get here, processing was successful
                success = True
                note_count = len(patient_result["notes"])
                results["successful_patients"] += 1
                results["total_notes_processed"] += note_count
                results["patient_data"][patient_id] = patient_result["notes"]
                print(f"  Successfully processed {note_count} notes for patient {patient_id}")
                
            except Exception as e:
                error_msg = f"Unexpected error processing patient {patient_id}: {str(e)}"
                print(f"  Error: {error_msg}")
                
                # Only count as failed after all retries
                if retry_count == max_retries - 1:
                    results["failed_patients"] += 1
                    results["errors"].append({
                        "patient_id": patient_id,
                        "error": error_msg
                    })
                
                retry_count += 1
                continue
    
    # Print summary
    print("\n=== Processing Summary ===")
    print(f"Successfully processed: {results['successful_patients']} patients")
    print(f"Failed to process: {results['failed_patients']} patients")
    print(f"Total notes collected: {results['total_notes_processed']}")
    
    return results

# #!/usr/bin/env python3
# """
# Adracare Encounter Notes Import Tool with Concurrent Processing

# This script fetches encounter notes concurrently from the Adracare API for multiple patients
# and writes them to a PostgreSQL-compatible SQL file asynchronously.
# """

# import json
# import asyncio
# import aiohttp
# import aiofiles
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# from config.settings import load_config
# from db.database import Database


# async def get_auth_token_async(api_base_url, username, password, session):
#     """
#     Get authentication token from Adracare API asynchronously.
    
#     Args:
#         api_base_url: Base URL for the Adracare API
#         username: Adracare API username 
#         password: Adracare API password
#         session: aiohttp ClientSession
        
#     Returns:
#         str: JWT authentication token
        
#     Raises:
#         Exception: If authentication fails
#     """
#     url = f"{api_base_url}/account_token"
#     payload = {
#         "username": username,
#         "password": password
#     }
    
#     async with session.post(url, json=payload) as response:
#         if response.status not in [200, 201]:
#             raise Exception(f"Authentication failed: {response.status} - {await response.text()}")
        
#         data = await response.json()
#         return data["jwt"]


# async def get_encounter_notes_async(api_base_url, auth_token, patient_id, session):
#     """
#     Get encounter notes for a specific patient from the Adracare API asynchronously.
    
#     Args:
#         api_base_url: Base URL for the Adracare API
#         auth_token: JWT authentication token
#         patient_id: Adracare patient ID
#         session: aiohttp ClientSession
        
#     Returns:
#         dict: JSON response containing encounter notes
        
#     Raises:
#         Exception: If the API request fails
#     """
#     url = f"{api_base_url}/patients/{patient_id}/encounter_notes"
#     headers = {
#         "Authorization": f"Bearer {auth_token}"
#     }
    
#     try:
#         async with session.get(url, headers=headers) as response:
#             if response.status != 200:
#                 error_message = f"Failed to get encounter notes: {response.status} - {await response.text()}"
#                 return {"error": error_message, "data": []}
            
#             return await response.json()
#     except Exception as e:
#         return {"error": f"Exception occurred: {str(e)}", "data": []}


# async def process_patient_async(db, api_base_url, auth_token, patient_id, default_author_id, session):
#     """
#     Process encounter notes for a single patient asynchronously.
    
#     Args:
#         db (Database): Database connection handler
#         api_base_url (str): Adracare API base URL
#         auth_token (str): Authentication token for Adracare API
#         patient_id (str): External patient ID from Adracare
#         default_author_id (int): Default author user ID
#         session: aiohttp ClientSession
        
#     Returns:
#         dict: Results of patient processing
#     """
#     # Initialize result structure
#     patient_result = {
#         "patient_id": patient_id,
#         "messages": [],
#         "notes_found": 0,
#         "processed_notes": [],
#         "success": False
#     }
    
#     msg = f"Fetching encounter notes for patient {patient_id}..."
#     print(msg)
#     patient_result["messages"].append(msg)
    
#     try:
#         encounter_notes_response = await get_encounter_notes_async(
#             api_base_url, auth_token, patient_id, session
#         )
        
#         if "error" in encounter_notes_response:
#             error_msg = f"Error fetching notes for patient {patient_id}: {encounter_notes_response['error']}"
#             print(error_msg)
#             patient_result["messages"].append(error_msg)
#             patient_result["error"] = encounter_notes_response["error"]
#             return patient_result
        
#         notes_data = extract_notes_data(encounter_notes_response)
        
#         msg = f"Found {len(notes_data)} encounter notes for {patient_id}."
#         print(msg)
#         patient_result["messages"].append(msg)
#         patient_result["notes_found"] = len(notes_data)
        
#         if notes_data:
#             local_patient_id = db.get_local_patient_id(patient_id)
#             if not local_patient_id:
#                 error_msg = f"Could not find local patient ID for Adracare patient ID: {patient_id}"
#                 print(error_msg)
#                 patient_result["messages"].append(error_msg)
#                 patient_result["error"] = error_msg
#                 return patient_result
            
#             for note in notes_data:
#                 note["local_patient_id"] = local_patient_id
            
#             patient_result["notes_data"] = notes_data
#             patient_result["local_patient_id"] = local_patient_id
#             patient_result["success"] = True
#         else:
#             no_notes_msg = "No notes to process."
#             print(no_notes_msg)
#             patient_result["messages"].append(no_notes_msg)
#             patient_result["success"] = True
    
#     except Exception as e:
#         error_msg = f"Error processing patient {patient_id}: {str(e)}"
#         print(error_msg)
#         patient_result["messages"].append(error_msg)
#         patient_result["error"] = str(e)
    
#     return patient_result


# async def write_sql_async(db, sql_file, notes_data, default_author_id, results_dict):
#     """
#     Generate and write SQL statements asynchronously.
    
#     Args:
#         db (Database): Database connection handler
#         sql_file (str): Path to the SQL output file
#         notes_data (list): List of note data dictionaries
#         default_author_id (int): Default author user ID
#         results_dict (dict): Results dictionary to update
        
#     Returns:
#         list: Processed records information
#     """
#     processed_records = []
    
#     try:
#         # Generate SQL in a thread pool to avoid blocking the event loop
#         with ThreadPoolExecutor() as executor:
#             loop = asyncio.get_event_loop()
#             sql_statements = await loop.run_in_executor(
#                 executor, 
#                 lambda: [
#                     db.generate_note_sql(
#                         note, 
#                         note["local_patient_id"], 
#                         default_author_id
#                     ) for note in notes_data
#                 ]
#             )
        
#         # Write all SQL statements to the file asynchronously
#         async with aiofiles.open(sql_file, "a") as f:
#             for i, (note, sql_statement) in enumerate(zip(notes_data, sql_statements)):
#                 if sql_statement:
#                     # Add comment with note_id and local_patient_id
#                     comment = f"-- note_id: {note.get('id', 'unknown')}, patient_id: {note['local_patient_id']}\n"
#                     await f.write(comment)
#                     await f.write(sql_statement + "\n\n")
                    
#                     created_at = note.get("created_at")
#                     note_id = note.get("id")
#                     processed_records.append((created_at, note_id))
                    
#                     # Update results dictionary
#                     if note_id:
#                         results_dict["processed_notes"][note_id] = {
#                             "patient_id": note.get("patient_id"),
#                             "local_patient_id": note["local_patient_id"],
#                             "created_at": created_at,
#                             "processed_at": datetime.now().isoformat(),
#                             "sql_generated": True
#                         }
    
#     except Exception as e:
#         print(f"Error writing SQL: {e}")
    
#     return processed_records


# async def main_async():
#     """Main asynchronous execution function for the import script."""
#     # Load configuration
#     config = load_config()
    
#     # Initialize results structure - or load existing one if it exists
#     results_file = "results.json"
#     try:
#         async with aiofiles.open(results_file, "r") as infile:
#             content = await infile.read()
#             results = json.loads(content)
#             results["last_run"] = datetime.now().isoformat()
#     except (FileNotFoundError, json.JSONDecodeError):
#         results = {
#             "first_run": datetime.now().isoformat(),
#             "last_run": datetime.now().isoformat(),
#             "patients": {},
#             "processed_notes": {}
#         }
    
#     # Initialize database connection
#     db = Database(config["db_config"])
    
#     try:
#         if not db.connect():
#             raise Exception("Failed to connect to the database")
        
#         print("Fetching patient IDs from providers...")
#         config = load_config(fetch_patient_ids=True, db=db)
        
#         # Create aiohttp session for all HTTP requests
#         async with aiohttp.ClientSession() as session:
#             print("Getting authentication token...")
#             auth_token = await get_auth_token_async(
#                 config["api_base_url"],
#                 config["username"],
#                 config["password"],
#                 session
#             )
#             print("Authentication successful!")
            
#             # Process all patients concurrently
#             print(f"Processing {len(config['patient_ids'])} patients concurrently...")
#             tasks = []
#             for patient_id in config["patient_ids"]:
#                 # Initialize patient entry in results if needed
#                 if patient_id not in results["patients"]:
#                     results["patients"][patient_id] = []
                
#                 # Create task for each patient
#                 task = process_patient_async(
#                     db, 
#                     config["api_base_url"], 
#                     auth_token, 
#                     patient_id, 
#                     config["default_author_id"],
#                     session
#                 )
#                 tasks.append(task)
            
#             # Wait for all tasks to complete
#             patient_results = await asyncio.gather(*tasks)
            
#             # Filter out failed patients
#             successful_patients = [p for p in patient_results if p.get("success", False)]
#             failed_patients = [p for p in patient_results if not p.get("success", False)]
            
#             print(f"Successfully fetched data for {len(successful_patients)} patients.")
#             print(f"Failed to fetch data for {len(failed_patients)} patients.")
            
#             # Collect all notes to process
#             all_notes = []
#             for patient in successful_patients:
#                 patient_notes = patient.get("notes_data", [])
#                 if patient_notes:
#                     # Add only notes that haven't been processed yet
#                     new_notes = []
#                     for note in patient_notes:
#                         note_id = note.get("id")
#                         if note_id not in results["processed_notes"]:
#                             new_notes.append(note)
#                         else:
#                             print(f"Note {note_id} already processed, skipping.")
                    
#                     all_notes.extend(new_notes)
#                     print(f"Added {len(new_notes)} new notes from patient {patient['patient_id']}.")
            
#             # Write all notes to SQL file asynchronously
#             if all_notes:
#                 print(f"Writing {len(all_notes)} notes to SQL file...")
#                 processed_records = await write_sql_async(
#                     db,
#                     "output.sql",
#                     all_notes,
#                     config["default_author_id"],
#                     results
#                 )
                
#                 print(f"Successfully generated SQL for {len(processed_records)} notes.")
#             else:
#                 print("No new notes to process.")
                
#             # Update patient results in overall results
#             for patient in patient_results:
#                 patient_id = patient["patient_id"]
#                 # Don't overwrite the entire history, just append new results
#                 patient_entry = {
#                     "run_time": datetime.now().isoformat(),
#                     "notes_found": patient.get("notes_found", 0),
#                     "success": patient.get("success", False)
#                 }
#                 if "error" in patient:
#                     patient_entry["error"] = patient["error"]
                
#                 results["patients"][patient_id].append(patient_entry)
    
#     except Exception as e:
#         print(f"Error: {e}")
#         if "errors" not in results:
#             results["errors"] = []
#         results["errors"].append({
#             "timestamp": datetime.now().isoformat(),
#             "error": str(e)
#         })
    
#     finally:
#         db.close()
    
#     # Write results to file
#     async with aiofiles.open(results_file, "w") as outfile:
#         await outfile.write(json.dumps(results, indent=2))
    
#     print(f"\nAll processing complete. See '{results_file}' for detailed logs.")


# def main():
#     """Entry point for script, runs the async main function."""
#     asyncio.run(main_async())


# if __name__ == "__main__":
#     main()