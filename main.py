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
#     Process encounter notes for a single patient.
    
#     Args:
#         db (Database): Database connection handler
#         api_base_url (str): Adracare API base URL
#         auth_token (str): Authentication token for Adracare API
#         patient_id (str): External patient ID from Adracare
#         default_author_id (int): Default author user ID (used only when created_by_account_id is None)
        
#     Returns:
#         dict: Results of patient processing
#     """
#     # Initialize result structure
#     patient_result = {
#         "patient_id": patient_id,
#         "messages": [],
#         "notes_found": 0,
#         "processed_notes": []
#     }
    
#     msg = f"Fetching encounter notes for patient {patient_id}..."
#     print(msg)
#     patient_result["messages"].append(msg)
    
#     try:
#         encounter_notes_response = get_encounter_notes(api_base_url, auth_token, patient_id)
#         notes_data = extract_notes_data(encounter_notes_response)
        
#         msg = f"Found {len(notes_data)} encounter notes for {patient_id}."
#         print(msg)
#         patient_result["messages"].append(msg)
#         patient_result["notes_found"] = len(notes_data)
        
#         if notes_data:
#             sql_gen_msg = "Generating SQL statements for notes..."
#             print(sql_gen_msg)
#             patient_result["messages"].append(sql_gen_msg)
            
#             # Always use append mode for individual patient processing to maintain all notes
#             processed_records = db.generate_notes_sql(notes_data, patient_id, default_author_id, "output.sql", "a")
            
#             for created_at, _ in processed_records:
#                 note_msg = f"Generated SQL for note created on {created_at}"
#                 print(note_msg)
#                 patient_result["messages"].append(note_msg)
            
#             patient_result["processed_notes"] = processed_records
            
#             success_msg = f"Successfully generated SQL for {len(processed_records)} notes."
#             print(success_msg)
#             patient_result["messages"].append(success_msg)
#         else:
#             no_notes_msg = "No notes to process."
#             print(no_notes_msg)
#             patient_result["messages"].append(no_notes_msg)
    
#     except Exception as e:
#         error_msg = f"Error processing patient {patient_id}: {str(e)}"
#         print(error_msg)
#         patient_result["messages"].append(error_msg)
#         patient_result["error"] = str(e)
    
#     return patient_result

# def main():
#     """Main execution function for the import script."""
#     # Load configuration
#     config = load_config()
    
#     # Initialize results structure - or load existing one if it exists
#     results_file = "results.json"
#     try:
#         with open(results_file, "r") as infile:
#             results = json.load(infile)
#             # Add timestamp for this run
#             results["last_run"] = datetime.now().isoformat()
#     except (FileNotFoundError, json.JSONDecodeError):
#         # Create new results if file doesn't exist or is invalid
#         results = {
#             "first_run": datetime.now().isoformat(),
#             "last_run": datetime.now().isoformat(),
#             "patients": {},
#             "processed_notes": {}  # Track by note ID
#         }
    
#     # Initialize database connection
#     db = Database(config["db_config"])
    
#     try:
#         # Connect to database (for lookups only, not for insertions)
#         if not db.connect():
#             raise Exception("Failed to connect to the database")
        
#         # Load configuration with dynamic patient ID fetching
#         print("Fetching patient IDs from providers...")
#         config = load_config(fetch_patient_ids=True, db=db)
        
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
            
#             # Initialize patient in results if not present
#             if patient_id not in results["patients"]:
#                 results["patients"][patient_id] = []
            
#             # Get encounter notes
#             encounter_notes_response = get_encounter_notes(
#                 config["api_base_url"], 
#                 auth_token, 
#                 patient_id
#             )
            
#             notes_data = extract_notes_data(encounter_notes_response)
#             print(f"Found {len(notes_data)} encounter notes for {patient_id}.")
            
#             # Filter out already processed notes
#             new_notes = []
#             for note in notes_data:
#                 note_id = note.get("id")
#                 if note_id not in results["processed_notes"]:
#                     new_notes.append(note)
#                 else:
#                     print(f"Note {note_id} already processed, skipping.")
            
#             print(f"Found {len(new_notes)} new notes to process.")
            
#             # Process new notes
#             if new_notes:
#                 # Use write mode for first patient, append mode for subsequent patients
#                 file_mode = "w" if patient_id == config["patient_ids"][0] else "a"
#                 processed_records = db.generate_notes_sql(
#                     new_notes, 
#                     patient_id, 
#                     config["default_author_id"],
#                     "output.sql",
#                     file_mode,
#                     limit=1000 
#                 )
                
#                 # Record processed notes
#                 for i, note in enumerate(new_notes):
#                     note_id = note.get("id")
#                     if i < len(processed_records):  # Ensure we have records
#                         created_at, _ = processed_records[i]
#                         results["processed_notes"][note_id] = {
#                             "patient_id": patient_id,
#                             "created_at": note.get("created_at"),
#                             "processed_at": datetime.now().isoformat(),
#                             "sql_generated": True
#                         }
#                         # Add to this run's patient results
#                         results["patients"][patient_id].append({
#                             "note_id": note_id,
#                             "created_at": note.get("created_at"),
#                             "processed_at": datetime.now().isoformat(),
#                             "sql_generated": True
#                         })
                
#                 print(f"Successfully generated SQL for {len(processed_records)} notes.")
#             else:
#                 print("No new notes to process.")
    
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
    
#     # Write results to JSON file
#     with open(results_file, "w") as outfile:
#         json.dump(results, outfile, indent=2)
    
#     print(f"\nAll processing complete. See '{results_file}' for detailed logs.")

# if __name__ == "__main__":
#     main()

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
# from api.adracare import extract_notes_data
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


# async def get_encounter_notes_async(api_base_url, auth_token, patient_id, session, timeout=60, max_retries=3, retry_delay=5):
#     """
#     Get encounter notes for a specific patient from the Adracare API asynchronously.
    
#     Args:
#         api_base_url: Base URL for the Adracare API
#         auth_token: JWT authentication token
#         patient_id: Adracare patient ID
#         session: aiohttp ClientSession
#         timeout: Timeout in seconds for the request (default: 60)
#         max_retries: Maximum number of retry attempts (default: 3)
#         retry_delay: Delay in seconds between retries (default: 5)
        
#     Returns:
#         dict: JSON response containing encounter notes
        
#     Raises:
#         Exception: If the API request fails
#     """
#     url = f"{api_base_url}/patients/{patient_id}/encounter_notes"
#     headers = {
#         "Authorization": f"Bearer {auth_token}"
#     }
    
#     for attempt in range(max_retries):
#         try:
#             async with session.get(url, headers=headers, timeout=timeout) as response:
#                 if response.status != 200:
#                     error_message = f"Failed to get encounter notes: {response.status} - {await response.text()}"
#                     print(f"Attempt {attempt+1}/{max_retries} failed: {error_message}")
                    
#                     # If this is not the last attempt, wait and try again
#                     if attempt < max_retries - 1:
#                         print(f"Waiting {retry_delay} seconds before retrying...")
#                         await asyncio.sleep(retry_delay)
#                         continue
                    
#                     return {"error": error_message, "data": []}
                
#                 # Successfully got the response
#                 return await response.json()
                
#         except asyncio.TimeoutError:
#             print(f"Attempt {attempt+1}/{max_retries} timed out after {timeout} seconds")
            
#             # If this is not the last attempt, wait and try again
#             if attempt < max_retries - 1:
#                 print(f"Waiting {retry_delay} seconds before retrying...")
#                 await asyncio.sleep(retry_delay)
#                 continue
            
#             return {"error": f"Request timed out after {max_retries} attempts", "data": []}
            
#         except Exception as e:
#             error_msg = f"Exception occurred: {str(e)}"
#             print(f"Attempt {attempt+1}/{max_retries} failed: {error_msg}")
            
#             # If this is not the last attempt, wait and try again
#             if attempt < max_retries - 1:
#                 print(f"Waiting {retry_delay} seconds before retrying...")
#                 await asyncio.sleep(retry_delay)
#                 continue
            
#             return {"error": error_msg, "data": []}


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
#         # Use the updated function with retry logic and longer timeout
#         encounter_notes_response = await get_encounter_notes_async(
#             api_base_url, auth_token, patient_id, session, 
#             timeout=120, max_retries=3, retry_delay=5
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
        
#         # Configure aiohttp session with proper timeout settings
#         timeout = aiohttp.ClientTimeout(total=120)  # 2 minutes total timeout

#         # Create aiohttp session for all HTTP requests
#         async with aiohttp.ClientSession(timeout=timeout) as session:
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
            
#             # Wait for all tasks to complete with a maximum of 10 concurrent tasks
#             # This helps prevent overloading the server with too many simultaneous requests
#             patient_results = []
#         for i, batch in enumerate([tasks[j:j+10] for j in range(0, len(tasks), 10)]):
#             batch_results = await asyncio.gather(*batch)
#             patient_results.extend(batch_results)
#             # Add a small delay between batches to reduce server load
#             if len(tasks) > 10:
#                 await asyncio.sleep(2)
            
#             # Add this line to show progress
#             print(f"Processed batch {i+1}/{len(tasks)//10 + (1 if len(tasks) % 10 > 0 else 0)}")
            
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
            
#             # Clear the output file for the first write
#             if all_notes and config["patient_ids"]:
#                 # Open in write mode to clear the file
#                 async with aiofiles.open("output.sql", "w") as f:
#                     await f.write("-- Adracare Encounter Notes SQL Import\n")
#                     await f.write(f"-- Generated at: {datetime.now().isoformat()}\n\n")
            
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



import json
import asyncio
import aiohttp
import aiofiles
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from config.settings import load_config
from api.adracare import extract_notes_data


async def get_auth_token_async(api_base_url, username, password, session):
    """
    Get authentication token from Adracare API asynchronously.
    
    Args:
        api_base_url: Base URL for the Adracare API
        username: Adracare API username 
        password: Adracare API password
        session: aiohttp ClientSession
        
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
    
    async with session.post(url, json=payload) as response:
        if response.status not in [200, 201]:
            raise Exception(f"Authentication failed: {response.status} - {await response.text()}")
        
        data = await response.json()
        return data["jwt"]


async def get_encounter_notes_async(api_base_url, auth_token, patient_id, session, timeout=60, max_retries=3, retry_delay=5):
    """
    Get encounter notes for a specific patient from the Adracare API asynchronously.
    
    Args:
        api_base_url: Base URL for the Adracare API
        auth_token: JWT authentication token
        patient_id: Adracare patient ID
        session: aiohttp ClientSession
        timeout: Timeout in seconds for the request (default: 60)
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Delay in seconds between retries (default: 5)
        
    Returns:
        dict: JSON response containing encounter notes
        
    Raises:
        Exception: If the API request fails
    """
    url = f"{api_base_url}/patients/{patient_id}/encounter_notes"
    headers = {
        "Authorization": f"Bearer {auth_token}"
    }
    
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status != 200:
                    error_message = f"Failed to get encounter notes: {response.status} - {await response.text()}"
                    print(f"Attempt {attempt+1}/{max_retries} failed: {error_message}")
                    
                    # If this is not the last attempt, wait and try again
                    if attempt < max_retries - 1:
                        print(f"Waiting {retry_delay} seconds before retrying...")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    return {"error": error_message, "data": []}
                
                # Successfully got the response
                return await response.json()
                
        except asyncio.TimeoutError:
            print(f"Attempt {attempt+1}/{max_retries} timed out after {timeout} seconds")
            
            # If this is not the last attempt, wait and try again
            if attempt < max_retries - 1:
                print(f"Waiting {retry_delay} seconds before retrying...")
                await asyncio.sleep(retry_delay)
                continue
            
            return {"error": f"Request timed out after {max_retries} attempts", "data": []}
            
        except Exception as e:
            error_msg = f"Exception occurred: {str(e)}"
            print(f"Attempt {attempt+1}/{max_retries} failed: {error_msg}")
            
            # If this is not the last attempt, wait and try again
            if attempt < max_retries - 1:
                print(f"Waiting {retry_delay} seconds before retrying...")
                await asyncio.sleep(retry_delay)
                continue
            
            return {"error": error_msg, "data": []}


async def process_patient_async(api_base_url, auth_token, patient_id, default_author_id, session):
    """
    Process encounter notes for a single patient asynchronously.
    
    Args:
        api_base_url (str): Adracare API base URL
        auth_token (str): Authentication token for Adracare API
        patient_id (str): External patient ID from Adracare
        default_author_id (int): Default author user ID
        session: aiohttp ClientSession
        
    Returns:
        dict: Results of patient processing
    """
    # Initialize result structure
    patient_result = {
        "patient_id": patient_id,
        "messages": [],
        "notes_found": 0,
        "processed_notes": [],
        "success": False
    }
    
    msg = f"Fetching encounter notes for patient {patient_id}..."
    print(msg)
    patient_result["messages"].append(msg)
    
    try:
        # Use the updated function with retry logic and longer timeout
        encounter_notes_response = await get_encounter_notes_async(
            api_base_url, auth_token, patient_id, session, 
            timeout=120, max_retries=3, retry_delay=5
        )
        
        if "error" in encounter_notes_response:
            error_msg = f"Error fetching notes for patient {patient_id}: {encounter_notes_response['error']}"
            print(error_msg)
            patient_result["messages"].append(error_msg)
            patient_result["error"] = encounter_notes_response["error"]
            return patient_result
        
        notes_data = extract_notes_data(encounter_notes_response)
        
        msg = f"Found {len(notes_data)} encounter notes for {patient_id}."
        print(msg)
        patient_result["messages"].append(msg)
        patient_result["notes_found"] = len(notes_data)
        
        if notes_data:
            # Instead of looking up local patient ID, just use the external ID directly
            for note in notes_data:
                note["external_patient_id"] = patient_id
            
            patient_result["notes_data"] = notes_data
            patient_result["external_patient_id"] = patient_id
            patient_result["success"] = True
        else:
            no_notes_msg = "No notes to process."
            print(no_notes_msg)
            patient_result["messages"].append(no_notes_msg)
            patient_result["success"] = True
    
    except Exception as e:
        error_msg = f"Error processing patient {patient_id}: {str(e)}"
        print(error_msg)
        patient_result["messages"].append(error_msg)
        patient_result["error"] = str(e)
    
    return patient_result


async def write_sql_async(sql_file, notes_data, default_author_id, results_dict):
    """
    Generate and write SQL statements asynchronously without DB dependencies.
    
    Args:
        sql_file (str): Path to the SQL output file
        notes_data (list): List of note data dictionaries
        default_author_id (int): Default author user ID
        results_dict (dict): Results dictionary to update
        
    Returns:
        list: Processed records information
    """
    processed_records = []
    
    try:
        # Generate SQL in a thread pool to avoid blocking the event loop
        with ThreadPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            sql_statements = await loop.run_in_executor(
                executor, 
                lambda: [
                    generate_note_sql(
                        note, 
                        note["external_patient_id"], 
                        default_author_id
                    ) for note in notes_data
                ]
            )
        
        # Write all SQL statements to the file asynchronously
        async with aiofiles.open(sql_file, "a") as f:
            for i, (note, sql_statement) in enumerate(zip(notes_data, sql_statements)):
                if sql_statement:
                    # Add comment with note_id and external_patient_id
                    comment = f"-- note_id: {note.get('id', 'unknown')}, patient_id: {note['external_patient_id']}\n"
                    await f.write(comment)
                    await f.write(sql_statement + "\n\n")
                    
                    created_at = note.get("created_at")
                    note_id = note.get("id")
                    processed_records.append((created_at, note_id))
                    
                    # Update results dictionary
                    if note_id:
                        results_dict["processed_notes"][note_id] = {
                            "patient_id": note.get("patient_id"),
                            "external_patient_id": note["external_patient_id"],
                            "created_at": created_at,
                            "processed_at": datetime.now().isoformat(),
                            "sql_generated": True
                        }
    
    except Exception as e:
        print(f"Error writing SQL: {e}")
    
    return processed_records


def generate_note_sql(note, external_patient_id, default_author_id):
    """
    Generate SQL for a single note without database connection.
    
    Args:
        note (dict): Note data
        external_patient_id (str): External patient ID from Adracare
        default_author_id (int): Default author user ID
        
    Returns:
        str: SQL statement for the note, or None if there's an error
    """
    try:
        # Since we can't process HTML directly without the dependency, let's treat it as plain text
        # You'll want to add any HTML parsing if necessary
        note_text = note.get("notes", "").replace("'", "''")  # Simple SQL escaping
        created_at = note.get("created_at")
        updated_at = note.get("updated_at")
        
        # Skip notes with missing created_at or updated_at
        if created_at is None or updated_at is None:
            print(f"Skipping note {note.get('id', 'unknown')} due to missing created_at or updated_at")
            return None
        
        adracare_account_id = note.get("created_by_account_id")
        
        # Use the Adracare account ID directly or fall back to default
        author_id = adracare_account_id if adracare_account_id else default_author_id
        
        # Create a SQL statement with a placeholder for local patient ID (to be replaced later)
        sql_template = f"""
        INSERT INTO patient_notes (notes, external_patient_id, author_id, created_at, updated_at)
        VALUES ('{note_text}', '{external_patient_id}', '{author_id}', '{created_at}' AT TIME ZONE 'UTC', '{updated_at}' AT TIME ZONE 'UTC')
        RETURNING id;
        """
        
        return sql_template
            
    except Exception as e:
        print(f"Error generating SQL for note {note.get('id', 'unknown')}: {e}")
        return None


async def main_async():
    """Main asynchronous execution function for the import script."""
    # Load configuration
    config = load_config()
    
    # Initialize results structure - or load existing one if it exists
    results_file = "results.json"
    try:
        async with aiofiles.open(results_file, "r") as infile:
            content = await infile.read()
            results = json.loads(content)
            results["last_run"] = datetime.now().isoformat()
    except (FileNotFoundError, json.JSONDecodeError):
        results = {
            "first_run": datetime.now().isoformat(),
            "last_run": datetime.now().isoformat(),
            "patients": {},
            "processed_notes": {}
        }
    
    try:
        print("Using patient IDs from config...")
        
        # Configure aiohttp session with proper timeout settings
        timeout = aiohttp.ClientTimeout(total=120)  # 2 minutes total timeout

        # Create aiohttp session for all HTTP requests
        async with aiohttp.ClientSession(timeout=timeout) as session:
            print("Getting authentication token...")
            auth_token = await get_auth_token_async(
                config["api_base_url"],
                config["username"],
                config["password"],
                session
            )
            print("Authentication successful!")
            
            # Process all patients concurrently
            print(f"Processing {len(config['patient_ids'])} patients concurrently...")
            tasks = []
            for patient_id in config["patient_ids"]:
                # Initialize patient entry in results if needed
                if patient_id not in results["patients"]:
                    results["patients"][patient_id] = []
                
                # Create task for each patient
                task = process_patient_async(
                    config["api_base_url"], 
                    auth_token, 
                    patient_id, 
                    config["default_author_id"],
                    session
                )
                tasks.append(task)
            
            # Wait for all tasks to complete with a maximum of 10 concurrent tasks
            # This helps prevent overloading the server with too many simultaneous requests
            patient_results = []
            for i, batch in enumerate([tasks[j:j+10] for j in range(0, len(tasks), 10)]):
                batch_results = await asyncio.gather(*batch)
                patient_results.extend(batch_results)
                # Add a small delay between batches to reduce server load
                if len(tasks) > 10:
                    await asyncio.sleep(2)
                
                # Add this line to show progress
                print(f"Processed batch {i+1}/{len(tasks)//10 + (1 if len(tasks) % 10 > 0 else 0)}")
            
            # Filter out failed patients
            successful_patients = [p for p in patient_results if p.get("success", False)]
            failed_patients = [p for p in patient_results if not p.get("success", False)]
            
            print(f"Successfully fetched data for {len(successful_patients)} patients.")
            print(f"Failed to fetch data for {len(failed_patients)} patients.")
            
            # Collect all notes to process
            all_notes = []
            for patient in successful_patients:
                patient_notes = patient.get("notes_data", [])
                if patient_notes:
                    # Add only notes that haven't been processed yet
                    new_notes = []
                    for note in patient_notes:
                        note_id = note.get("id")
                        if note_id not in results["processed_notes"]:
                            new_notes.append(note)
                        else:
                            print(f"Note {note_id} already processed, skipping.")
                    
                    all_notes.extend(new_notes)
                    print(f"Added {len(new_notes)} new notes from patient {patient['patient_id']}.")
            
            # Clear the output file for the first write
            if all_notes and config["patient_ids"]:
                # Open in write mode to clear the file
                async with aiofiles.open("output.sql", "w") as f:
                    await f.write("-- Adracare Encounter Notes SQL Import\n")
                    await f.write(f"-- Generated at: {datetime.now().isoformat()}\n\n")
            
            # Write all notes to SQL file asynchronously
            if all_notes:
                print(f"Writing {len(all_notes)} notes to SQL file...")
                processed_records = await write_sql_async(
                    "output.sql",
                    all_notes,
                    config["default_author_id"],
                    results
                )
                
                print(f"Successfully generated SQL for {len(processed_records)} notes.")
            else:
                print("No new notes to process.")
                
            # Update patient results in overall results
            for patient in patient_results:
                patient_id = patient["patient_id"]
                # Don't overwrite the entire history, just append new results
                patient_entry = {
                    "run_time": datetime.now().isoformat(),
                    "notes_found": patient.get("notes_found", 0),
                    "success": patient.get("success", False)
                }
                if "error" in patient:
                    patient_entry["error"] = patient["error"]
                
                results["patients"][patient_id].append(patient_entry)
    
    except Exception as e:
        print(f"Error: {e}")
        if "errors" not in results:
            results["errors"] = []
        results["errors"].append({
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        })
    
    # Write results to file
    async with aiofiles.open(results_file, "w") as outfile:
        await outfile.write(json.dumps(results, indent=2))
    
    print(f"\nAll processing complete. See '{results_file}' for detailed logs.")


def main():
    """Entry point for script, runs the async main function."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()