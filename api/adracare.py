# """
# Functions for interacting with the Adracare API.
# """
# import requests


# def get_auth_token(api_base_url, username, password):
#     """
#     Get authentication token from Adracare API.
    
#     Args:
#         api_base_url: Base URL for the Adracare API
#         username: Adracare API username 
#         password: Adracare API password
        
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
    
#     response = requests.post(url, json=payload)
#     if response.status_code not in [200, 201]:
#         raise Exception(f"Authentication failed: {response.status_code} - {response.text}")
    
#     data = response.json()
#     return data["jwt"]


# def get_encounter_notes(api_base_url, auth_token, patient_id):
#     """
#     Get encounter notes for a specific patient from the Adracare API.
    
#     Args:
#         api_base_url: Base URL for the Adracare API
#         auth_token: JWT authentication token
#         patient_id: Adracare patient ID
        
#     Returns:
#         dict: JSON response containing encounter notes
        
#     Raises:
#         Exception: If the API request fails
#     """
#     url = f"{api_base_url}/patients/{patient_id}/encounter_notes"
#     headers = {
#         "Authorization": f"Bearer {auth_token}"
#     }
    
#     response = requests.get(url, headers=headers)
#     if response.status_code != 200:
#         raise Exception(f"Failed to get encounter notes: {response.status_code} - {response.text}")
    
#     return response.json()


# def extract_notes_data(encounter_notes_response):
#     """
#     Extract relevant note data from the API response.
    
#     Args:
#         encounter_notes_response: JSON response from the Adracare API
        
#     Returns:
#         list: List of dictionaries containing note data
#     """
#     notes_data = []
#     for item in encounter_notes_response.get("data", []):
#         # Get the note ID
#         note_id = item.get("id", "")
        
#         attributes = item.get("attributes", {})
#         note_data = {
#             "id": note_id,  # Add the note ID
#             "notes": attributes.get("notes", ""),
#             "created_at": attributes.get("created_at"),
#             "updated_at": attributes.get("updated_at"),
#             "patient_id": attributes.get("patient_id"),
#             "created_by_account_id": attributes.get("created_by_account_id")
#         }
#         notes_data.append(note_data)
    
#     return notes_data  # Move this outside the loop

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
    
    # Check if there's an error in the response
    if "error" in encounter_notes_response:
        return {"error": encounter_notes_response["error"], "notes": []}
    
    # Process all notes in the response
    try:
        for item in encounter_notes_response.get("data", []):
            # Get the note ID
            note_id = item.get("id", "")
            
            attributes = item.get("attributes", {})
            note_data = {
                "id": note_id,  # Add the note ID
                "notes": attributes.get("notes", ""),
                "created_at": attributes.get("created_at"),
                "updated_at": attributes.get("updated_at"),
                "patient_id": attributes.get("patient_id"),
                "created_by_account_id": attributes.get("created_by_account_id")
            }
            notes_data.append(note_data)
        
        return {"success": True, "notes": notes_data}
    except Exception as e:
        return {"error": f"Error extracting notes data: {str(e)}", "notes": []}


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