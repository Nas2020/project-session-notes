"""
Functions for interacting with the Adracare API.
"""
import requests


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
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to get encounter notes: {response.status_code} - {response.text}")
    
    return response.json()


def extract_notes_data(encounter_notes_response):
    """
    Extract relevant note data from the API response.
    
    Args:
        encounter_notes_response: JSON response from the Adracare API
        
    Returns:
        list: List of dictionaries containing note data
    """
    notes_data = []
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
    
    return notes_data  # Move this outside the loop