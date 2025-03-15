"""
Configuration and settings management for the Adracare Import tool.
"""
import os
import json
from dotenv import load_dotenv


def load_config(fetch_patient_ids=False, db=None):
    """
    Load environment variables and configuration settings.
    
    Args:
        fetch_patient_ids (bool): If True, fetch patient IDs dynamically from the database
        db (Database): Database instance for fetching patient IDs (required if fetch_patient_ids is True)
        
    Returns:
        dict: Configuration settings
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Base settings from environment variables
    settings = {
        "api_base_url": os.getenv("ADRA_BASE_URL"),
        "username": os.getenv("ADRA_USERNAME"),
        "password": os.getenv("ADRA_PASSWORD"),
        "default_author_id": int(os.getenv("DEFAULT_AUTHOR_ID", "0")),
        "db_config": {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "database": os.getenv("DB_DATABASE", "rocketdoctor_development"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "")
        }
    }
    
    # Read or initialize patient_ids
    if fetch_patient_ids:
        if not db:
            raise ValueError("Database instance required when fetch_patient_ids is True")
        
        # Read provider_ids from providers.json
        try:
            with open("providers.json", "r") as f:
                providers_config = json.load(f)
            provider_ids = providers_config.get("provider_ids", [])
        except Exception as e:
            print(json.dumps({"error": f"Failed to read providers.json: {e}"}))
            provider_ids = []

        # Fetch patient IDs and external IDs
        all_external_ids = []
        provider_logs = {}
        
        for provider_id in provider_ids:
            patient_ids = db.get_patient_ids_by_provider(provider_id)
            provider_logs[provider_id] = {
                "patient_count": len(patient_ids),
                "patient_ids": patient_ids
            }
            print(json.dumps({
                "info": f"Provider {provider_id} has {len(patient_ids)} unique patients"
            }))
            
            for patient_id in patient_ids:
                external_id = db.get_external_id_by_patient_id(patient_id)
                if external_id and external_id not in all_external_ids:
                    all_external_ids.append(external_id)
        
        # Save provider logs
        with open("provider-logs.json", "w") as f:
            json.dump(provider_logs, f, indent=2)
        
        # Update config.json with external IDs
        config = {"patient_ids": all_external_ids}
        with open("config.json", "w") as f:
            json.dump(config, f, indent=2)
        
        settings["patient_ids"] = all_external_ids
    else:
        # Read config.json for patient IDs
        try:
            with open("config.json", "r") as f:
                config = json.load(f)
            settings["patient_ids"] = config.get("patient_ids", [])
        except Exception as e:
            print(json.dumps({"error": f"Failed to read config.json: {e}"}))
            settings["patient_ids"] = []

    return settings