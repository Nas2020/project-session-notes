"""
Configuration and settings management for the Adracare Import tool.
"""
import os
import json
from dotenv import load_dotenv


def load_config():
    """Load environment variables and configuration settings."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Read config.json for patient IDs
    with open("config.json", "r") as f:
        config = json.load(f)
    
    # Extract configuration values
    settings = {
        "patient_ids": config.get("patient_ids", []),
        "api_base_url": os.getenv("ADRA_BASE_URL"),
        "username": os.getenv("ADRA_USERNAME"),
        "password": os.getenv("ADRA_PASSWORD"),
        "default_author_id": os.getenv("DEFAULT_AUTHOR_ID", 1),
        "db_config": {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", 5432),
            "database": os.getenv("DB_DATABASE", "rocketdoctor_development"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "")
        }
    }
    
    return settings