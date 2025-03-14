#!/usr/bin/env python3
import json
import psycopg2
import os
from datetime import datetime

def fetch_alberta_providers(db_config=None):
    """
    Fetches all provider IDs from users who have an ab_prac_id (Alberta practitioners)
    and updates the providers.json file.
    
    Args:
        db_config (dict): Database connection configuration.
    """
    if db_config is None:
        # Default configuration - update with your actual credentials
        db_config = {
            "dbname": "your_database",
            "user": "your_username",
            "password": "your_password",
            "host": "localhost",
            "port": 5432
        }
    
    # Connect to the database
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Connected to the database successfully")
    except Exception as e:
        print(f"Database connection error: {e}")
        return
    
    try:
        # Query to fetch all user IDs with non-null ab_prac_id
        query = """
        SELECT id FROM users 
        WHERE ab_prac_id IS NOT NULL 
        AND ab_prac_id != ''
        ORDER BY id
        """
        
        cursor.execute(query)
        provider_ids = [str(row[0]) for row in cursor.fetchall()]
        
        # Create a backup of the existing providers.json if it exists
        providers_file = "providers.json"
        if os.path.exists(providers_file):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_file = f"providers_backup_{timestamp}.json"
            try:
                with open(providers_file, 'r') as src, open(backup_file, 'w') as dst:
                    dst.write(src.read())
                print(f"Backup created: {backup_file}")
            except Exception as e:
                print(f"Warning: Could not create backup: {e}")
        
        # Write the new provider_ids to providers.json
        provider_data = {"provider_ids": provider_ids}
        with open(providers_file, 'w') as f:
            json.dump(provider_data, f, indent=2)
        
        print(f"Successfully fetched {len(provider_ids)} Alberta providers")
        print(f"Updated {providers_file}")
        
        # Optional: Show sample of providers
        sample_size = min(10, len(provider_ids))
        if sample_size > 0:
            print(f"\nSample of first {sample_size} provider IDs:")
            for i in range(sample_size):
                print(f"  - {provider_ids[i]}")
        
        # Optional: Show count comparison with previous file
        try:
            with open(backup_file, 'r') as f:
                old_data = json.load(f)
                old_count = len(old_data.get("provider_ids", []))
                diff = len(provider_ids) - old_count
                print(f"\nPrevious provider count: {old_count}")
                print(f"New provider count: {len(provider_ids)}")
                print(f"Difference: {diff:+d} providers")
        except:
            pass
            
    except Exception as e:
        print(f"Error fetching Alberta providers: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed")

def get_provider_details(db_config=None, limit=10):
    """
    Fetches detailed information about Alberta providers.
    
    Args:
        db_config (dict): Database connection configuration.
        limit (int): Maximum number of providers to fetch details for.
    """
    if db_config is None:
        # Default configuration
        db_config = {
            "dbname": "your_database",
            "user": "your_username",
            "password": "your_password",
            "host": "localhost",
            "port": 5432
        }
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        query = """
        SELECT id, email, first_name, last_name, ab_prac_id, active, role, country 
        FROM users 
        WHERE ab_prac_id IS NOT NULL 
        AND ab_prac_id != '' 
        AND active = true
        ORDER BY id
        LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        providers = cursor.fetchall()
        
        print(f"\n===== Details for {len(providers)} Alberta providers =====")
        print(f"{'ID':<8} {'Name':<30} {'AB Practice ID':<15} {'Role':<15} {'Country':<10}")
        print("-" * 80)
        
        for provider in providers:
            id, email, first_name, last_name, ab_prac_id, active, role, country = provider
            name = f"{first_name or ''} {last_name or ''}".strip()
            print(f"{id:<8} {name:<30} {ab_prac_id:<15} {role or 'N/A':<15} {country or 'N/A':<10}")
        
    except Exception as e:
        print(f"Error getting provider details: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Database configuration - update with your actual database credentials
    db_config = {
        "dbname": "your_database",
        "user": "your_username",
        "password": "your_password",
        "host": "localhost", 
        "port": 5432
    }
    
    # Fetch Alberta providers and update providers.json
    fetch_alberta_providers(db_config)
    
    # Optionally show detailed information about some providers
    get_provider_details(db_config, limit=5)
    
    print("\nDone!")