#!/usr/bin/env python3
"""
Adracare Notes Manager

This script provides an interactive menu to manage Adracare encounter notes.
"""
import os
import json
from datetime import datetime
import sys

# Import the main migration script
from main import main as run_migration
from config.settings import load_config
from db.database import Database

def delete_migrated_notes():
    """Delete all previously migrated notes based on results.json"""
    results_file = "results.json"
    
    # Check if results file exists
    if not os.path.exists(results_file):
        print("No previous migration results found.")
        return
    
    # Confirm deletion
    confirmation = input("This will delete ALL previously migrated notes. Type 'DELETE' to confirm: ")
    if confirmation != "DELETE":
        print("Deletion cancelled.")
        return
    
    # Load results.json
    with open(results_file, "r") as infile:
        results = json.load(infile)
    
    if not results.get("imported_notes"):
        print("No imported notes found in results file.")
        return
    
    # Load database configuration
    config = load_config()
    db = Database(config["db_config"])
    
    if not db.connect():
        print("Failed to connect to database")
        return
    
    try:
        cursor = db.conn.cursor()
        deleted_count = 0
        skipped_count = 0
        
        print("Deleting notes...")
        
        # Loop through imported notes and delete them
        for note_id, note_info in results["imported_notes"].items():
            # Use the stored database ID if available
            if "db_id" in note_info:
                cursor.execute(
                    "DELETE FROM patient_notes WHERE id = %s",
                    (note_info["db_id"],)
                )
                rows_deleted = cursor.rowcount
                deleted_count += rows_deleted
                
                if rows_deleted == 0:
                    print(f"Note with DB ID {note_info['db_id']} not found, may have been already deleted.")
            else:
                # Fall back to the less precise method if db_id not available
                patient_id = note_info["patient_id"]
                created_at = note_info["created_at"]
                
                local_patient_id = db.get_local_patient_id(patient_id)
                if not local_patient_id:
                    print(f"Could not find local patient ID for {patient_id}, skipping")
                    skipped_count += 1
                    continue
                    
                cursor.execute(
                    "DELETE FROM patient_notes WHERE patient_id = %s AND created_at::text LIKE %s",
                    (local_patient_id, f"%{created_at.split('T')[0]}%")
                )
                deleted_count += cursor.rowcount
        
        db.conn.commit()
        
        # Create a backup of the original results
        backup_file = f"results_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        with open(backup_file, "w") as outfile:
            json.dump(results, outfile, indent=2)
            
        # Clear the imported notes in the results
        results["imported_notes"] = {}
        results["patients"] = {}
        results["last_deletion"] = datetime.now().isoformat()
        
        # Update results file
        with open(results_file, "w") as outfile:
            json.dump(results, outfile, indent=2)
            
        print(f"Successfully deleted {deleted_count} notes. Skipped {skipped_count} notes.")
        print(f"Previous results backed up to {backup_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        db.conn.rollback()
    
    finally:
        db.close()
        
def show_menu():
    """Display the main menu and handle user input"""
    while True:
        print("\n===== Adracare Notes Manager =====")
        print("1. Start new migration of patient notes")
        print("2. Re-run migration of patient notes")
        print("3. Delete all previously migrated patient notes")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ")
        
        if choice == "1":
            # Start new migration (remove existing results)
            if os.path.exists("results.json"):
                backup = f"results_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
                os.rename("results.json", backup)
                print(f"Previous results backed up to {backup}")
            run_migration()
            
        elif choice == "2":
            # Re-run migration (keep existing results)
            if not os.path.exists("results.json"):
                print("No previous migration results found. Starting fresh migration.")
            run_migration()
            
        elif choice == "3":
            # Delete migrated notes
            delete_migrated_notes()
            
        elif choice == "4":
            # Exit
            print("Exiting. Goodbye!")
            sys.exit(0)
            
        else:
            print("Invalid choice. Please enter a number between 1 and 4.")

if __name__ == "__main__":
    show_menu()