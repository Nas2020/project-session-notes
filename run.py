# #!/usr/bin/env python3
# """
# Adracare Notes Manager

# This script provides an interactive menu to manage Adracare encounter notes.
# """
# import os
# import json
# from datetime import datetime
# import sys

# # Import the main migration script
# from main import main as run_migration
        
# def show_menu():
#     """Display the main menu and handle user input"""
#     while True:
#         print("\n===== Adracare Notes Manager =====")
#         print("1. Start new migration of patient notes")
#         print("2. Re-run migration of patient notes")
#         # print("3. Delete all previously migrated patient notes")
#         print("4. Exit")
        
#         choice = input("\nEnter your choice (1-4): ")
        
#         if choice == "1":
#             # Start new migration (remove existing results)
#             if os.path.exists("results.json"):
#                 backup = f"results_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
#                 os.rename("results.json", backup)
#                 print(f"Previous results backed up to {backup}")
#             run_migration()
            
#         elif choice == "2":
#             # Re-run migration (keep existing results)
#             if not os.path.exists("results.json"):
#                 print("No previous migration results found. Starting fresh migration.")
#             run_migration()
            
#         # elif choice == "3":
#         #     # Delete migrated notes
#         #     delete_migrated_notes()
            
#         elif choice == "4":
#             # Exit
#             print("Exiting. Goodbye!")
#             sys.exit(0)
            
#         else:
#             print("Invalid choice. Please enter a number between 1 and 4.")

# if __name__ == "__main__":
#     show_menu()

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


def show_menu():
    """Display the main menu and handle user input"""
    while True:
        print("\n===== Adracare Notes Manager =====")
        print("1. Start new migration of patient notes")
        print("2. Re-run migration of patient notes")
        print("3. Show providers and patient counts")
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
            # Show provider and patient information
            show_provider_info()
            
        elif choice == "4":
            # Exit
            print("Exiting. Goodbye!")
            sys.exit(0)
            
        else:
            print("Invalid choice. Please enter a number between 1 and 4.")


def show_provider_info():
    """Display information about providers and patient counts"""
    try:
        # Check for providers.json
        if not os.path.exists("providers.json"):
            print("providers.json not found. Please create it first.")
            return
            
        # Display provider IDs
        with open("providers.json", "r") as f:
            providers = json.load(f)
        print(f"\nConfigured providers: {', '.join(providers.get('provider_ids', []))}")
        
        # Display patient information if available
        if os.path.exists("provider-logs.json"):
            with open("provider-logs.json", "r") as f:
                provider_logs = json.load(f)
                
            print("\nProvider Statistics:")
            print("-" * 50)
            print(f"{'Provider ID':<15}{'Patient Count':<15}")
            print("-" * 50)
            
            for provider_id, data in provider_logs.items():
                print(f"{provider_id:<15}{data['patient_count']:<15}")
                
            print("-" * 50)
            
            # Show total if config.json exists
            if os.path.exists("config.json"):
                with open("config.json", "r") as f:
                    config = json.load(f)
                print(f"Total unique patients: {len(config.get('patient_ids', []))}")
        else:
            print("\nNo provider logs found. Run the migration first to generate statistics.")
            
    except Exception as e:
        print(f"Error retrieving provider information: {e}")


if __name__ == "__main__":
    # If command line arguments, use them
    if len(sys.argv) > 1:
        if sys.argv[1] == "start-new":
            # Option 1 logic
            if os.path.exists("results.json"):
                backup = f"results_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
                os.rename("results.json", backup)
                print(f"Previous results backed up to {backup}")
            run_migration()
        elif sys.argv[1] == "re-run":
            # Option 2 logic
            run_migration()
        elif sys.argv[1] == "info":
            # Option 3 logic
            show_provider_info()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Available commands: start-new, re-run, info")
    else:
        # Interactive menu mode
        show_menu()