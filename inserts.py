# import psycopg2
# import re
# import os
# import time
# from datetime import datetime

# def execute_sql_file_in_batches(file_path, batch_size=500, db_config=None):
#     """
#     Execute SQL statements from a file in batches and log errors to a file.
    
#     Args:
#         file_path (str): Path to the SQL file.
#         batch_size (int): Number of SQL statements to execute per batch.
#         db_config (dict): Database connection configuration.
#                           Example: {
#                               "dbname": "your_db",
#                               "user": "your_user",
#                               "password": "your_password",
#                               "host": "localhost",
#                               "port": 5432
#                           }
#     """
#     if db_config is None:
#         raise ValueError("Database configuration (db_config) is required.")
    
#     # Create output directory if it doesn't exist
#     log_dir = "logs"
#     os.makedirs(log_dir, exist_ok=True)
    
#     # Create timestamped log files
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     error_log_path = os.path.join(log_dir, f"error_log_{timestamp}.log")
#     summary_log_path = os.path.join(log_dir, f"summary_{timestamp}.log")
    
#     print(f"Starting execution of {file_path}")
#     start_time = time.time()
    
#     def split_sql_statements(sql_content):
#         statements = []
#         sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        
#         current_statement = ""
#         in_string = False
#         string_delimiter = None
        
#         i = 0
#         while i < len(sql_content):
#             char = sql_content[i]
#             current_statement += char
            
#             if char in ("'", '"') and (i == 0 or sql_content[i-1] != '\\'):
#                 if not in_string:
#                     in_string = True
#                     string_delimiter = char
#                 elif string_delimiter == char:
#                     in_string = False
#                     string_delimiter = None
            
#             elif char == ';' and not in_string:
#                 if i + 1 >= len(sql_content) or sql_content[i+1].isspace():
#                     stmt = current_statement.strip()
#                     if stmt and "INSERT INTO patient_notes" in stmt:
#                         statements.append(stmt)
#                     else:
#                         print(f"Invalid statement skipped: {stmt[:100]}...")
#                     current_statement = ""
            
#             i += 1
        
#         last_stmt = current_statement.strip()
#         if last_stmt and "INSERT INTO patient_notes" in last_stmt:
#             statements.append(last_stmt)
        
#         return statements
    
#     # Read and split the SQL file
#     try:
#         with open(file_path, "r", encoding="utf-8") as sql_file:
#             sql_content = sql_file.read()
#             sql_statements = split_sql_statements(sql_content)
#     except Exception as e:
#         print(f"Error reading SQL file: {e}")
#         return
    
#     total_statements = len(sql_statements)
#     print(f"Found {total_statements} SQL statements to execute")
    
#     # Connect to the database
#     try:
#         conn = psycopg2.connect(**db_config)
#         conn.set_session(autocommit=False)  # Ensure we control transactions
#         cursor = conn.cursor()
#     except Exception as e:
#         print(f"Database connection error: {e}")
#         return
    
#     # Initialize counters
#     successful_statements = 0
#     failed_statements = 0
    
#     # Open the error log file
#     with open(error_log_path, "w", encoding="utf-8") as error_log, \
#          open(summary_log_path, "w", encoding="utf-8") as summary_log:
        
#         summary_log.write(f"Execution Summary for {file_path}\n")
#         summary_log.write(f"Started at: {timestamp}\n")
#         summary_log.write(f"Total statements: {total_statements}\n\n")
        
#         try:
#             # Execute SQL statements in batches
#             batch_count = (total_statements + batch_size - 1) // batch_size  # Ceiling division
            
#             for i in range(0, total_statements, batch_size):
#                 batch_number = i // batch_size + 1
#                 batch_end = min(i + batch_size, total_statements)
#                 batch = sql_statements[i:batch_end]
                
#                 print(f"Executing batch {batch_number} of {batch_count} ({i+1}-{batch_end} of {total_statements} statements)")
#                 batch_start_time = time.time()
                
#                 batch_success = 0
#                 batch_failed = 0
                
#                 # Start a transaction for the batch
#                 try:
#                     for stmt_index, stmt in enumerate(batch):
#                         if not stmt.strip():
#                             continue
                            
#                         try:
#                             cursor.execute(stmt)
#                             batch_success += 1
#                             successful_statements += 1
#                         except Exception as e:
#                             conn.rollback()
#                             # Log the error and the SQL statement to the error log file
#                             error_log.write(f"Error in batch {batch_number}, statement {i + stmt_index + 1}:\n")
#                             # Limit the statement length in the log to avoid huge log files
#                             max_stmt_log_length = 1000
#                             error_log.write(f"Statement: {stmt[:max_stmt_log_length]}{'...' if len(stmt) > max_stmt_log_length else ''}\n")
#                             error_log.write(f"Error: {e}\n\n")
#                             batch_failed += 1
#                             failed_statements += 1
                            
#                     # Commit the batch if there were successful statements
#                     if batch_success > 0:
#                         conn.commit()
#                 except Exception as batch_e:
#                     conn.rollback()
#                     error_log.write(f"Batch {batch_number} failed with error: {batch_e}\n\n")
#                     print(f"Batch {batch_number} failed: {batch_e}")
                
#                 batch_time = time.time() - batch_start_time
#                 print(f"Batch {batch_number} completed in {batch_time:.2f}s - Success: {batch_success}, Failed: {batch_failed}")
                
#                 # Write batch summary
#                 summary_log.write(f"Batch {batch_number}: {batch_success} succeeded, {batch_failed} failed, {batch_time:.2f}s\n")
                
#                 # Periodic flush to ensure logs are written
#                 error_log.flush()
#                 summary_log.flush()
            
#             total_time = time.time() - start_time
#             completion_message = f"Execution completed in {total_time:.2f}s - Total Success: {successful_statements}, Total Failed: {failed_statements}"
#             print(completion_message)
            
#             # Write final summary
#             summary_log.write("\n" + "="*50 + "\n")
#             summary_log.write(f"Execution completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
#             summary_log.write(f"Total time: {total_time:.2f}s\n")
#             summary_log.write(f"Total successful statements: {successful_statements}\n")
#             summary_log.write(f"Total failed statements: {failed_statements}\n")
#             summary_log.write(f"Success rate: {successful_statements/total_statements*100:.2f}%\n")
            
#         except Exception as e:
#             # Log any unexpected errors to the error log file
#             error_message = f"An unexpected error occurred: {e}"
#             error_log.write(error_message + "\n")
#             print(error_message)
#         finally:
#             # Close the database connection
#             cursor.close()
#             conn.close()
#             print(f"Logs saved to {error_log_path} and {summary_log_path}")

# if __name__ == "__main__":
#     # Configurable parameters
#     db_config = {
#         "dbname": "your_database",
#         "user": "your_username",
#         "password": "your_password",
#         "host": "localhost",
#         "port": 5432
#     }
    
#     # Get file path from command line argument or use default
#     import sys
#     file_path = sys.argv[1] if len(sys.argv) > 1 else "output.sql"
    
#     # Execute SQL file in batches
#     execute_sql_file_in_batches(file_path, batch_size=500, db_config=db_config)


import psycopg2
import re
import os
import time
import json
import uuid
from datetime import datetime


class SQLExecutor:
    def __init__(self, db_config=None, log_dir="logs", tracking_file="insert_tracking.json"):
        """
        Initialize the SQL executor with database configuration and tracking setup.
        
        Args:
            db_config (dict): Database connection configuration.
            log_dir (str): Directory for log files.
            tracking_file (str): File to track executed SQL statements.
        """
        self.db_config = db_config or {}
        self.log_dir = log_dir
        self.tracking_file = tracking_file
        self.tracking_data = self._load_tracking_data()
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Create timestamp for log files
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.error_log_path = os.path.join(log_dir, f"error_log_{self.timestamp}.log")
        self.summary_log_path = os.path.join(log_dir, f"summary_{self.timestamp}.log")
    
    def _load_tracking_data(self):
        """Load tracking data from file or create if it doesn't exist."""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"Error reading tracking file. Creating new tracking data.")
                return {"executed_notes": {}}
        return {"executed_notes": {}}
    
    def _save_tracking_data(self):
        """Save tracking data to file."""
        with open(self.tracking_file, 'w') as f:
            json.dump(self.tracking_data, f, indent=2)
    
    def _extract_note_info(self, sql_comment):
        """Extract note_id and patient_id from SQL comment."""
        note_id_match = re.search(r'note_id:\s*([0-9a-f-]+)', sql_comment)
        patient_id_match = re.search(r'patient_id:\s*([0-9a-f-]+)', sql_comment)
        
        note_id = note_id_match.group(1) if note_id_match else None
        patient_id = patient_id_match.group(1) if patient_id_match else None
        
        return note_id, patient_id
    
    def split_sql_statements(self, sql_content):
        """
        Split SQL content into individual statements with their associated comments.
        
        Returns:
            list: List of tuples (comment, statement)
        """
        # Match comments followed by INSERT statements
        pattern = r'(--\s*note_id:[^\n]+)[\s\n]+(INSERT INTO patient_notes[^;]+;)'
        matches = re.findall(pattern, sql_content, re.DOTALL)
        
        statements = []
        for comment, stmt in matches:
            statements.append((comment.strip(), stmt.strip()))
        
        return statements
    
    def execute_statement(self, cursor, stmt):
        """Execute a single SQL statement."""
        cursor.execute(stmt)
        # Get the returned ID from the RETURNING clause
        result = cursor.fetchone()
        return result[0] if result else None
    
    def execute_sql_file(self, file_path, mode='new', batch_size=500):
        """
        Execute SQL statements from a file based on the selected mode.
        
        Args:
            file_path (str): Path to the SQL file.
            mode (str): Execution mode ('new', 're-insert', 'delete', 'empty')
            batch_size (int): Number of SQL statements per batch.
            
        Returns:
            tuple: (successful_count, failed_count, skipped_count)
        """
        print(f"Starting execution of {file_path} in {mode} mode")
        start_time = time.time()
        
        # Read and split the SQL file
        try:
            with open(file_path, "r", encoding="utf-8") as sql_file:
                sql_content = sql_file.read()
                sql_statements = self.split_sql_statements(sql_content)
        except Exception as e:
            print(f"Error reading SQL file: {e}")
            return 0, 0, 0
        
        total_statements = len(sql_statements)
        print(f"Found {total_statements} SQL statements to execute")
        
        # Connect to the database
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.set_session(autocommit=False)  # Ensure we control transactions
            cursor = conn.cursor()
        except Exception as e:
            print(f"Database connection error: {e}")
            return 0, 0, 0
        
        # Initialize counters
        successful_statements = 0
        failed_statements = 0
        skipped_statements = 0
        
        # Open log files
        with open(self.error_log_path, "w", encoding="utf-8") as error_log, \
             open(self.summary_log_path, "w", encoding="utf-8") as summary_log:
            
            summary_log.write(f"Execution Summary for {file_path}\n")
            summary_log.write(f"Mode: {mode}\n")
            summary_log.write(f"Started at: {self.timestamp}\n")
            summary_log.write(f"Total statements: {total_statements}\n\n")
            
            try:
                # Calculate number of batches
                batch_count = (total_statements + batch_size - 1) // batch_size
                
                for i in range(0, total_statements, batch_size):
                    batch_number = i // batch_size + 1
                    batch_end = min(i + batch_size, total_statements)
                    batch = sql_statements[i:batch_end]
                    
                    print(f"Processing batch {batch_number} of {batch_count} ({i+1}-{batch_end} of {total_statements} statements)")
                    batch_start_time = time.time()
                    
                    batch_success = 0
                    batch_failed = 0
                    batch_skipped = 0
                    
                    # Process each statement in the batch
                    for stmt_index, (comment, stmt) in enumerate(batch):
                        # Extract note_id and patient_id from comment
                        note_id, patient_id = self._extract_note_info(comment)
                        
                        if not note_id:
                            error_msg = f"Could not extract note_id from comment: {comment}"
                            error_log.write(f"{error_msg}\n")
                            print(error_msg)
                            batch_failed += 1
                            failed_statements += 1
                            continue
                        
                        # Check if this note has already been executed
                        already_executed = note_id in self.tracking_data["executed_notes"]
                        
                        # Handle based on mode
                        if mode == 'new' and already_executed:
                            summary_log.write(f"Skipped note_id: {note_id} (already executed)\n")
                            batch_skipped += 1
                            skipped_statements += 1
                            continue
                        
                        elif mode == 're-insert' and already_executed:
                            summary_log.write(f"Skipped note_id: {note_id} (already executed)\n")
                            batch_skipped += 1
                            skipped_statements += 1
                            continue
                            
                        elif mode == 'delete' and already_executed:
                            # Delete the record if it exists
                            try:
                                db_id = self.tracking_data["executed_notes"][note_id]["db_id"]
                                delete_sql = f"DELETE FROM patient_notes WHERE id = {db_id};"
                                cursor.execute(delete_sql)
                                conn.commit()
                                
                                # Remove from tracking data
                                del self.tracking_data["executed_notes"][note_id]
                                self._save_tracking_data()
                                
                                batch_success += 1
                                successful_statements += 1
                                summary_log.write(f"Deleted note_id: {note_id} (db_id: {db_id})\n")
                            except Exception as e:
                                conn.rollback()
                                error_log.write(f"Error deleting note_id {note_id}: {e}\n")
                                batch_failed += 1
                                failed_statements += 1
                            continue
                            
                        # Execute the statement (for 'new' mode or 're-insert' for unexecuted statements)
                        if mode in ['new', 're-insert']:
                            try:
                                db_id = self.execute_statement(cursor, stmt)
                                conn.commit()
                                
                                # Record in tracking data
                                self.tracking_data["executed_notes"][note_id] = {
                                    "patient_id": patient_id,
                                    "db_id": db_id,
                                    "executed_at": datetime.now().isoformat(),
                                    "mode": mode
                                }
                                self._save_tracking_data()
                                
                                batch_success += 1
                                successful_statements += 1
                                summary_log.write(f"Executed note_id: {note_id} (db_id: {db_id})\n")
                            except Exception as e:
                                conn.rollback()
                                error_msg = f"Error executing note_id {note_id}: {e}"
                                error_log.write(f"{error_msg}\n")
                                print(error_msg)
                                batch_failed += 1
                                failed_statements += 1
                    
                    batch_time = time.time() - batch_start_time
                    print(f"Batch {batch_number} completed in {batch_time:.2f}s - Success: {batch_success}, Failed: {batch_failed}, Skipped: {batch_skipped}")
                    
                    # Write batch summary
                    summary_log.write(f"Batch {batch_number}: {batch_success} succeeded, {batch_failed} failed, {batch_skipped} skipped, {batch_time:.2f}s\n")
                    
                    # Periodic flush to ensure logs are written
                    error_log.flush()
                    summary_log.flush()
                
                total_time = time.time() - start_time
                completion_message = (
                    f"Execution completed in {total_time:.2f}s - "
                    f"Total Success: {successful_statements}, "
                    f"Total Failed: {failed_statements}, "
                    f"Total Skipped: {skipped_statements}"
                )
                print(completion_message)
                
                # Write final summary
                summary_log.write("\n" + "="*50 + "\n")
                summary_log.write(f"Execution completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                summary_log.write(f"Total time: {total_time:.2f}s\n")
                summary_log.write(f"Total successful statements: {successful_statements}\n")
                summary_log.write(f"Total failed statements: {failed_statements}\n")
                summary_log.write(f"Total skipped statements: {skipped_statements}\n")
                if total_statements > 0:
                    success_rate = successful_statements / (total_statements - skipped_statements) * 100 if (total_statements - skipped_statements) > 0 else 0
                    summary_log.write(f"Success rate: {success_rate:.2f}%\n")
                
            except Exception as e:
                # Log any unexpected errors
                error_message = f"An unexpected error occurred: {e}"
                error_log.write(error_message + "\n")
                print(error_message)
            finally:
                # Close the database connection
                cursor.close()
                conn.close()
                print(f"Logs saved to {self.error_log_path} and {self.summary_log_path}")
        
        return successful_statements, failed_statements, skipped_statements
    
    def generate_stats(self):
        """Generate statistics about tracked executions."""
        if not self.tracking_data["executed_notes"]:
            return "No executions tracked yet."
        
        stats = {
            "total_notes": len(self.tracking_data["executed_notes"]),
            "by_mode": {}
        }
        
        for note_id, data in self.tracking_data["executed_notes"].items():
            mode = data.get("mode", "unknown")
            if mode not in stats["by_mode"]:
                stats["by_mode"][mode] = 0
            stats["by_mode"][mode] += 1
        
        return stats
        
    def empty_table(self):
        """
        Empty the patient_notes table and reset tracking data.
        
        Returns:
            bool: True if successful, False otherwise
        """
        print("Attempting to empty the patient_notes table...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Begin transaction
            cursor.execute("BEGIN;")
            
            # Get count before deletion
            cursor.execute("SELECT COUNT(*) FROM patient_notes;")
            count_before = cursor.fetchone()[0]
            
            # Delete all records
            cursor.execute("DELETE FROM patient_notes;")
            
            # Get count after deletion
            cursor.execute("SELECT COUNT(*) FROM patient_notes;")
            count_after = cursor.fetchone()[0]
            
            # Commit transaction
            conn.commit()
            
            # Clear tracking data
            self.tracking_data = {"executed_notes": {}}
            self._save_tracking_data()
            
            print(f"Successfully emptied patient_notes table. Removed {count_before} records.")
            print("Tracking data has been reset.")
            
            return True
        except Exception as e:
            print(f"Error emptying table: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


def get_db_config():
    """Get database configuration with user input or from environment."""
    config = {}
    
    # Try to load from .env file if it exists
    env_file = '.env'
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    # Check environment variables first
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    
    # If not in environment, prompt user
    if not all([db_name, db_user, db_password, db_host]):
        print("Database configuration not found in environment. Please enter details:")
        config["dbname"] = input("Database name: ") if not db_name else db_name
        config["user"] = input("Database user: ") if not db_user else db_user
        config["password"] = input("Database password: ") if not db_password else db_password
        config["host"] = input("Database host [localhost]: ") or "localhost" if not db_host else db_host
        config["port"] = input("Database port [5432]: ") or "5432" if not db_port else db_port
    else:
        config = {
            "dbname": db_name,
            "user": db_user,
            "password": db_password,
            "host": db_host,
            "port": int(db_port) if db_port else 5432
        }
    
    return config


def display_menu():
    """Display the main menu options."""
    print("\n" + "="*50)
    print("SQL EXECUTION MENU".center(50))
    print("="*50)
    print("1. New Insert (Skip already executed notes)")
    print("2. Re-Insert (Skip already executed notes)")
    print("3. Delete Previous Inserts")
    print("4. Show Execution Statistics")
    print("5. Empty patient_notes Table (Start Clean)")
    print("6. Exit")
    print("="*50)
    choice = input("Enter your choice (1-6): ")
    return choice


def main():
    """Main function to run the SQL executor."""
    # Get database configuration
    db_config = get_db_config()
    
    # Initialize SQL executor
    executor = SQLExecutor(db_config=db_config)
    
    # Get file path from command line argument or use default
    import sys
    default_file = "output.sql"
    file_path = sys.argv[1] if len(sys.argv) > 1 else default_file
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        sys.exit(1)
    
    while True:
        choice = display_menu()
        
        if choice == '1':
            # New Insert
            executor.execute_sql_file(file_path, mode='new')
        elif choice == '2':
            # Re-Insert
            executor.execute_sql_file(file_path, mode='re-insert')
        elif choice == '3':
            # Delete Previous Inserts
            confirmation = input("Are you sure you want to delete previous inserts? (y/n): ")
            if confirmation.lower() == 'y':
                executor.execute_sql_file(file_path, mode='delete')
            else:
                print("Delete operation cancelled.")
        elif choice == '4':
            # Show Statistics
            stats = executor.generate_stats()
            print("\nExecution Statistics:")
            print(json.dumps(stats, indent=2))
        elif choice == '5':
            # Empty table
            confirmation = input("WARNING: This will delete ALL records from the patient_notes table.\nAre you absolutely sure? (type 'YES' to confirm): ")
            if confirmation == 'YES':
                executor.empty_table()
            else:
                print("Empty table operation cancelled.")
        elif choice == '6':
            # Exit
            print("Exiting program. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()