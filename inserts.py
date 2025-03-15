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
    
#     # Function to split SQL properly respecting statements in functions, etc.
#     def split_sql_statements(sql_content):
#         # This regex handles most SQL statements but may need adjustment for specific edge cases
#         # It tries to avoid splitting on semicolons within quotes, comments, or function definitions
#         statements = []
#         # Remove comments first
#         sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        
#         # Simple state machine for statement extraction
#         current_statement = ""
#         in_string = False
#         in_identifier = False
#         string_delimiter = None
        
#         for char in sql_content:
#             current_statement += char
            
#             if char == "'" and not in_identifier:
#                 if not in_string:
#                     in_string = True
#                     string_delimiter = "'"
#                 elif string_delimiter == "'":
#                     in_string = False
#                     string_delimiter = None
            
#             elif char == '"' and not in_string:
#                 if not in_identifier:
#                     in_identifier = True
#                 else:
#                     in_identifier = False
            
#             elif char == ';' and not in_string and not in_identifier:
#                 stmt = current_statement.strip()
#                 if stmt:
#                     statements.append(stmt)
#                 current_statement = ""
        
#         # Add the last statement if not empty
#         last_stmt = current_statement.strip()
#         if last_stmt:
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
from datetime import datetime

def execute_sql_file_in_batches(file_path, batch_size=500, db_config=None):
    """
    Execute SQL statements from a file in batches and log errors to a file.
    
    Args:
        file_path (str): Path to the SQL file.
        batch_size (int): Number of SQL statements to execute per batch.
        db_config (dict): Database connection configuration.
                          Example: {
                              "dbname": "your_db",
                              "user": "your_user",
                              "password": "your_password",
                              "host": "localhost",
                              "port": 5432
                          }
    """
    if db_config is None:
        raise ValueError("Database configuration (db_config) is required.")
    
    # Create output directory if it doesn't exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Create timestamped log files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_log_path = os.path.join(log_dir, f"error_log_{timestamp}.log")
    summary_log_path = os.path.join(log_dir, f"summary_{timestamp}.log")
    
    print(f"Starting execution of {file_path}")
    start_time = time.time()
    
    # Function to split SQL properly respecting statements in functions, etc.
    def split_sql_statements(sql_content):
        statements = []
        # Remove single-line comments
        sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        
        current_statement = ""
        in_string = False
        string_delimiter = None
        
        i = 0
        while i < len(sql_content):
            char = sql_content[i]
            current_statement += char
            
            # Handle string literals
            if char in ("'", '"') and (i == 0 or sql_content[i-1] != '\\'):  # Check for unescaped quotes
                if not in_string:
                    in_string = True
                    string_delimiter = char
                elif string_delimiter == char:
                    in_string = False
                    string_delimiter = None
            
            # Split on semicolon only if not in string and followed by whitespace/newline
            elif char == ';' and not in_string:
                # Peek ahead to ensure it’s a statement end (whitespace, newline, or end of content)
                if i + 1 >= len(sql_content) or sql_content[i+1].isspace():
                    stmt = current_statement.strip()
                    if stmt:
                        statements.append(stmt)
                    current_statement = ""
            
            i += 1
        
        # Add any remaining statement
        last_stmt = current_statement.strip()
        if last_stmt:
            statements.append(last_stmt)
        
        return statements
    
    # Read and split the SQL file
    try:
        with open(file_path, "r", encoding="utf-8") as sql_file:
            sql_content = sql_file.read()
            sql_statements = split_sql_statements(sql_content)
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        return
    
    total_statements = len(sql_statements)
    print(f"Found {total_statements} SQL statements to execute")
    
    # Connect to the database
    try:
        conn = psycopg2.connect(**db_config)
        conn.set_session(autocommit=False)  # Ensure we control transactions
        cursor = conn.cursor()
    except Exception as e:
        print(f"Database connection error: {e}")
        return
    
    # Initialize counters
    successful_statements = 0
    failed_statements = 0
    
    # Open the error log file
    with open(error_log_path, "w", encoding="utf-8") as error_log, \
         open(summary_log_path, "w", encoding="utf-8") as summary_log:
        
        summary_log.write(f"Execution Summary for {file_path}\n")
        summary_log.write(f"Started at: {timestamp}\n")
        summary_log.write(f"Total statements: {total_statements}\n\n")
        
        try:
            # Execute SQL statements in batches
            batch_count = (total_statements + batch_size - 1) // batch_size  # Ceiling division
            
            for i in range(0, total_statements, batch_size):
                batch_number = i // batch_size + 1
                batch_end = min(i + batch_size, total_statements)
                batch = sql_statements[i:batch_end]
                
                print(f"Executing batch {batch_number} of {batch_count} ({i+1}-{batch_end} of {total_statements} statements)")
                batch_start_time = time.time()
                
                batch_success = 0
                batch_failed = 0
                
                # Start a transaction for the batch
                try:
                    for stmt_index, stmt in enumerate(batch):
                        if not stmt.strip():
                            continue
                            
                        try:
                            cursor.execute(stmt)
                            batch_success += 1
                            successful_statements += 1
                        except Exception as e:
                            # Log the error and the SQL statement to the error log file
                            error_log.write(f"Error in batch {batch_number}, statement {i + stmt_index + 1}:\n")
                            # Limit the statement length in the log to avoid huge log files
                            max_stmt_log_length = 1000
                            error_log.write(f"Statement: {stmt[:max_stmt_log_length]}{'...' if len(stmt) > max_stmt_log_length else ''}\n")
                            error_log.write(f"Error: {e}\n\n")
                            batch_failed += 1
                            failed_statements += 1
                            
                    # Commit the batch if there were successful statements
                    if batch_success > 0:
                        conn.commit()
                except Exception as batch_e:
                    conn.rollback()
                    error_log.write(f"Batch {batch_number} failed with error: {batch_e}\n\n")
                    print(f"Batch {batch_number} failed: {batch_e}")
                
                batch_time = time.time() - batch_start_time
                print(f"Batch {batch_number} completed in {batch_time:.2f}s - Success: {batch_success}, Failed: {batch_failed}")
                
                # Write batch summary
                summary_log.write(f"Batch {batch_number}: {batch_success} succeeded, {batch_failed} failed, {batch_time:.2f}s\n")
                
                # Periodic flush to ensure logs are written
                error_log.flush()
                summary_log.flush()
            
            total_time = time.time() - start_time
            completion_message = f"Execution completed in {total_time:.2f}s - Total Success: {successful_statements}, Total Failed: {failed_statements}"
            print(completion_message)
            
            # Write final summary
            summary_log.write("\n" + "="*50 + "\n")
            summary_log.write(f"Execution completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            summary_log.write(f"Total time: {total_time:.2f}s\n")
            summary_log.write(f"Total successful statements: {successful_statements}\n")
            summary_log.write(f"Total failed statements: {failed_statements}\n")
            summary_log.write(f"Success rate: {successful_statements/total_statements*100:.2f}%\n")
            
        except Exception as e:
            # Log any unexpected errors to the error log file
            error_message = f"An unexpected error occurred: {e}"
            error_log.write(error_message + "\n")
            print(error_message)
        finally:
            # Close the database connection
            cursor.close()
            conn.close()
            print(f"Logs saved to {error_log_path} and {summary_log_path}")

if __name__ == "__main__":
    # Configurable parameters
    db_config = {
        "dbname": "your_database",
        "user": "your_username",
        "password": "your_password",
        "host": "localhost",
        "port": 5432
    }
    
    # Get file path from command line argument or use default
    import sys
    file_path = sys.argv[1] if len(sys.argv) > 1 else "output.sql"
    
    # Execute SQL file in batches
    execute_sql_file_in_batches(file_path, batch_size=500, db_config=db_config)