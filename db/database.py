# import psycopg2
# import json
# from utils.text_processing import extract_text_from_html


# class Database:
#     """
#     Database connection and operations handler.
#     """
    
#     def __init__(self, db_config):
#         """
#         Initialize database connection.
        
#         Args:
#             db_config (dict): Database configuration parameters
#         """
#         self.db_config = db_config
#         self.conn = None
    
#     def connect(self):
#         """
#         Create a database connection.
        
#         Returns:
#             bool: True if connection successful, False otherwise
#         """
#         try:
#             self.conn = psycopg2.connect(**self.db_config)
#             return True
#         except Exception as e:
#             print(json.dumps({"error": f"Database connection error: {e}"}))
#             return False
    
#     def close(self):
#         """Close the database connection if open."""
#         if self.conn:
#             self.conn.close()
#             self.conn = None
    
#     def get_local_patient_id(self, external_id):
#         """
#         Find the local patient ID based on the Adracare external ID.
        
#         Args:
#             external_id (str): External patient ID from Adracare
            
#         Returns:
#             int or None: Local patient ID if found, None otherwise
#         """
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute(
#                 "SELECT id FROM patients WHERE external_id = %s", 
#                 (external_id,)
#             )
#             result = cursor.fetchone()
#             if result:
#                 return result[0]
#             return None
#         except Exception as e:
#             print(json.dumps({"error": f"Error finding local patient ID: {e}"}))
#             return None
    
#     def get_local_author_id(self, adracare_account_id):
#         """
#         Find the local user ID based on the Adracare created_by_account_id.
        
#         Args:
#             adracare_account_id (str): Created_by_account_id from Adracare response
            
#         Returns:
#             int or None: Local user ID if found, None otherwise
#         """
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute(
#                 "SELECT id FROM users WHERE adracare_account_id = %s",
#                 (adracare_account_id,)
#             )
#             result = cursor.fetchone()
#             if result:
#                 return result[0]
#             return None
#         except Exception as e:
#             print(json.dumps({"error": f"Error finding local author ID: {e}"}))
#             return None
    
#     def generate_notes_sql(self, notes_data, adracare_patient_id, default_author_id, output_file_path="output.sql", file_mode="w"):
#         """
#         Generate SQL insert statements for notes and write them to an output file.
        
#         Args:
#             notes_data (list): List of note dictionaries from Adracare
#             adracare_patient_id (str): Patient ID from Adracare
#             default_author_id (int): Default author user ID (used only when created_by_account_id is None)
#             output_file_path (str): Path to the output file where SQL statements will be written
#             file_mode (str): File open mode - use "w" to overwrite or "a" to append
                
#         Returns:
#             list: Tuples of (created_at, None) for each processed note
#         """
#         processed_records = []
#         try:
#             # Log the input data for debugging
#             print(json.dumps({"debug": "Notes data received", "notes_data": [str(note) for note in notes_data]}))

#             # Find the local patient ID
#             local_patient_id = self.get_local_patient_id(adracare_patient_id)
            
#             if not local_patient_id:
#                 print(json.dumps({
#                     "warning": f"Could not find local patient ID for Adracare patient ID: {adracare_patient_id}"
#                 }))
#                 return processed_records
            
#             # Open the output file to write SQL statements
#             with open(output_file_path, file_mode) as sql_file:
#                 # Modified SQL to return the ID (not executed, just written to file)
#                 sql_template = """
#                 INSERT INTO patient_notes (notes, patient_id, author_user_id, created_at, updated_at)
#                 VALUES ('{notes}', {patient_id}, {author_id}, '{created_at}' AT TIME ZONE 'UTC', '{updated_at}' AT TIME ZONE 'UTC')
#                 RETURNING id;
#                 """
                
#                 for note in notes_data:
#                     try:
#                         # Access fields directly from the flat dictionary
#                         note_text = extract_text_from_html(note.get("notes", ""))
#                         created_at = note.get("created_at")
#                         updated_at = note.get("updated_at")
#                         adracare_account_id = note.get("created_by_account_id")
                        
#                         # Log the note for debugging
#                         print(json.dumps({"debug": "Processing note", "note": note}))
    
#                         # Handle missing created_by_account_id
#                         if adracare_account_id is None:
#                             print(json.dumps({
#                                 "warning": f"Missing created_by_account_id for note, using default_author_id: {default_author_id}"
#                             }))
#                             author_id = default_author_id
#                         else:
#                             author_id = self.get_local_author_id(adracare_account_id)
#                             if not author_id:
#                                 print(json.dumps({
#                                     "warning": f"No user found for adracare_account_id: {adracare_account_id}"
#                                 }))
#                                 # Skip this note but continue processing others
#                                 continue
    
#                         # Format the SQL statement
#                         sql_statement = sql_template.format(
#                             notes=note_text.replace("'", "''"),  # Escape single quotes
#                             patient_id=local_patient_id,
#                             author_id=author_id,
#                             created_at=created_at,
#                             updated_at=updated_at
#                         )
                        
#                         # Write the SQL statement to the file
#                         sql_file.write(sql_statement + "\n")
                        
#                         # Append the record with a placeholder db_id (since no insertion is done)
#                         processed_records.append((created_at, None))
                    
#                     except Exception as e:
#                         print(json.dumps({"error": f"Error processing note: {e}"}))
#                         # Continue processing other notes
#                         continue
                    
#         except Exception as e:
#             print(json.dumps({"error": f"Error generating SQL statements: {e}"}))
        
#         return processed_records
   
#     def get_patient_ids_by_provider(self, provider_id):
#         """
#         Fetch unique patient IDs associated with a provider from the appointments table.
        
#         Args:
#             provider_id (str): Provider's user ID
            
#         Returns:
#             list: Sorted list of unique patient IDs
#         """
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute(
#                 "SELECT DISTINCT patient_id FROM appointments WHERE user_id = %s ORDER BY patient_id ASC",
#                 (provider_id,)
#             )
#             result = cursor.fetchall()
#             return [row[0] for row in result] if result else []
#         except Exception as e:
#             print(json.dumps({"error": f"Error fetching patient IDs for provider {provider_id}: {e}"}))
#             return []

#     def get_external_id_by_patient_id(self, patient_id):
#         """
#         Get the external ID for a patient.
        
#         Args:
#             patient_id (int): Local patient ID
            
#         Returns:
#             str or None: External patient ID if found, None otherwise
#         """
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute(
#                 "SELECT external_id FROM patients WHERE id = %s",
#                 (patient_id,)
#             )
#             result = cursor.fetchone()
#             return result[0] if result else None
#         except Exception as e:
#             print(json.dumps({"error": f"Error fetching external ID for patient {patient_id}: {e}"}))
#             return None

import psycopg2
import json
from utils.text_processing import extract_text_from_html


class Database:
    """
    Database connection and operations handler.
    """
    
    def __init__(self, db_config):
        """
        Initialize database connection.
        
        Args:
            db_config (dict): Database configuration parameters
        """
        self.db_config = db_config
        self.conn = None
    
    def connect(self):
        """
        Create a database connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.conn = psycopg2.connect(**self.db_config)
            return True
        except Exception as e:
            print(json.dumps({"error": f"Database connection error: {e}"}))
            return False
    
    def close(self):
        """Close the database connection if open."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def get_local_patient_id(self, external_id):
        """
        Find the local patient ID based on the Adracare external ID.
        
        Args:
            external_id (str): External patient ID from Adracare
            
        Returns:
            int or None: Local patient ID if found, None otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT id FROM patients WHERE external_id = %s", 
                (external_id,)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            return None
        except Exception as e:
            print(json.dumps({"error": f"Error finding local patient ID: {e}"}))
            return None
    
    def get_local_author_id(self, adracare_account_id):
        """
        Find the local user ID based on the Adracare created_by_account_id.
        
        Args:
            adracare_account_id (str): Created_by_account_id from Adracare response
            
        Returns:
            int or None: Local user ID if found, None otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT id FROM users WHERE adracare_account_id = %s",
                (adracare_account_id,)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            return None
        except Exception as e:
            print(json.dumps({"error": f"Error finding local author ID: {e}"}))
            return None

    def _format_properly_escaped_sql(self, template, params):
        """
        Use psycopg2 to properly format an SQL statement with parameters.
        
        Args:
            template (str): SQL template with %s placeholders
            params (tuple): Parameters to substitute into the template
            
        Returns:
            str: Properly formatted SQL statement
        """
        try:
            # Use existing connection if available, otherwise create a temporary one
            if self.conn:
                cursor = self.conn.cursor()
            else:
                temp_conn = psycopg2.connect(**self.db_config)
                cursor = temp_conn.cursor()
            formatted_sql = cursor.mogrify(template, params).decode('utf-8')
            return formatted_sql
        except Exception as e:
            print(json.dumps({"error": f"Error formatting SQL: {e}"}))
            # Fallback with manual escaping
            return template.replace("%s", "'{}'").format(*[str(p).replace("'", "''") for p in params])
        finally:
            if not self.conn and 'temp_conn' in locals():
                temp_conn.close()
        
    # def generate_notes_sql(self, notes_data, adracare_patient_id, default_author_id, output_file_path="output.sql", file_mode="w"):
    #     """
    #     Generate SQL insert statements for notes and write them to an output file.
        
    #     Args:
    #         notes_data (list): List of note dictionaries from Adracare
    #         adracare_patient_id (str): Patient ID from Adracare
    #         default_author_id (int): Default author user ID (used only when created_by_account_id is None)
    #         output_file_path (str): Path to the output file where SQL statements will be written
    #         file_mode (str): File open mode - use "w" to overwrite or "a" to append
                
    #     Returns:
    #         list: Tuples of (created_at, None) for each processed note
    #     """
    #     processed_records = []
    #     try:
    #         # Log the input data for debugging
    #         print(json.dumps({"debug": "Notes data received", "notes_data": [str(note) for note in notes_data]}))

    #         # Find the local patient ID
    #         local_patient_id = self.get_local_patient_id(adracare_patient_id)
            
    #         if not local_patient_id:
    #             print(json.dumps({
    #                 "warning": f"Could not find local patient ID for Adracare patient ID: {adracare_patient_id}"
    #             }))
    #             return processed_records
            
    #         # Open the output file to write SQL statements
    #         with open(output_file_path, file_mode) as sql_file:
    #             # SQL template with parameter placeholders
    #             sql_template = """
    #             INSERT INTO patient_notes (notes, patient_id, author_user_id, created_at, updated_at)
    #             VALUES (%s, %s, %s, %s AT TIME ZONE 'UTC', %s AT TIME ZONE 'UTC')
    #             RETURNING id;
    #             """
                
    #             for note in notes_data:
    #                 try:
    #                     # Access fields directly from the flat dictionary
    #                     note_text = extract_text_from_html(note.get("notes", ""))
    #                     created_at = note.get("created_at")
    #                     updated_at = note.get("updated_at")
    #                     adracare_account_id = note.get("created_by_account_id")
                        
    #                     # Log the note for debugging
    #                     print(json.dumps({"debug": "Processing note", "note": note}))
    
    #                     # Handle missing created_by_account_id
    #                     if adracare_account_id is None:
    #                         print(json.dumps({
    #                             "warning": f"Missing created_by_account_id for note, using default_author_id: {default_author_id}"
    #                         }))
    #                         author_id = default_author_id
    #                     else:
    #                         author_id = self.get_local_author_id(adracare_account_id)
    #                         if not author_id:
    #                             print(json.dumps({
    #                                 "warning": f"No user found for adracare_account_id: {adracare_account_id}"
    #                             }))
    #                             # Skip this note but continue processing others
    #                             continue
    
    #                     # Create parameter tuple
    #                     params = (
    #                         note_text,
    #                         local_patient_id,
    #                         author_id,
    #                         created_at,
    #                         updated_at
    #                     )
                        
    #                     # Format the SQL statement using proper escaping
    #                     sql_statement = self._format_properly_escaped_sql(sql_template, params)

    #                     # Check if statement already ends with semicolon
    #                     if sql_statement.strip().endswith(';'):
    #                         sql_file.write(sql_statement + "\n")
    #                     else:
    #                         sql_file.write(sql_statement + ";\n")
                        
    #                     # Append the record with a placeholder db_id (since no insertion is done)
    #                     processed_records.append((created_at, None))
                    
    #                 except Exception as e:
    #                     print(json.dumps({"error": f"Error processing note: {e}"}))
    #                     # Continue processing other notes
    #                     continue
                    
    #     except Exception as e:
    #         print(json.dumps({"error": f"Error generating SQL statements: {e}"}))
        
    #     return processed_records
    
    def generate_notes_sql(self, notes_data, adracare_patient_id, default_author_id, output_file_path="output.sql", file_mode="w", limit=1000):
        """
        Generate SQL insert statements for notes and write them to an output file, up to a specified limit.
        
        Args:
            notes_data (list): List of note dictionaries from Adracare
            adracare_patient_id (str): Patient ID from Adracare
            default_author_id (int): Default author user ID
            output_file_path (str): Path to the output file
            file_mode (str): File open mode ("w" to overwrite, "a" to append)
            limit (int): Maximum number of records to process in this call
            
        Returns:
            list: Tuples of (created_at, None) for each processed note
        """
        processed_records = []
        record_count = 0
        
        try:
            print(json.dumps({"debug": "Notes data received", "notes_data": [str(note) for note in notes_data]}))

            local_patient_id = self.get_local_patient_id(adracare_patient_id)
            if not local_patient_id:
                print(json.dumps({
                    "warning": f"Could not find local patient ID for Adracare patient ID: {adracare_patient_id}"
                }))
                return processed_records
            
            with open(output_file_path, file_mode) as sql_file:
                sql_template = """
                INSERT INTO patient_notes (notes, patient_id, author_user_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s AT TIME ZONE 'UTC', %s AT TIME ZONE 'UTC')
                RETURNING id;
                """
                
                for note in notes_data:
                    if record_count >= limit:
                        print(f"Reached limit of {limit} records for this call, stopping.")
                        break
                    
                    try:
                        note_text = extract_text_from_html(note.get("notes", ""))
                        created_at = note.get("created_at")
                        updated_at = note.get("updated_at")
                        adracare_account_id = note.get("created_by_account_id")
                        
                        print(json.dumps({"debug": "Processing note", "note": note}))
                        
                        if adracare_account_id is None:
                            print(json.dumps({
                                "warning": f"Missing created_by_account_id for note, using default_author_id: {default_author_id}"
                            }))
                            author_id = default_author_id
                        else:
                            author_id = self.get_local_author_id(adracare_account_id)
                            if not author_id:
                                print(json.dumps({
                                    "warning": f"No user found for adracare_account_id: {adracare_account_id}"
                                }))
                                continue
                        
                        params = (note_text, local_patient_id, author_id, created_at, updated_at)
                        sql_statement = self._format_properly_escaped_sql(sql_template, params)
                        
                        # Ensure statement is properly terminated
                        if not sql_statement.strip().endswith(';'):
                            sql_statement += ';'
                        sql_file.write(sql_statement + "\n")
                        
                        processed_records.append((created_at, None))
                        record_count += 1
                        
                    except Exception as e:
                        print(json.dumps({"error": f"Error processing note: {e}"}))
                        continue
                        
        except Exception as e:
            print(json.dumps({"error": f"Error generating SQL statements: {e}"}))
        
        print(f"Generated SQL for {record_count} records in this call.")
        return processed_records
   
    def get_patient_ids_by_provider(self, provider_id):
        """
        Fetch unique patient IDs associated with a provider from the appointments table.
        
        Args:
            provider_id (str): Provider's user ID
            
        Returns:
            list: Sorted list of unique patient IDs
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT DISTINCT patient_id FROM appointments WHERE user_id = %s ORDER BY patient_id ASC",
                (provider_id,)
            )
            result = cursor.fetchall()
            return [row[0] for row in result] if result else []
        except Exception as e:
            print(json.dumps({"error": f"Error fetching patient IDs for provider {provider_id}: {e}"}))
            return []

    def get_external_id_by_patient_id(self, patient_id):
        """
        Get the external ID for a patient.
        
        Args:
            patient_id (int): Local patient ID
            
        Returns:
            str or None: External patient ID if found, None otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT external_id FROM patients WHERE id = %s",
                (patient_id,)
            )
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(json.dumps({"error": f"Error fetching external ID for patient {patient_id}: {e}"}))
            return None