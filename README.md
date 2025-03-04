# Adracare Encounter Notes Import Script

This script fetches encounter notes from the Adracare API for **one or more patients** (defined in `config.json`) and imports those notes into a local PostgreSQL database. It also **generates a JSON log file** (`results.json`) showing which patients were processed, how many notes were found, and the status of each insert operation.

## Table of Contents

1. [Requirements](#requirements)  
2. [Installation](#installation)  
3. [Configuration](#configuration)  
    - [Environment Variables](#environment-variables)  
    - [JSON Config File](#json-config-file)  
4. [Usage](#usage)  
5. [Results JSON File](#results-json-file)  
6. [Notes](#notes)  
7. [Troubleshooting](#troubleshooting)

---

## Requirements

- **Python 3.7+**  
- **PostgreSQL** (local or remote)  
- **pip** (Python package installer)  
- *Optional but recommended:* a **virtual environment** (e.g. `venv` or `virtualenv`)

---

## Installation

1. **Clone or download** this repository to your local machine.

2. **Create and activate a virtual environment** (optional, but recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # or
   venv\Scripts\activate  # On Windows
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   Your `requirements.txt` should include:
   ```
   requests
   beautifulsoup4
   psycopg2-binary
   python-dotenv
   ```

---

## Configuration

### 1) Environment Variables

Use a `.env` file to store your Adracare credentials, API base URL, and database details. **Make sure to add `.env` to your `.gitignore`** so you don’t commit it to version control.

Below is an example `.env` file:

```bash
# Adracare API base URL
ADRA_BASE_URL="https://rdca-api.adracare.com/apis/v2"

# Adracare credentials
ADRA_USERNAME="YOUR_ADRA_USERNAME"
ADRA_PASSWORD="YOUR_ADRA_PASSWORD"

# PostgreSQL Database
DB_HOST="localhost"
DB_PORT="5432"
DB_DATABASE="rocketdoctor_development"
DB_USER="postgres"
DB_PASSWORD=""
```

### 2) JSON Config File

Create a file named `config.json` in your project’s root directory to specify **one or more** patient IDs. For example:

```json
{
  "patient_ids": [
    "0b8db062-78b6-4cd0-b36b-65388de4458f"
  ]
}
```

If you have **multiple** patients, simply include them all in the `patient_ids` array:

```json
{
  "patient_ids": [
    "0b8db062-78b6-4cd0-b36b-65388de4458f",
    "6a1f0123-86d1-4cdf-a87d-abcdefghijk"
  ]
}
```

---

## Usage

1. **Load your environment variables** from `.env`:
   - This is handled automatically when you run the script (via `python-dotenv`).

2. **Ensure `config.json` is in the same directory** as your script (`import_notes.py`).  

3. **Run the script**:
   ```bash
   python import_notes.py
   ```
   - The script will:
     1. **Read** patient IDs from `config.json`.
     2. **Authenticate** with the Adracare API using your credentials from `.env`.
     3. **Loop** through each patient ID, fetch their encounter notes, and **insert** them into your PostgreSQL database.
     4. **Accumulate** log messages in memory and **write** a detailed report to `results.json` at the end.

---

## Results JSON File

When the script finishes, it creates (or overwrites) a file named **`results.json`**. It contains:

- A **timestamp** for when the script ran.
- An array of **patients**, each with:
  - **patient_id**: The Adracare patient ID.
  - **messages**: A list of console-like messages describing what happened for that patient.
  - **notes_found**: Number of notes retrieved from the Adracare API for this patient.
  - **inserted_notes**: An array of timestamps (`created_at`) for each note successfully inserted into the database.

An example `results.json` might look like this:

```json
{
  "timestamp": "2025-03-04T12:34:56.789012",
  "patients": [
    {
      "patient_id": "0b8db062-78b6-4cd0-b36b-65388de4458f",
      "messages": [
        "Fetching encounter notes for patient 0b8db062-78b6-4cd0-b36b-65388de4458f...",
        "Found 4 encounter notes for 0b8db062-78b6-4cd0-b36b-65388de4458f.",
        "Inserting notes into local database...",
        "Inserted note created on 2023-05-01T20:40:41.649-06:00",
        "Inserted note created on 2023-01-17T12:52:44.982-07:00",
        "Inserted note created on 2022-03-29T12:11:22.062-06:00",
        "Inserted note created on 2022-03-28T12:27:53.944-06:00",
        "Successfully inserted 4 notes into the database."
      ],
      "notes_found": 4,
      "inserted_notes": [
        "2023-05-01T20:40:41.649-06:00",
        "2023-01-17T12:52:44.982-07:00",
        "2022-03-29T12:11:22.062-06:00",
        "2022-03-28T12:27:53.944-06:00"
      ]
    }
  ]
}
```

This provides a **machine-readable** record of what happened during the run, along with a **human-friendly** list of messages.

---

## Notes

- **Multiple Patients**: If you add more patient IDs to `config.json`, the script will process each one in turn and log them separately in `results.json`.
- **Duplicate Handling**:  
  - This script **does not** handle duplicates. Running it repeatedly may cause repeated inserts.  
  - If you need to prevent duplicates or handle note updates, you should store a unique identifier (like an encounter ID from the Adracare API) in your local DB and either skip or update existing entries.
- **HTML to Plain Text**:  
  - The script uses `BeautifulSoup` to strip HTML tags from the Adracare notes.  
  - If you need the original HTML format, consider modifying the `extract_text_from_html` function to store raw HTML in a separate column.
