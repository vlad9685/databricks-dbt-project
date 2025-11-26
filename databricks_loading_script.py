import os
from databricks.sdk import WorkspaceClient

# Configuration
DATABRICKS_HOST = "YOUR_DATABRICKS_HOST"
DATABRICKS_TOKEN = "YOUR_DATABRICKS_TOKEN"
LOCAL_RAW_DATA_DIR = "raw_data"
VOLUME_BRONZE_PATH = "/Volumes/workspace/default/bronze"

print("Configuration loaded. Using the official Databricks SDK.")

def upload_files_to_databricks_sdk():
    """
    Connects to Databricks using the official SDK and uploads files to a Unity Catalog Volume.
    """
    # 1. Credential Check
    if "YOUR_DATABRICKS_HOST" in DATABRICKS_HOST or "YOUR_DATABRICKS_TOKEN" in DATABRICKS_TOKEN:
        print("ERROR: Please update the DATABRICKS_HOST and DATABRICKS_TOKEN variables with your credentials.")
        return

    print(f"Initializing Databricks Workspace Client for host: {DATABRICKS_HOST}...")
    try:
        # 2. API Connection using SDK
        w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        print(" -> Client initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Databricks Workspace Client: {e}")
        return

    # 3. Find Local Files
    print(f"Listing local files from '{LOCAL_RAW_DATA_DIR}'...")
    try:
        local_files = os.listdir(LOCAL_RAW_DATA_DIR)
        if not local_files:
            print(f"No files found in '{LOCAL_RAW_DATA_DIR}'. Please run the ingestion script first.")
            return
    except FileNotFoundError:
        print(f"ERROR: Local directory '{LOCAL_RAW_DATA_DIR}' not found. Did you run the ingestion script?")
        return

    # 4. Upload Files
    for file_name in local_files:
        local_file_path = os.path.join(LOCAL_RAW_DATA_DIR, file_name)
        # Construct the full target path within the Volume
        volume_target_path = f"{VOLUME_BRONZE_PATH}/{file_name}"
        
        print(f"Uploading '{local_file_path}' to '{volume_target_path}'...")
        try:
            with open(local_file_path, "rb") as f:
                w.files.upload(file_path=volume_target_path, contents=f, overwrite=True)
            print(f" -> Successfully uploaded {file_name}.")
        except Exception as e:
            print(f" -> FAILED to upload {file_name}. Error: {e}")


if __name__ == "__main__":
    upload_files_to_databricks_sdk()
    print("\nUpload process complete.")
