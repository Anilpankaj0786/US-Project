import os
import shutil
import requests
import zipfile
from pathlib import Path

# S3 utility functions import kar rahe hain
from s3_utils import create_bucket_if_not_exists, upload_file_to_s3

# BTS website ka base URL (yahin se data download hoga)
BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"

# S3 bucket ka naam
BUCKET_NAME = "airlines-bucket-07860"

# Local folders
RAW_FOLDER = "./data/raw"              # ZIP files yahin save hongi
PROCESSED_FOLDER = "./data/processed"  # Extracted CSV files yahin hongi

# Folders exist nahi karte toh create kar do
Path(RAW_FOLDER).mkdir(parents=True, exist_ok=True)
Path(PROCESSED_FOLDER).mkdir(parents=True, exist_ok=True)

# Kaunse years ka data download karna hai
YEARS_TO_DOWNLOAD = [2021, 2022]


# Step 1: ZIP file download karna
def download_one_month(year, month):

    # File ka naam dynamically ban raha hai
    file_name = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

    # Full download URL
    file_url = BTS_WEBSITE + file_name

    # Local system me ZIP file ka path
    save_path = os.path.join(RAW_FOLDER, file_name)

    # Agar ZIP file pehle se exist karti hai toh skip karo
    if os.path.exists(save_path):
        print(f" Already exists (skip download): {file_name}")
        return save_path

    print(f" Download start: {file_name}")

    try:
        # Server se request bhej rahe hain (stream=True = chunk me data aayega)
        response = requests.get(file_url, stream=True)

        # Agar file available nahi hai server pe toh skip
        if response.status_code != 200:
            print(f" File nahi mili server pe: {file_name}")
            return None

        # File ko write mode me open karke chunks me save karo
        with open(save_path, "wb") as file:
            # 1 MB ke chunks me data download ho raha hai (fast + efficient)
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

        print(f" Download complete: {file_name}")
        return save_path

    except Exception as e:
        print(f" Download error {file_name}: {e}")
        return None


# Step 2: ZIP file ko locally extract karna
def extract_zip(zip_path, year, month):

    # Extracted files ka folder (partitioned format me)
    extract_path = os.path.join(PROCESSED_FOLDER, f"{year}_{month}")
    os.makedirs(extract_path, exist_ok=True)

    print(f" Extract ho raha hai: {os.path.basename(zip_path)}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        print(f" Extract complete: {year}_{month}")
        return extract_path

    except Exception as e:
        print(f" Extract error: {e}")
        return None


# Step 3: Extracted CSV files ko S3 par partitioned format me upload karna
# S3 structure: download/year=2021/month=1/filename.csv
def upload_to_s3(extract_path, year, month):

    all_uploaded = True  # Track karo ki saari files upload hui ya nahi

    # Extracted folder ke andar saari CSV files pe loop
    for file in os.listdir(extract_path):

        if file.endswith(".csv"):

            full_path = os.path.join(extract_path, file)

            # S3 me partitioned structure: download/year=2021/month=1/file.csv
            s3_key = f"download/year={year}/month={month}/{file}"

            print(f" Upload ho raha hai: {s3_key}")

            try:
                upload_file_to_s3(full_path, BUCKET_NAME, s3_key)
                print(f" Upload complete: {s3_key}")

            except Exception as e:
                print(f" Upload fail hua: {s3_key} → {e}")
                all_uploaded = False  # Koi bhi file fail hui toh flag set karo

    return all_uploaded  # True = sab upload hua, False = kuch fail hua


# Step 4: Local files cleanup karna (sirf tab jab upload successful ho)
def cleanup_local(zip_path, extract_path):

    try:
        # ZIP file delete karo
        if os.path.exists(zip_path):
            os.remove(zip_path)
            print(f" ZIP delete ho gayi: {zip_path}")

        # Extracted folder delete karo (poora folder)
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
            print(f" Extracted folder delete ho gaya: {extract_path}")

    except Exception as e:
        print(f" Cleanup error: {e}")


# Main program yahin se start hota hai
if __name__ == "__main__":

    # Step 0: Bucket exist karti hai ya nahi check karo, nahi hai toh create karo
    print("\n === Step 0: Bucket check/create ===")
    create_bucket_if_not_exists(BUCKET_NAME)

    # Har year aur month ke liye loop
    for year in YEARS_TO_DOWNLOAD:
        for month in range(1, 13):  # 1 se 12 tak

            print(f"\n ====== Year: {year}, Month: {month} ======")

            # Step 1: ZIP file download karo
            print(f"\n --- Step 1: Download ---")
            zip_path = download_one_month(year, month)

            # Agar download fail hua toh is month ko skip karo
            if zip_path is None:
                print(f" Skipping {year}-{month} (download fail hua)")
                continue

            # Step 2: ZIP extract karo locally
            print(f"\n --- Step 2: Extract ---")
            extract_path = extract_zip(zip_path, year, month)

            # Agar extract fail hua toh is month ko skip karo
            if extract_path is None:
                print(f" Skipping {year}-{month} (extract fail hua)")
                continue

            # Step 3: Extracted CSV files S3 par upload karo
            print(f"\n --- Step 3: S3 Upload ---")
            upload_success = upload_to_s3(extract_path, year, month)

            # Step 4: Sirf tab cleanup karo jab saari files successfully upload ho gayi hon
            if upload_success:
                print(f"\n --- Step 4: Cleanup (upload successful tha) ---")
                cleanup_local(zip_path, extract_path)
            else:
                # Upload fail hua toh local files rakho — dobara run pe retry ho sakta hai
                print(f"\n Upload fail hua {year}-{month} ke liye → Local files rakhi hain, dobara run karo!")

    print("\n Saara process complete!")