# import os
# import zipfile

# #  Apne custom functions import kar rahe hain (S3 ke liye)
# from s3_utils import create_bucket_if_not_exists, upload_file_to_s3


# #  Raw (zip files) aur processed (extracted files) ke folders
# RAW_FOLDER = "./data/raw"
# PROCESSED_FOLDER = "./data/processed"

# #  S3 bucket ka naam
# BUCKET_NAME = "airlines-bucket-0786"


# #  Step 1: ZIP files ko extract karna

# def extract_files():

#     #  Processed folder create kar do agar exist nahi karta
#     os.makedirs(PROCESSED_FOLDER, exist_ok=True)

#     #  Raw folder ke andar jitni bhi files hain unpe loop
#     for file in os.listdir(RAW_FOLDER):

#         #  Sirf zip files process karni hain
#         if file.endswith(".zip"):

#             #  Zip file ka full path
#             zip_path = os.path.join(RAW_FOLDER, file)

#             #  File name se year aur month extract kar rahe hain
#             parts = file.split("_")
#             year = parts[-2]
#             month = parts[-1].replace(".zip", "")

#             #  Extract hone wali files ka folder (partition wise)
#             extract_path = os.path.join(PROCESSED_FOLDER, f"{year}_{month}")
#             os.makedirs(extract_path, exist_ok=True)

#             print(f" Extract ho raha hai: {file}")

#             #  Zip file ko open karke extract kar rahe hain
#             with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#                 zip_ref.extractall(extract_path)

#             print(f" Extract complete: {year}_{month}")



# #  Step 2: Extracted CSV files ko S3 par upload karna

# def upload_partitioned_data():

#     #  Processed folder ke andar recursively loop (subfolders bhi cover honge)
#     for root, dirs, files in os.walk(PROCESSED_FOLDER):

#         for file in files:

#             #  Sirf CSV files upload karni hain
#             if file.endswith(".csv"):

#                 #  Local file ka full path
#                 full_path = os.path.join(root, file)

#                 #  Folder name se year aur month nikal rahe hain
#                 folder_name = os.path.basename(root)
#                 year, month = folder_name.split("_")

#                 #  S3 me partitioned structure bana rahe hain
#                 #    Example: year=2021/month=1/file.csv
#                 s3_key = f"year={year}/month={month}/{file}"

#                 print(f" Upload ho raha hai: {s3_key}")

#                 #  File upload function call
#                 upload_file_to_s3(full_path, BUCKET_NAME, s3_key)

#                 print(f" Upload complete: {s3_key}")



# # Main execution yahin se start hota hai

# if __name__ == "__main__":

#     #  Step 0: Bucket create/check
#     create_bucket_if_not_exists(BUCKET_NAME)

#     #  Step 1: ZIP extract karo
#     extract_files()

#     #  Step 2: S3 par upload karo (partitioned format me)
#     upload_partitioned_data()
    
    
    
    
import os
import zipfile

from s3_utils import create_bucket_if_not_exists, upload_file_to_s3

RAW_FOLDER = "./data/raw"
PROCESSED_FOLDER = "./data/processed"
BUCKET_NAME = "airlines-bucket-0786"


def extract_files():

    os.makedirs(PROCESSED_FOLDER, exist_ok=True)

    for file in os.listdir(RAW_FOLDER):

        if file.endswith(".zip"):

            zip_path = os.path.join(RAW_FOLDER, file)

            parts = file.split("_")
            year = parts[-2]
            month = parts[-1].replace(".zip", "")

            extract_path = os.path.join(PROCESSED_FOLDER, f"{year}_{month}")
            os.makedirs(extract_path, exist_ok=True)

            print(f" Extract ho raha hai: {file}")

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)

            print(f" Extract complete: {year}_{month}")


def upload_partitioned_data():

    for root, dirs, files in os.walk(PROCESSED_FOLDER):

        for file in files:

            if file.endswith(".csv"):

                full_path = os.path.join(root, file)

                folder_name = os.path.basename(root)
                year, month = folder_name.split("_")

                s3_key = f"year={year}/month={month}/{file}"

                print(f" Upload ho raha hai: {s3_key}")

                upload_file_to_s3(full_path, BUCKET_NAME, s3_key)

                print(f" Upload complete: {s3_key}")


if __name__ == "__main__":

    create_bucket_if_not_exists(BUCKET_NAME)
    extract_files()
    upload_partitioned_data()