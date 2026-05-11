# import os
# from pathlib import Path

# from webob import year

# BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"

# LOCAL_SAVE_FOLDER = "./downloaded_files"

# Path(LOCAL_SAVE_FOLDER).mkdir(parents=True,exist_ok=True)
# print(f"Save folder ready: {LOCAL_SAVE_FOLDER}")

# YEAR_TO_DOWNLOAD = [2021,2022]

# def download_one_month(year, month, save_folder):
#     """
#     Downloads flight data for a single month from the BTS website.
#     Returns the path to the saved CSV file, or None if something went wrong.

#     Parameters:
#         year        : int  -- e.g. 2023
#         month       : int  -- e.g. 6 (June)
#         save_folder : str  -- where to save the file locally
#     """
#     # Ensure folder exists
#     Path(save_folder).mkdir(parents=True, exist_ok=True)

#     # Build the filename exactly as BTS names it
#     zip_filename = (
#         f"On_Time_Reporting_Carrier_On_Time_Performance_"
#         f"1987_present_{year}_{month}.zip"
#     )

#     download_url = BTS_WEBSITE + zip_filename

#     zip_save_path = os.path.join(save_folder, zip_filename)
#     csv_save_path = zip_save_path.replace(".zip", ".csv")

#     # If already downloaded, skip
#     if os.path.exists(csv_save_path):
#         print(f"Already downloaded: {year}-{month} (skipping)")
#         return csv_save_path

#     print(f"Downloading {year}-{month} from {download_url}")
    
    
# for year in YEARS_TO_DOWNLOAD:
#     print(f"\n Year {year} ")

#     for month in range(1, 4):

#         csv_file = download_one_month(year, month, LOCAL_SAVE_FOLDER)
    
    