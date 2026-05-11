"""
Data Acquisition — US Flight Delay Analytics
========================================================
Project : US Domestic Flight Delay Analytics Platform
Dataset : Bureau of Transportation Statistics (BTS) On-Time Performance
https://www.transtats.bts.gov/
Data Engineer Notes:- Hum 3 saal ke monthly CSVs download karte hain (2021-2023) = ~7 GB raw data- Airport reference (FAA) ~5 MB JSON format mein hoti hai- Carrier lookup CSV ~50 KB ki hoti hai- download_manifest.json sabhi completed downloads ko track karta hai
taaki idempotency ensure ho — koi bhi file kabhi do baar download na ho.
Idempotency ka matlab: Script baar baar chalao, result same rahega.
Agar ek file pehle se download ho chuki hai to dobara download nahi hogi.
"""
import os
# File/directory operations ke liye
# os module operating system ke saath interact karta hai
# Folder create, file path join, file existence check jaise kaam hote hain
import json
# JSON read/write ke liye (manifest store karna)
# JSON format me data save/load karne ke liye use hota hai
import time
# Sleep/delay ke liye (server ko overload na karna)
# time.sleep() use karke request ke beech delay lagate hain
import requests  # HTTP requests ke liye (file download karna)
# Internet/API se data fetch karne ke liye most popular library
from pathlib import Path
# Path object modern aur cleaner path handling deta hai
from datetime import datetime, timezone  # UTC timestamp ke liye
# Current date/time aur timezone-aware timestamps ke liye
# DIRECTORIES — Jahan downloaded files save hongi

LOCAL_DOWNLOAD_DIR = "./downloads"
# Current project folder ke andar downloads naam ka folder
# '.' current directory ko represent karta hai
# Agar './downloads' folder exist nahi karta to create kar do
# exist_ok=True matlab: folder pehle se ho to error mat do
Path(LOCAL_DOWNLOAD_DIR).mkdir(exist_ok=True)
# mkdir() = make directory
# Agar folder already hai to koi issue nahi aayega

# MANIFEST FILE — Kaun kaun si files download ho chuki hain, yeh track karta hai

# Real-world pattern: Yahi idea wget --continue, Spark checkpointing,
# dbt state, aur Airflow task state stores mein bhi use hota hai.
# Manifest ko downloads ke saath rakhte hain taaki data ke saath travel kare.
MANIFEST_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "download_manifest.json")
# os.path.join automatically correct path banata hai
# Windows/Linux dono me separator issue avoid hota hai

# BTS (Bureau of Transportation Statistics) CONFIG

BTS_BASE_URL = "https://transtats.bts.gov/PREZIP/"  # BTS ZIP files ka base URL
# Ye base URL hai jiske saath filename attach karke final download URL banega
YEARS = [2021]  # Kin kin saalon ka data chahiye
# List format me years store kiye gaye hain
# Future me multiple years add kar sakte ho
MONTHS = range(1, 3)  # Kin kin mahinon ka data chahiye
# range(1,3) => 1 aur 2 return karega
# Python range ka last value include nahi hota
# Full year ke liye: range(1, 13) kar do
# MANIFEST HELPER FUNCTIONS
# Ye functions manifest file ko load, save aur update karne ka kaam karte hain
# Functions reusable blocks hote hain jisse same code baar baar nahi likhna padta hai


def _load_manifest() -> dict:
    # Function manifest file ko load karega
    # -> dict ka matlab function dictionary return karega
    """
    Manifest file ko disk se load karo.
    Manifest ka schema (structure) kuch aisa hota hai:
    {
      "downloaded_files": {
        "<filename>": {
          "status":          "completed",           # completed ya failed
          "downloaded_at":   "2024-01-15T10:30:00Z",
          "file_size_bytes":  123456789,
          "local_zip_path":  "./downloads/<filename>",
          "year":  2021,
          "month": 1
        },
        ...
      }
    }
    Production note: Real production mein yeh data Delta table, DynamoDB,
    ya dedicated metadata DB mein store hoti hai. Single-machine pipeline ke
    liye JSON manifest kaafi hai kyunki human-readable bhi hai.
    """
    if os.path.exists(MANIFEST_PATH):
        # Check karta hai manifest file exist karti hai ya nahi
        # Agar manifest file exist karti hai to use load karo
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            # File read mode me open ho rahi hai
            # with use karne se file automatically close ho jati hai
            return json.load(f)
    # Pehli baar run ho raha hai — empty manifest return karo
    return {"downloaded_files": {}}
    # Empty structure return ho raha hai


def _save_manifest(manifest: dict) -> None:
    # Manifest dictionary ko save karne wala function
    """
    Manifest ko disk par atomically save karo.
    'Atomic write' technique:
    1. Pehle ek temporary file (.tmp) mein likho
    2. Phir us file ko final path par rename karo (os.replace)
    Yeh isliye karte hain kyunki:
    - Agar seedha manifest file mein likhte waqt program crash ho jaye
      to file corrupt ho sakti hai (adhi purani, adhi nayi data)
    - os.replace() POSIX aur Windows dono par atomic hai —
      ya to poori file replace hogi, ya bilkul nahi hogi
    """
    tmp_path = MANIFEST_PATH + ".tmp"  # Temporary file banao
    # Original filename ke saath .tmp append kiya gaya
    with open(tmp_path, "w") as f:
        # Temporary file write mode me open karo
        json.dump(manifest, f, indent=2)
        # Dictionary ko formatted JSON me save karo
        # indent=2 => pretty format
    os.replace(tmp_path, MANIFEST_PATH)  # Atomic rename (crash-safe)
    # Temporary file final manifest file se replace ho jayegi


def _is_already_downloaded(manifest: dict, filename: str) -> bool:
    # Check karega file pehle se successfully downloaded hai ya nahi
    """
    Check karo ki file pehle se successfully download ho chuki hai ya nahi.
    Sirf 'completed' status ko 'downloaded' maana jata hai.
    'failed' entry ko downloaded NAHI maana jata — wo retry ke liye eligible
hai.
    Returns:
        True  — file pehle se download ho chuki hai, skip karo
        False — file download karni padegi (nai hai ya pehle fail hui thi)
    """
    entry = manifest["downloaded_files"].get(filename)
    # Dictionary me filename search karo
    # .get() use karne se key missing hone par error nahi aata
    return entry is not None and entry.get("status") == "completed"
    # True tabhi return hoga jab:
    # 1. Entry exist kare
    # 2. Status completed ho


# Ye function successful download ka record save karta hai
def _record_download(manifest: dict, filename: str, year: int, month: int, local_zip_path: str, file_size_bytes: int) -> None:
    """
    Successful download ka record manifest mein likhkar disk par save karo.
    Yeh function tab call hota hai jab:- File successfully download ho jaye- Ya file disk par ho lekin manifest mein record nahi ho (crash recovery)
    """
    manifest["downloaded_files"][filename] = {
        # Dictionary ke andar filename key create ho rahi hai
        "status": "completed",
        # File successfully complete ho gayi
        "downloaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        # Current UTC timestamp save ho raha hai
        # strftime date ko formatted string me convert karta hai
        "file_size_bytes": file_size_bytes,
        # File size bytes me save karo
        "local_zip_path": local_zip_path,
        # Local system me file kaha stored hai
        "year": year,
        # Downloaded data ka year
        "month": month,
        # Downloaded data ka month
    }
    _save_manifest(manifest)
    # Updated manifest ko disk par save karo


def _record_failure(manifest: dict, filename: str, year: int, month: int, error: str) -> None:  # Failed download ka record save karne wala function
    """
    Failed download ka record manifest mein likhkar disk par save karo.
    Yeh isliye zaroori hai:- Audit trail — dekh sako ki kya galat hua- Retry logic — failed entries ko dobara download karne ki koshish ho sakti hai- Failed entry 'completed' nahi mani jati (isliye retry hoga)
    """
    manifest["downloaded_files"][filename] = {
        # Failed download ki details dictionary me store ho rahi hain
        "status": "failed",
        # Status failed mark karo
        "error": error,
        # Actual error message save ho raha hai
        "year": year,
        # Failure kis year file me hua
        "month": month,
        # Failure kis month file me hua
        "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        # Failure timestamp save karo
    }
    _save_manifest(manifest)
    # Failure details ko bhi immediately save karo


def download_bts_month(year: int, month: int, dest_dir: str, manifest: dict) -> str | None:
    """
    BTS On-Time Performance ka ZIP file download karo — idempotent tarike se.
    """
    #  BTS ka file naming convention follow karo
    filename = (
        f"On_Time_Reporting_Carrier_On_Time_Performance"
        f"_1987_present_{year}_{month}.zip"
    )
    # Dynamic filename create ho raha hai
    # f-string me variables directly inject hote hain
    url = f"{BTS_BASE_URL}{filename}"
    #  Final download URL generate ho raha hai
    local_zip = os.path.join(dest_dir, filename)
    # Local machine me save path create ho raha hai

    # Manifest kehta hai file pehle se hai 
    if _is_already_downloaded(manifest, filename):
      #  Agar manifest bol raha hai file already complete hai
        print(f"  [SKIP] Already downloaded (manifest): {filename}")
        # Console me skip message print karo
        return local_zip
      #  Existing path return kar do

    # File disk par hai lekin manifest mein nahi hai 
    if os.path.exists(local_zip) and os.path.getsize(local_zip) > 0:
      #  Check karo file physically disk me exist karti hai ya nahi
      # Aur size 0 se bada hai ya nahi
        size = os.path.getsize(local_zip)
        # Existing file ka size nikalo
        print("  [SKIP] ZIP found on disk but not in manifest — recording it.")
        print(f"         {filename}  ({size:,} bytes)")
        # Informational logging
        _record_download(manifest, filename, year, month, local_zip, size)
        #  Existing file ko manifest me add kar do
        return local_zip
      #  Existing path return karo

# Download proceed karo 
    print(f"  [DOWNLOAD] {year}-{month:02d}  →  {url}")
    #  Download start log
    #  :02d => month ko 2 digit format me print karega
    try:
      # Risky code block
        resp = requests.get(url, stream=True, timeout=120)
        # HTTP GET request bhejo
        # stream=True => chunks me data ayega
        # timeout=120 => 120 sec wait karega
        resp.raise_for_status()
        # HTTP error aaya to exception raise hoga
        bytes_written = 0
        # Track karega total downloaded bytes
        with open(local_zip, "wb") as f:
          #  ZIP file binary write mode me open ho rahi hai
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
              #  Data ko 1 MB chunks me read karo
                if chunk:
                    f.write(chunk)
                    # Chunk ko file me write karo
                    bytes_written += len(chunk)
                    #  Total bytes update karo
        print(f"  [OK]   Saved {bytes_written:,} bytes → {local_zip}")
        # Success message print karo
        _record_download(manifest, filename, year, month, local_zip, bytes_written)
        #  Successful download ko manifest me record karo
        return local_zip
      #  Local file path return karo
    except Exception as exc:
      # Agar koi bhi error aaye to yaha control aayega
        print(f"  [ERROR] {year}-{month:02d}: {exc}")
        # Error message console me print karo
        _record_failure(manifest, filename, year, month, str(exc))
        # Failure details save karo
        if os.path.exists(local_zip):
          # Agar incomplete file ban gayi ho
            os.remove(local_zip)
            # Corrupted/partial file delete karo
        return None
      # Failure case me None return karo


# Manifest summary print karne wala function
def print_manifest_summary(manifest: dict) -> None:
    entries = manifest["downloaded_files"]
    # Saari downloaded entries nikaalo
    if not entries:
        # Agar koi entry nahi hai
        print("  (manifest is empty — no downloads recorded yet)")
        return
    completed = [e for e in entries.values() if e["status"] == "completed"]
    # Completed entries ki list comprehension se filtering
    failed = [e for e in entries.values() if e["status"] == "failed"]
    # Failed entries filter karo
    total_bytes = sum(e.get("file_size_bytes", 0) for e in completed)
    # Completed files ka total size calculate karo
    print(f"{'─'*60}")  # Decorative separator line
    print(f"  Manifest: {MANIFEST_PATH}")
    # Manifest path print karo
    print(f"  Completed : {len(completed)}  |  Failed: {len(failed)}")
    # Completed aur failed count print karo
    print(f"  Total size: {total_bytes / (1024**3):.2f} GB")
    # Bytes ko GB me convert karke print karo
    if failed:
        # Agar failed files exist karti hain
        print("  Failed files:")
        for name, e in entries.items():
            # Dictionary ke har item par iterate karo
            if e["status"] == "failed":
                # Sirf failed entries print karo
                print(f"     {name}  — {e.get('error', 'unknown error')}")
                # Filename + error print karo
    print(f"{'─'*60}")  # Decorative separator line


# ------------------------------------------------------------------------------------------------------------------
# STEP 2: Download Airport Reference Data (FAA)
# FAA/OpenFlights airport metadata provide karta hai
# Isme airport ka:
# - IATA code
# - Airport name
# - City
# - Country
# - Latitude / Longitude
# - Altitude
# jaise details hoti hain 


def download_airport_reference(dest_dir):
    # Function airport reference data download karega
    # dest_dir = folder jaha files save hongi

    """
    Download airport reference from OpenFlights (public, free, no key needed).
     Nearby 7,700 airports worldwide.

    Columns: id, name, city, country, IATA, ICAO, lat, lon, altitude, tz_offset,
             DST, timezone_name, type, source

    DQ Watchpoints:
      - IATA field can be '\\N' (null in OpenFlights format) — filter these
      -- in python to see the null value  \\N is used
      - lat/lon values: range check lat -90..90, lon -180..180
      - Some airports appear twice with different source entries
    """
    url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
    # OpenFlights dataset ka raw download URL
    local_path = os.path.join(dest_dir, "airports.dat")
    # Local machine me file kaha save hogi uska path
    print(f"[DOWNLOAD] Airport reference data")
    # Console me message print hoga
    resp = requests.get(url, timeout=30)
    # HTTP GET request bhejo
    # timeout=30 => 30 sec wait karega response ke liye

    # PRINTING THE AIRPORT DATA USING THE BELOW LINE
    # print(resp.content)
    # Agar raw binary data dekhna ho to is line ko uncomment kar sakte ho
    resp.raise_for_status()
    # Agar HTTP error aaye (404/500 etc.)
    # to ye exception raise karega
    with open(local_path, "wb") as f:
        # File binary write mode me open ho rahi hai
        f.write(resp.content)
        # Downloaded content ko file me save karo
    # Parse and save as clean JSON
    # Raw .dat file ko clean JSON me convert karenge
    import csv
    # CSV parsing ke liye csv module import kiya
    cols = [
        "id",
        "name",
        "city",
        "country",
        "iata",
        "icao",
        "latitude",
        "longitude",
        "altitude",
        "utc_offset",
        "dst",
        "timezone",
        "type",
        "source"
    ]
    # Ye columns ka structure define kar raha hai
    # OpenFlights dataset me same order hota hai
    airports = []
    # Clean airport records store karne ke liye empty list
    for line in resp.text.splitlines():
        # Response text ko line-by-line process karo
        row = next(csv.reader([line]))
        # CSV parser line ko correctly split karega
        # commas aur quotes properly handle honge
        if len(row) == len(cols):
            # Sirf valid rows process karo
            # Jisme proper number of columns ho
            d = dict(zip(cols, row))
            # Column names + values ko dictionary me convert karo
            # DQ: Filter valid IATA codes (3 uppercase letters)
            if d["iata"] and len(d["iata"]) == 3 and d["iata"] != "\\N":
                # Conditions:
                # 1. IATA empty nahi hona chahiye
                # 2. Length exactly 3 honi chahiye
                # 3. '\\N' null value nahi honi chahiye
                airports.append(d)
                # Valid airport dictionary ko list me add karo
    json_path = os.path.join(dest_dir, "airports.json")
    # Final cleaned JSON file ka path
    with open(json_path, "w") as f:
        # JSON file write mode me open karo
        json.dump(airports, f, indent=2)
        # Airport list ko JSON format me save karo
        # indent=2 => pretty formatting
    print(f"  [OK] {len(airports)} airports → {json_path}")
    # Total valid airports print karo
    return json_path
    # Final JSON file path return karo

if __name__ == "__main__":
    # Ye block tabhi run hota hai
    # jab file directly execute hoti hai
    print("=" * 70)
    # Decorative line print karo
    print("  US Flight Delay Analytics — Data Acquisition")
    # Project title print karo
    print("=" * 70)
    # download the airport code info in dat file then the json file as required
    airport_path = download_airport_reference(LOCAL_DOWNLOAD_DIR)
    # Airport reference data download function call karo
    # Returned JSON path airport_path variable me store hoga
    # Load (or create) the manifest once at startup
    manifest = _load_manifest()
    # Existing manifest load karo
    # Agar nahi hai to new empty manifest create hoga
    print(
        f"\n  Manifest loaded — {len(manifest['downloaded_files'])} entries found.")
    # Manifest me total kitni entries hain wo print karo
    for year in YEARS:
        # YEARS list ke har year par loop chalega
        for month in MONTHS:
            # Har month par nested loop chalega
            zip_path = download_bts_month(
                year,
                month,
                LOCAL_DOWNLOAD_DIR,
                manifest
            )
            # BTS monthly zip download function call ho raha hai
            if zip_path:
                # Agar successfully download hua
                print(f"  Ready: {zip_path}")
                # Downloaded file path print karo
                # Uncomment when S3 upload is wired up:
                # s3_prefix = f"flights/year={year}/month={month:02d}"
                # upload_to_s3(zip_path, s3_prefix)
                # Future use:
                # S3 upload ke liye partition path create hoga
            time.sleep(2)
            # 2 second ka delay
            # BTS server overload avoid karne ke liye
    print_manifest_summary(manifest)
    # Final manifest summary print karo
    print("  Done.")
    # Script completion message print karo
