#!/usr/bin/env python
# coding: utf-8

# # Common Crawl WET File Downloader & Processor
# 
# This notebook downloads random WET files from Common Crawl based on the paths in `wet.paths`, then:
# 1. Extracts the gzipped files
# 2. Deletes the compressed versions
# 3. Renames files to `data-{file_id}` format based on their position in the paths file

# In[ ]:


import requests
import random
import os
from pathlib import Path
from tqdm import tqdm


# ## Step 1: Download Files
# 
# Configure and download random WET files from Common Crawl.

# In[ ]:


# Configuration
BASE_URL = "https://data.commoncrawl.org/"
PATHS_FILE = "wet.paths"
DOWNLOAD_DIR = "downloaded_wet_files"
NUM_FILES_TO_DOWNLOAD = 20


# In[ ]:


# Create download directory if it doesn't exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
print(f"Download directory: {os.path.abspath(DOWNLOAD_DIR)}")


# In[ ]:


# Read all paths from the file
with open(PATHS_FILE, 'r') as f:
    all_paths = [line.strip() for line in f.readlines() if line.strip()]

print(f"Total paths available: {len(all_paths)}")


# In[ ]:


# Select random paths
random.seed(42)  # For reproducibility, remove or change seed for different random selection
selected_paths = random.sample(all_paths, min(NUM_FILES_TO_DOWNLOAD, len(all_paths)))

print(f"Selected {len(selected_paths)} random files to download")
print("\nSelected files:")
for i, path in enumerate(selected_paths, 1):
    print(f"{i}. {path}")


# In[ ]:


def download_file(url, local_path):
    """
    Download a file from URL to local path with progress bar.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(local_path, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
            else:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(local_path)) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
        
        return True, None
    except Exception as e:
        return False, str(e)


# In[ ]:


# Download selected files
successful_downloads = 0
failed_downloads = []

print(f"\nStarting download of {len(selected_paths)} files...\n")

for i, path in enumerate(selected_paths, 1):
    # Construct full URL
    url = BASE_URL + path
    
    # Create local file path (preserve filename)
    filename = os.path.basename(path)
    local_path = os.path.join(DOWNLOAD_DIR, filename)
    
    print(f"[{i}/{len(selected_paths)}] Downloading: {filename}")
    
    # Download the file
    success, error = download_file(url, local_path)
    
    if success:
        successful_downloads += 1
        file_size = os.path.getsize(local_path) / (1024 * 1024)  # Size in MB
        print(f"✓ Successfully downloaded ({file_size:.2f} MB)\n")
    else:
        failed_downloads.append((filename, error))
        print(f"✗ Failed: {error}\n")

# Summary
print("="*80)
print(f"Download Summary:")
print(f"  Successful: {successful_downloads}/{len(selected_paths)}")
print(f"  Failed: {len(failed_downloads)}")

if failed_downloads:
    print("\nFailed downloads:")
    for filename, error in failed_downloads:
        print(f"  - {filename}: {error}")


# In[ ]:


# List downloaded files
downloaded_files = os.listdir(DOWNLOAD_DIR)
print(f"\nFiles in download directory: {len(downloaded_files)}")
for file in downloaded_files:
    file_path = os.path.join(DOWNLOAD_DIR, file)
    file_size = os.path.getsize(file_path) / (1024 * 1024)
    print(f"  - {file} ({file_size:.2f} MB)")


# ## Step 2: Extract Files and Rename
# 
# Now we'll extract the downloaded .gz files, delete the compressed versions, and rename them with their file IDs from the paths file.

# In[ ]:


import gzip
import shutil

# Create a mapping of filename to its index in the original paths file
filename_to_index = {}
for i, path in enumerate(all_paths):
    filename = os.path.basename(path)
    filename_to_index[filename] = i

print(f"Created mapping for {len(filename_to_index)} files")


# In[ ]:


def extract_gz_file(gz_path, output_path):
    """
    Extract a .gz file to the specified output path.
    """
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return True, None
    except Exception as e:
        return False, str(e)

# Extract all .gz files in the download directory
print("Extracting downloaded files...\n")
extracted_files = []
extraction_errors = []

for filename in os.listdir(DOWNLOAD_DIR):
    if filename.endswith('.gz'):
        gz_path = os.path.join(DOWNLOAD_DIR, filename)
        # Remove .gz extension for extracted file
        extracted_filename = filename[:-3]  # Remove '.gz'
        extracted_path = os.path.join(DOWNLOAD_DIR, extracted_filename)
        
        print(f"Extracting: {filename}")
        success, error = extract_gz_file(gz_path, extracted_path)
        
        if success:
            extracted_size = os.path.getsize(extracted_path) / (1024 * 1024)
            print(f"✓ Extracted ({extracted_size:.2f} MB)")
            extracted_files.append((filename, extracted_filename))
        else:
            print(f"✗ Failed: {error}")
            extraction_errors.append((filename, error))
        print()

print("="*80)
print(f"Extraction Summary:")
print(f"  Successful: {len(extracted_files)}")
print(f"  Failed: {len(extraction_errors)}")


# In[ ]:


# Delete the original .gz files after successful extraction
print("\nDeleting compressed files...\n")
deleted_count = 0

for gz_filename, extracted_filename in extracted_files:
    gz_path = os.path.join(DOWNLOAD_DIR, gz_filename)
    if os.path.exists(gz_path):
        os.remove(gz_path)
        print(f"✓ Deleted: {gz_filename}")
        deleted_count += 1

print(f"\nDeleted {deleted_count} compressed files")


# In[ ]:


# Rename extracted files to data-{file_id}
print("\nRenaming files to data-{file_id} format...\n")
renamed_files = []
rename_errors = []

for gz_filename, extracted_filename in extracted_files:
    extracted_path = os.path.join(DOWNLOAD_DIR, extracted_filename)
    
    # Get the file ID from the paths file
    if gz_filename in filename_to_index:
        file_id = filename_to_index[gz_filename]
        new_filename = f"data-{file_id}"
        new_path = os.path.join(DOWNLOAD_DIR, new_filename)
        
        try:
            os.rename(extracted_path, new_path)
            print(f"✓ Renamed: {extracted_filename}")
            print(f"  -> data-{file_id}")
            renamed_files.append((extracted_filename, new_filename, file_id))
        except Exception as e:
            print(f"✗ Failed to rename {extracted_filename}: {e}")
            rename_errors.append((extracted_filename, str(e)))
    else:
        print(f"⚠ Warning: Could not find file ID for {gz_filename}")
        rename_errors.append((extracted_filename, "File ID not found"))
    print()

print("="*80)
print(f"Rename Summary:")
print(f"  Successful: {len(renamed_files)}")
print(f"  Failed: {len(rename_errors)}")


# In[ ]:


# List final processed files
print("\nFinal files in download directory:")
print("="*80)

current_files = sorted(os.listdir(DOWNLOAD_DIR))
total_size = 0

for file in current_files:
    file_path = os.path.join(DOWNLOAD_DIR, file)
    if os.path.isfile(file_path):
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        total_size += file_size
        
        # Extract file_id if it's a data-{id} file
        if file.startswith('data-'):
            file_id = file.split('-')[1]
            original_path = all_paths[int(file_id)]
            print(f"\n{file} ({file_size:.2f} MB)")
            print(f"  Original: {os.path.basename(original_path)}")
            print(f"  Path ID: {file_id}")
        else:
            print(f"\n{file} ({file_size:.2f} MB)")

print("\n" + "="*80)
print(f"Total files: {len(current_files)}")
print(f"Total size: {total_size:.2f} MB ({total_size/1024:.2f} GB)")


# In[ ]:





# In[ ]:




