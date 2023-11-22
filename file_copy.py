import os
import shutil
import csv
from concurrent.futures import ThreadPoolExecutor
import threading

# Define the source and target directories and the buffer size for file copying
source_dir = '/home/komodo/Downloads'
target_dir = '/home/komodo/Documents'
csv_file = 'file_list.csv'
buffer_size = 1024 * 1024  # 1MB buffer
file_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.pdf', '.doc', '.docx', '.txt', '.webp', '.svg', '.csv', '.mov', '.mp3', '.mp4', '.ppt', '.pptx', '.rar', '.zip', '.wav', '.wmv', 'xls', 'xlsx']

# Use a lock for thread-safe operations on the CSV data list
csv_data_lock = threading.Lock()
csv_data = []

# Function to copy a single file
def copy_file(source_file, target_file):
    with open(source_file, 'rb') as src, open(target_file, 'wb') as dst:
        shutil.copyfileobj(src, dst, buffer_size)

# Function to process a single file (copy and log its details)
def process_file(root, file):
    copied = False
    if any(file.lower().endswith(ext) for ext in file_extensions):
        source_file = os.path.join(root, file)
        target_file = os.path.join(target_dir, os.path.relpath(source_file, source_dir))

        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        copy_file(source_file, target_file)
        copied = True

    with csv_data_lock:
        csv_data.append([file, os.path.splitext(file)[1], source_file, copied])

# Main function to walk through the directory, process files and generate CSV
def copy_files_and_generate_csv(source, target, csv_path):
    with ThreadPoolExecutor() as executor:
        for root, dirs, files in os.walk(source):
            for file in files:
                executor.submit(process_file, root, file)

    # Write to CSV after all files are processed
    with open(csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['File Name and Extension', 'File Extension', 'File Path', 'Copied'])
        writer.writerows(csv_data)

# Run the function
copy_files_and_generate_csv(source_dir, target_dir, csv_file)
