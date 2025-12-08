"""Add metadata.yaml for view if missing."""

# Load libraries
import os
import re

# Set directory to loop through
root_directory = '../../sql/moz-fx-data-shared-prod'

# Define reusable functions
def get_top_level_dirs(root_path):
    top_level_dirs = []
    for item in os.listdir(root_path):
        top_level_dirs.append(item)
    top_level_dirs.sort()
    return top_level_dirs

# Find all top level folders in that folder
top_level_directories = get_top_level_dirs(root_directory)
print(top_level_directories)

# Find all the "derived" directories in that list
derived_top_level_directories = []

for top_level_dir in top_level_directories:
    if "_derived" in top_level_dir:
        derived_top_level_directories.append(top_level_dir)

print(derived_top_level_directories)

# Loop through each derived folder
for derived_dir in derived_top_level_directories:
    view_dir = re.sub(r"_derived", "", derived_dir)
    # If there is no corresponding view layer, don't bother
    if view_dir not in top_level_directories:
        continue


    processing_str = f"""Checking if we can add metadata.yaml files in {view_dir}, using metadata found in {derived_dir}."""
    print(processing_str)

    view_folder_fpath = os.path.join(root_directory, view_dir)

    print('view_folder_fpath: ', view_folder_fpath)
    view_folders = get_top_level_dirs(view_folder_fpath)
    
    print('view folders:')
    print(view_folders)
    for view_folder in view_folders:
        print(view_folder)
        # if metadata.yaml doesn't exist in folder, check if derived one has a copy
        view_level_fpath = os.path.join(view_folder_fpath, view_folder)
        print(view_level_fpath)

        if not os.path.isdir(view_level_fpath):
            continue

        files_in_view_folder = os.listdir(view_level_fpath)
        print('files_in_view_folder: ')
        print(files_in_view_folder)

        if 'metadata.yaml' not in files_in_view_folder:
            print("Check if derived table has metadata.yaml since view doesn't")
            #Check if corresponding derived folder has metadata.yaml

            derived_dir_fpath = os.path.join(root_directory, derived_dir)
            print('derived_dir_fpath: ', derived_dir_fpath)
            # Calculate the corresponding derived view folder

            # Get it's files

            # Check if it has a metadata.yaml

    # Get all the directories within this directory







# files_in_directory = []
# for entry in os.listdir(directory_path):
#     full_path = os.path.join(directory_path, entry)
#     if os.path.isfile(full_path):
#         files_in_directory.append(entry)

# print(files_in_directory)

    


# dirname for dirname in os.listdir(root_directory)

# def get_top_level_directories(path):
#     return [
#         name for name in os.listdir(path)
#         if os.path.isdir(os.path.join(path, name))
#     ]

# Example usage:
# base_path = "/your/base/directory"
# top_dirs = get_top_level_directories(base_path)
# print(top_dirs)