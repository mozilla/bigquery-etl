"""Add metadata.yaml for view if missing."""

# Load libraries
import os
import re
import yaml

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

    view_folders = get_top_level_dirs(view_folder_fpath)
    
    for view_folder in view_folders:
        # if metadata.yaml doesn't exist in folder, check if derived one has a copy
        view_level_fpath = os.path.join(view_folder_fpath, view_folder)
        print(view_level_fpath)

        if not os.path.isdir(view_level_fpath):
            continue

        files_in_view_folder = os.listdir(view_level_fpath)

        if 'metadata.yaml' not in files_in_view_folder:
            print("Check if derived table has metadata.yaml since view doesn't")
            #Check if corresponding derived folder has metadata.yaml

            derived_dir_fpath = os.path.join(root_directory, derived_dir)
            print('derived_dir_fpath: ', derived_dir_fpath)

            # Find matching derived folders with version suffix (e.g., view_folder_v1, view_folder_v2)
            if os.path.isdir(derived_dir_fpath):
                all_derived_folders = os.listdir(derived_dir_fpath)

                # Find all folders that match the pattern: view_folder_v#
                matching_folders = []
                pattern = re.compile(rf'^{re.escape(view_folder)}_v(\d+)$')

                for folder in all_derived_folders:
                    match = pattern.match(folder)
                    if match:
                        version = int(match.group(1))
                        matching_folders.append((version, folder))

                if matching_folders:
                    # Sort by version number and get the highest
                    matching_folders.sort(reverse=True)
                    highest_version, highest_version_folder = matching_folders[0]

                    derived_view_folder_fpath = os.path.join(derived_dir_fpath, highest_version_folder)
                    print(f'Checking derived folder (v{highest_version}): {derived_view_folder_fpath}')

                    # Check if the folder is a directory
                    if os.path.isdir(derived_view_folder_fpath):
                        # Get its files
                        files_in_derived_folder = os.listdir(derived_view_folder_fpath)
                        print(f'files_in_derived_folder: {files_in_derived_folder}')

                        # Check if it has a metadata.yaml
                        if 'metadata.yaml' in files_in_derived_folder:
                            print(f'✓ Found metadata.yaml in derived folder: {derived_view_folder_fpath}')

                            # Read the derived metadata.yaml
                            derived_metadata_path = os.path.join(derived_view_folder_fpath, 'metadata.yaml')
                            with open(derived_metadata_path, 'r') as f:
                                derived_metadata = yaml.safe_load(f)

                            # Extract only the specified keys
                            view_metadata = {}
                            keys_to_copy = ['friendly_name', 'description', 'owners']

                            for key in keys_to_copy:
                                if key in derived_metadata:
                                    view_metadata[key] = derived_metadata[key]

                            # Only create the file if we have at least one key to copy
                            if view_metadata:
                                view_metadata_path = os.path.join(view_level_fpath, 'metadata.yaml')
                                with open(view_metadata_path, 'w') as f:
                                    yaml.dump(view_metadata, f, default_flow_style=False, sort_keys=False)
                                print(f'✓ Created metadata.yaml in view folder: {view_metadata_path}')
                                print(f'  Copied keys: {list(view_metadata.keys())}')
                            else:
                                print(f'✗ No relevant keys found in derived metadata to copy')
                        else:
                            print(f'✗ No metadata.yaml found in derived folder: {derived_view_folder_fpath}')
                    else:
                        print(f'✗ Path is not a directory: {derived_view_folder_fpath}')
                else:
                    print(f'✗ No matching derived folder found for pattern: {view_folder}_v#')
            else:
                print(f'✗ Derived directory does not exist: {derived_dir_fpath}')

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