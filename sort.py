import os
import shutil
import fitparse
import gpxpy

# Define source and destination directories
source_dir = "activities"
cycling_dir = source_dir + "/cycling"
running_dir = source_dir + "/running"
walking_dir = source_dir + "/walking"

# Ensure destination directories exist
for folder in [cycling_dir, running_dir, walking_dir]:
    os.makedirs(folder, exist_ok=True)

def determine_activity_type_fit(file_path):
    """Determine activity type from .fit file."""
    fitfile = fitparse.FitFile(file_path)
    # First check for sport messages
    for record in fitfile.get_messages("sport"):
        sport = record.get_value("sport")
        if sport:
            sport = sport.lower()
            if sport == "ebikeride":
                return "cycling"
            return sport

    # If not found, also check in session messages
    for record in fitfile.get_messages("session"):
        sport = record.get_value("sport")
        if sport:
            sport = sport.lower()
            if sport == "ebikeride":
                return "cycling"
            return sport

    return "unknown"

def determine_activity_type_gpx(file_path):
    """Determine activity type from .gpx file."""
    with open(file_path, "r") as gpx_file:
        gpx = gpxpy.parse(gpx_file)
        if gpx.tracks:
            for track in gpx.tracks:
                if track.type:
                    sport = track.type.lower()
                    if sport == "ebikeride":
                        return "cycling"
                    return sport
    return "unknown"

def move_file(file_path, activity_type):
    """Move file based on activity type."""
    if activity_type == "cycling":
        dest_folder = cycling_dir
    elif activity_type == "running":
        dest_folder = running_dir
    elif activity_type == "walking":
        dest_folder = walking_dir
    else:
        print(f"Unknown activity type for {file_path}, skipping...")
        return
    
    shutil.move(file_path, os.path.join(dest_folder, os.path.basename(file_path)))
    print(f"Moved {file_path} to {dest_folder}")

# Process files
for file_name in os.listdir(source_dir):
    file_path = os.path.join(source_dir, file_name)
    if os.path.isfile(file_path):
        activity_type = "unknown"
        try:
            if file_name.endswith(".fit"):
                activity_type = determine_activity_type_fit(file_path)
            elif file_name.endswith(".gpx"):
                activity_type = determine_activity_type_gpx(file_path)
            
            if activity_type in ["cycling", "running", "walking"]:
                move_file(file_path, activity_type)
            else:
                print(f"Uncategorised activity type {activity_type} for {file_name}, skipping...")
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
