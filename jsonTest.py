import json

# Path to the JSON file
json_file_path = "C:/Users/Thomas/Studium/SteamProc/TestJSON.json"

# Read the JSON file with explicit encoding
with open(json_file_path, "r", encoding="utf-8") as file:
    json_data = json.load(file)

# Print the JSON data in a readable format
formatted_json = json.dumps(json_data, indent=4)
print(formatted_json)
