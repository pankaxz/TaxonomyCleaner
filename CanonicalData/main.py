import json
import os

def load_canonical_data():
    file_path = os.path.join(os.path.dirname(__file__), 'Input', 'canonical_data.json')
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {file_path}")
        return None

def sort_canonical_data(data):
    if not data:
        return None
    
    sorted_data = {}
    # Sort categories (keys of the main dictionary)
    for category in sorted(data.keys(), key=str.lower):
        sorted_data[category] = {}
        # Sort subcategories (keys of the inner dictionary)
        for subcategory in sorted(data[category].keys(), key=str.lower):
            # Sort the list of values for each subcategory
            sorted_data[category][subcategory] = sorted(data[category][subcategory], key=str.lower)
            
    return sorted_data

def save_canonical_data(data):
    file_path = os.path.join(os.path.dirname(__file__), 'Input', 'canonical_data.json')
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Successfully saved sorted data to {file_path}")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    data = load_canonical_data()
    if data:
        print("Successfully loaded canonical_data.json")
        sorted_data = sort_canonical_data(data)
        save_canonical_data(sorted_data)
