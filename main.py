import pandas as pd
import json
import os
import shutil


def map_values_to_descriptions(data, source_column, descriptions):
    mapped_data = {}
    for key, desc in descriptions.items():
        # Try to find and map the value using the current description's corresponding index
        if key in data[source_column]:
            mapped_data[desc] = data[source_column][key]
    # Add quarter and date information if present
    if '0' in data[source_column] and '1' in data[source_column]:
        mapped_data['quarter'] = data[source_column]['0']  # "Three Months Ended"
        mapped_data['date'] = data[source_column]['1']  # Specific date
    return mapped_data


def process_json_data(json_file_path):
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Extract descriptions as keys from the 'Unnamed: 0' column (ignoring nulls)
    descriptions = {str(k): v for k, v in data['Unnamed: 0'].items() if v}

    # Initialize a structure to hold all mapped data
    all_mapped_data = {}

    # Iterate over each column in the JSON data, excluding 'Unnamed: 0' which holds descriptions
    for column in data:
        if column != 'Unnamed: 0' and column.startswith('Unnamed'):
            mapped_data = map_values_to_descriptions(data, column, descriptions)
            new_key = column.replace('Unnamed', 'Result')
            all_mapped_data[new_key] = mapped_data

    return all_mapped_data


def extract_specific_table(number, dfs):
    dfs[number].dropna(axis=1, inplace=True, thresh=3)
    dfs[number] = dfs[number].T.drop_duplicates().T
    dfs[number] = dfs[number].drop(columns=dfs[number].columns[(dfs[number] == '$').any()])
    dfs[number] = dfs[number].drop(columns=dfs[number].columns[(dfs[number] == '%').any()])
    # dfs[number].to_csv(f"csv{number}.csv", index=False)
    dfs[number].to_json(f"./temp/json{number}.json", indent=4)

    json_file_path = f"./temp/json{number}.json"
    all_mapped_data = process_json_data(json_file_path)

    # Print the combined JSON structure
    # print(json.dumps(all_mapped_data, indent=4))

    # Optionally save this combined structure to a file
    output_file_path = f"./final/FinalJson{number}.json"
    with open(output_file_path, 'w') as file:
        json.dump(all_mapped_data, file, indent=4)


def extract_all_tables():
    url = './aapl-20231230.html'
    dfs = pd.read_html(url, header=0, flavor='bs4', encoding='cp1252')
    print(len(dfs))
    for i in range(len(dfs)):
        try:
            extract_specific_table(i, dfs)
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            continue;


def delete_everything_in_folder(folder_path):
    # Check if the folder exists
    if not os.path.exists(folder_path):
        print(f"The folder {folder_path} does not exist.")
        return

    # Iterate over all the items in the folder
    for item_name in os.listdir(folder_path):
        # Construct full path to the item
        item_path = os.path.join(folder_path, item_name)

        # Check if it's a file or a folder
        if os.path.isfile(item_path):
            os.remove(item_path)  # Delete the file
            print(f"Deleted file: {item_path}")
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)  # Delete the directory and all its contents
            print(f"Deleted folder and its contents: {item_path}")
        else:
            print(f"Unknown item type: {item_path}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    extract_all_tables()
    delete_everything_in_folder("./temp")
