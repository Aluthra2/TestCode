import pandas as pd
import json
import os
import shutil
import re

def is_specific_date_format(string):
    """
    Checks if the given string matches a specific date format (e.g., "December 31, 2022").
    """
    return bool(re.match(r'^[A-Za-z]+ \d{1,2}, \d{4}$', string))


def process_json_data(json_file_path):
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    structure_column = "Unnamed: 0"
    value_columns = [key for key in data.keys() if key != structure_column]

    nested_json = {}
    current_section = None

    for index, label in data[structure_column].items():
        if label is None:  # End of a section
            current_section = None
        else:
            if current_section is None:  # New section begins
                current_section = label
                nested_json[current_section] = {}
            else:
                for col in value_columns:
                    if "Three Months Ended" in data[col]["0"]:
                        # For json4, include the general time period with the date
                        date_label = f"{data[col]['0']} {data[col]['1']}"
                    else:
                        # For json6, directly use the date, avoiding addition of any preceding text
                        date_label = data[col]['0']

                    # Assign value if present, ensuring not to append section names for json6
                    value = data[col].get(str(index))
                    if value:
                        if label not in nested_json[current_section]:
                            nested_json[current_section][label] = {}
                        nested_json[current_section][label][date_label] = value

    # Specifically for json6, removing any mistakenly included section names
    if any("ASSETS:" in key for key in nested_json.get("ASSETS:", {})):
        assets_section = nested_json.get("ASSETS:", {})
        for item, dates in assets_section.items():
            new_dates = {date.replace("ASSETS:", "").strip(): value for date, value in dates.items()}
            assets_section[item] = new_dates
        nested_json["ASSETS:"] = assets_section

    return nested_json

def extract_specific_table(number, dfs):
    dfs[number].dropna(axis=1, inplace=True, thresh=4)
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
    # delete_everything_in_folder("./temp")
