import csv, json

from phase_01_01_load_CSV import find_csv_file_dir

def convert_csv_to_dict():
    """
    param file_path: generator of csv file path
    return: list of dict of csv file path
    """
    insert_dict_file = []
    # get csv-file generator
    csv_generator = find_csv_file_dir()
    count = 0
    for csv_data in csv_generator:
        with open(file=csv_data, mode="r", encoding="utf-8-sig") as csv_file:
            # convert each csv file into dictionary file
            dict_file = csv.DictReader(csv_file)
            for row in dict_file:
                clean_row = {key: value.strip() for key, value in row.items()}
                json_row = json.dumps(clean_row).encode("utf-8")
                insert_dict_file.append(json_row)
                count += 1
    # print(count)
    return insert_dict_file

# convert_csv_to_dict()