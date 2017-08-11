import pandas as pd

import json
import os
import glob
import argparse


def load_filenames(data_path, ext='.json'):
    """load files per an associated extension"""
    return glob.glob(os.path.join(data_path, "".join(["*",ext]) ))

def acquire_data(fname):
    """acquire session data based on list of session keys"""
    with open(fname, 'r') as f:
        data_json = json.load(f)
        key_ids = list(data_json.keys())
        num_entries  = len(key_ids)
        session_data = data_json[key_ids[-1]]
    return session_data

def write_data(fname, data):
    """write json data to csv format"""
    with open(fname, 'w') as f:
        # convert to csv format
        df = pd.DataFrame(data)
        df.to_csv(fname, index=False)



if __name__ == "__main__":
    # script level arguments
    def_path  = os.path.join(os.path.join("..", 'data'))
    parser = argparse.ArgumentParser(description="csv to json conversion")
    parser.add_argument('-p', '--path', default=def_path, nargs='?', help='associated bucketpath')
    args = parser.parse_args()

    # load json files
    json_path = os.path.join(args.path, 'json')
    fnames = load_filenames(json_path)
    data_json = acquire_data(fnames[-1])
    print("Num Records: {}".format( len(data_json) ) )
    # write to csv format
    csv_path  = os.path.join(args.path, 'csv')
    base_file = os.path.basename(fnames[-1])
    base_text = os.path.splitext(base_file)
    csv_file  = "".join([base_text[0], '.csv'])
    csv_file  = os.path.join(csv_path, csv_file)
    write_data(csv_file, data_json)
    print("File Output to: {}".format(csv_file))
