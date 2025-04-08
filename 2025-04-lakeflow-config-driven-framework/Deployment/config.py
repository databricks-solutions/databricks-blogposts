import json

def load_config(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config