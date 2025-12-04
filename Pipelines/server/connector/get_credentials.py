import os
from pathlib import Path
import yaml

def get_postgres_creds():
    if os.environ.get('PROD_SERVER_FLAG', None) is None:
        cred_file_name = 'local_config.yaml'
    else:
        cred_file_name = 'config.yaml'
    with open(
            os.path.join(Path().absolute().parent, cred_file_name)
            , 'r'
    ) as file:
        data = yaml.safe_load(file)
    return data