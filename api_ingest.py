import requests
import json
from datetime import datetime
import os

# Establish API base url and params
# TODO: Make API call dynamic. Use .yml configuration to feed into script?
cat_facts_api = "https://cat-fact.herokuapp.com/facts/random"
cat_facts_api_params = {"animal_type": "cat", "amount": 10}

# Make request call to get JSON
results = requests.get(cat_facts_api, cat_facts_api_params)
print(f"API return code: {results.status_code}")
print("Printing first 1000 characters:")
print(results.text[0:1000])

if results.status_code == 200:
    print("API request successful. Dumping data into /tmp directory")
    try:
        os.makedirs("./tmp")
    except:
        print("./tmp already exists, proceeding with write")
        pass
else:
    error_msg = f"API request unsuccessful - received code {results.status_code}"
    raise Exception(error_msg)

# Get current datetime to be used to timestamp .json file
now_string = datetime.now().strftime("%Y%m%d_%H%M%S")
file_name = "./tmp/" + now_string + ".json"

with open(file_name, 'w') as f:
    json.dump(results.json(), f)

try:
    print("Creating dbfs directory")
    dbutils.fs.mkdirs("/dbfs/catfacts_express/raw")
except:
    pass

for filename in os.listdir("./tmp"):
    file_path = os.path.join("./tmp", filename)
    try:
        print("Copying " + file_path + " to DBFS")
        dbutils.fs.cp("file:///databricks/driver/tmp/" + filename, "/dbfs/catfacts_express/raw")
        print(dbutils.fs.ls("/dbfs/catfacts_express/raw"))

        print("Removing file " + file_path)
        os.unlink(file_path)
    except Exception as e:
        print(e)
