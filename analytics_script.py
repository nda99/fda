import requests
import json
import zipfile
import urllib.request
import pandas as pd
from os import listdir

endpoint_path = "/Users/kfgp499/Documents/endpoint_data.json"
prefix = "/Users/kfgp499/Documents/fda_data/part_"
with open(endpoint_path, "r") as input_file:
    input_dict = json.load(input_file)

data_df = pd.DataFrame(input_dict["results"]["drug"]["event"]["partitions"])

i = 0
for url in data_df["file"]:
    print("Downloading: " + str(i))
    part_name = prefix + str(i) + ".json"
    urllib.request.urlretrieve(url, part_name)
    i = i + 1

i = 0
for file in [f for f in listdir("/Users/kfgp499/Documents/fda_data/")]:
    print(i)
    if file != ".DS_Store":
        archive = zipfile.ZipFile("/Users/kfgp499/Documents/fda_data/" + file, "r")
        subset_dict = json.load(archive.open(archive.namelist()[0]))["results"]

        ids = [
            [
                {
                    "rep_id": this["safetyreportid"]
                    if "safetyreportid" in this
                    else None,
                    "primary_source_country": this["primarysourcecountry"]
                    if "primarysourcecountry" in this
                    else None,
                    "occur_country": this["occurcountry"]
                    if "occurcountry" in this
                    else None,
                    "patient_onset_age": this["patient"]["patientonsetage"]
                    if "patientonsetage" in this["patient"]
                    else None,
                    "patient_weight": this["patient"]["patientweight"]
                    if "patientweight" in this["patient"]
                    else None,
                    "patient_sex": this["patient"]["patientsex"]
                    if "patientsex" in this["patient"]
                    else None,
                    "drug_indication": drug["drugindication"]
                    if "drugindication" in drug
                    else None,
                    "drug_name": drug["medicinalproduct"]
                    if "medicinalproduct" in drug
                    else None,
                    "active_substance": drug["activesubstance"]["activesubstancename"]
                    if "activesubstance" in drug
                    else None,
                    "receipt_date": this["receiptdate"]
                    if "receiptdate" in this
                    else None,
                }
                for drug in this["patient"]["drug"]
            ]
            for this in subset_dict
        ]

        flat_list = [item for sublist in ids for item in sublist]

        id_df = pd.DataFrame(flat_list)
        id_df.to_csv(
            "/Users/kfgp499/Documents/fda_data_reaction/partition_" + str(i) + ".csv",
            index=False,
        )
    i = i + 1
