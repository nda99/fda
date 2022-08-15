import os
import glob
import pandas as pd

path = "/Users/kfgp499/Documents/fda_data_csv/"

all_files = glob.glob(os.path.join(path, "*.csv"))

df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

df.to_csv(path + "drug_data.csv", index=False)
