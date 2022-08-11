import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime
from dask.distributed import Client
client = Client("10.50.86.251:34744")
pd.set_option('display.max_columns', 500)

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/exhibits/infection/'

claims_type = ["primary", "secondary"]
outcome = ["UTI", "PNEU"]

for ctype in claims_type:
    ## read in final dataset
    df = pd.read_csv(inputPath + '{}PNEU.csv'.format(ctype), low_memory=False)

    ## define categorical columns
    ccol = ['race_name']
    ## define numeric columns
    ncol = ['age', 'female', 'dual', 'disability', 'combinedscore', 'count_cc']
    ## define chronic conditions columns
    cc_col = [l for l in list(df.columns) if l.endswith('_final')]

    # ## combine american indian and other for cell reporting rules
    # df = df.replace({'race_name': {"american indian": "other"}})

    ## calculate the count of chronic conditions for each patient
    df['count_cc'] = df.apply(lambda x: x[cc_col].sum(), axis=1)

    ## calculate sample size by short- vs. long-stay and highest pressure ulcer
    print(df.groupby('short_stay')['BENE_ID'].count())
    df.groupby(['short_stay', 'race_name'])['BENE_ID'].count().sort_values(ascending=False).to_csv(
        writePath + '{}PNEUbyRACE.csv'.format(ctype)
    )


    ## calculate the grand mean of numeric variables by short- vs. long-stay
    for col in ncol:
        print(df.groupby('short_stay')[col].mean())
