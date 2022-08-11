## CALCULATE DESCRIPTIVE DATA FOR FINAL HOSPITAL CLAIMS SAMPLE TO CREATE EXHIBIT2

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime
from dask.distributed import Client
# client = Client("10.50.86.251:58343")
pd.set_option('display.max_columns', 500)

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/new/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/exhibits/infection/FINAL/'

claims_type = ["primary", "secondary"]
outcome = ["UTI", "PNEU"]

for ctype in claims_type:
    print(ctype)
    ## read in final analytical sample data
    df = pd.read_csv(inputPath + '{}UTI.csv'.format(ctype), low_memory=False) ## substite UTI for PNEU to run the same code for pneumonia
    ## define categorical columns
    ccol = ['race_name']
    ## define numeric columns
    ncol = ['age', 'female', 'dual', 'disability', 'combinedscore', 'count_cc']
    ## define chronic conditions columns
    cc_col = [l for l in list(df.columns) if l.endswith('_final')]

    ## calculate the total number of chronic conditions for each patient
    df['count_cc'] = df.apply(lambda x: x[cc_col].sum(), axis=1)

    # calculate sample size by short- vs. long-stay and race
    print(df.groupby('short_stay')['BENE_ID'].count())
    df.groupby(['short_stay', 'race_name'])['BENE_ID'].count().reindex([
        (True, 'white'), (True, 'black'), (True, 'hispanic'), (True, 'asian'), (True, 'american_indian'), (True, 'other'),
        (False, 'white'), (False, 'black'), (False, 'hispanic'), (False, 'asian'), (False, 'american_indian'), (False, 'other')]).\
        to_csv(
                writePath + 'FINAL/{}UTIbyRACE.csv'.format(ctype)
                )

    ## calculate the grand mean of numeric variables by short- vs. long-stay
    for col in ncol:
        print(df.groupby('short_stay')[col].mean())




