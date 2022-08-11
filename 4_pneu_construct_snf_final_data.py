## this code is to create final analytical sample data for SNF PNEU claims
## 1) remove missing values
## 2) keep only merged SNF and MDS from the same nursing home


import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:56590")

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/SMDS/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/SMDS/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ['primary', 'secondary']

## these code were simpilar to the code for UTI, which had more comments explaining each line of code
for year in years:
    snf = dd.read_parquet('/gpfs/data/cms-share/data/medicare/{}/medpar/parquet'.format(year))
    snf = snf.reset_index()
    snf = snf[['MEDPAR_ID', 'ORG_NPI_NUM']]

    mds = dd.read_parquet('/gpfs/data/sanghavi-lab/DESTROY/MDS/cross_walked/xwalk_mds_unique_new/{}/'.format(year))
    mds.columns = [col.upper() for col in mds.columns]
    mds = mds[['MDS_ASMT_ID', 'A0100A_NPI_NUM']]

    dfprimary = dd.read_parquet(inputPath + '{0}PNEU/{1}'.format('primary', year))
    dfsecond = dd.read_parquet(inputPath + '{0}PNEU/{1}'.format('second', year))
    df = dd.concat([dfprimary, dfsecond])

    ## select claims merged with an MDS assessment within 7 days of admitting to a nursing home
    df = df[df['within_7_admission']==1]
    print(df.shape[0].compute())
    ## MERGE ORG_NPI_NUM COLUMN
    df_merged = df.merge(snf, on='MEDPAR_ID', how='left')
    df_merged = df_merged.merge(mds, on='MDS_ASMT_ID', how='left')
    df_merged = df_merged.astype({'ORG_NPI_NUM': 'str',
                                  'A0100A_NPI_NUM': 'str'})
    ## subset to claims matched with MDS from the same nursing home
    df_matched = df_merged[df_merged['A0100A_NPI_NUM']==df_merged['ORG_NPI_NUM']]
    print(df_matched.shape[0].compute())

    ## further clean data
    ## remove missing race
    df_matched = df_matched[df_matched['race_RTI'] != '0']
    replace_race = {'1': 'white', '2': 'black', '3': 'other', '4': 'asian', '5': 'hispanic', '6': 'american_indian'}

    df_matched = df_matched.assign(race_name=df_matched['race_RTI'].replace(replace_race))

    ## create individual-level variables
    # create dual indicator;
    # a patient is considered a dual if he/she is a full dual in any month of hospital admission year for pneumonia;
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]

    ## dual is True if the patient is a full dual in any month of the hospitalization year
    df_matched['dual'] = df_matched[dual].isin(['02', '04', '08']).any(axis=1)

    ## create female and disability indicators
    df_matched['female'] = df_matched['sex'] == '2'
    df_matched['disability'] = df_matched['ENTLMT_RSN_CURR'] == '1'

    df_matched['short_stay'] = True
    df_matched = df_matched.reset_index()
    ## select columns
    cols_use = ['BENE_ID', 'MEDPAR_ID', 'MDS_ASMT_ID', 'MEDPAR_YR_NUM', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'race_name', 'female', 'age',
                'dual', 'disability', 'short_stay', 'I2300_PNEU_CD', 'days_dischrg']

    df_matched.to_parquet(
        writePath + 'primaryPNEU/{}'.format(year)
    )

