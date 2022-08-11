## this code is to create final analytical sample data for SNF UTI claims
## 1) remove missing values
## 2) keep only merged SNF and MDS from the same nursing home
## 3) create resident-level coariates


import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:44798")

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/SMDS/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/SMDS/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ['primary', 'secondary']

for year in years:
    ## read in raw MedPAR SNF data to add the facility id for nursing homes
    snf = dd.read_parquet('/gpfs/data/cms-share/data/medicare/{}/medpar/parquet'.format(year))
    snf = snf.reset_index()
    snf = snf[['MEDPAR_ID', 'ORG_NPI_NUM']]
    ## read in MDS raw data to add the facility id for nursing homes
    mds = dd.read_parquet('/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'.format(year))
    mds.columns = [col.upper() for col in mds.columns]
    mds = mds[['MDS_ASMT_ID', 'A0100A_NPI_NUM']]
    ## aggregate the primary and the second SNF denominator file to be primary SNF denominator file
    dfp = dd.read_parquet(inputPath + '{0}UTI/{1}'.format('primary', year))
    dfs = dd.read_parquet(inputPath + '{0}UTI/{1}'.format('second', year))
    df = dd.concat([dfp, dfs])
    ## select claims merged with an MDS assessment within 30 days of admitting to a nursing home
    df = df[df['within_30_admission']==1]
    ## remove laste three-month of data in 2015
    df = df.astype({'ADMSN_DT': 'datetime64[ns]'})
    df = df[(df['ADMSN_DT']<datetime(2015, 10, 1)) | (df['ADMSN_DT']>=datetime(2016, 1, 1))]

    ## MERGE ORG_NPI_NUM COLUMN
    df_merged = df.merge(snf, on='MEDPAR_ID', how='left')
    ## MERGE A0100A_NPI_NUM column
    df_merged = df_merged.merge(mds, on='MDS_ASMT_ID', how='left')
    df_merged = df_merged.astype({'ORG_NPI_NUM': 'str',
                                  'A0100A_NPI_NUM': 'str'})
    ## make sure the nursing home completing MDS is the same nursing home submitting SNF claims
    df_matched = df_merged[df_merged['A0100A_NPI_NUM']==df_merged['ORG_NPI_NUM']]

    ## futher clean data
    ## remove missing race
    df_matched = df_matched[df_matched['race_RTI'] != '0']
    replace_race = {'1': 'white', '2': 'black', '3': 'other', '4': 'asian', '5': 'hispanic', '6': 'american_indian'}

    df_matched = df_matched.assign(race_name=df_matched['race_RTI'].replace(replace_race))

    ## create individual-level variables
    # create dual indicator;
    # a patient is considered a dual if he/she is a full dual in any month of hospital admission year for infection;
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]

    ## dual is True if the patient is a full dual in any month of the hospitalization year
    df_matched['dual'] = df_matched[dual].isin(['02', '04', '08']).any(axis=1)

    ## create female and disability indicators
    df_matched['female'] = df_matched['sex'] == '2'
    df_matched['disability'] = df_matched['ENTLMT_RSN_CURR'] == '1'
    ## all residents with SNF claims are short-stay residents because Medicare only covers the first 100-day stay at nursing home
    df_matched['short_stay'] = True

    # ## select columns
    # cols_use = ['BENE_ID', 'MEDPAR_ID', 'MDS_ASMT_ID', 'MEDPAR_YR_NUM', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'race_name', 'female', 'age',
    #             'dual', 'disability', 'short_stay', 'I2300_UTI_CD', 'days_dischrg']

    df_matched.to_parquet(
        writePath + '{0}UTI/{1}'.format('primary', year)
    )




