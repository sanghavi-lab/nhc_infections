## THIS SCRIPT MERGE MBSF BASE FILE TO SNF CLAIMS FOR BENEFICIARIES' CHARACTERISTICS AND
## TO SUBSET TO FEE-FOR-SERVICE BENEFICIARIES ONLY

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:40823")

mbsfPath = '/gpfs/data/cms-share/data/medicare/{}/mbsf/mbsf_abcd/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/MBSF/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'

years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]
outcome = ["UTI", "PNEU"]

### MERGE WITH MBSF #########################################################################
for year in years:
    ## identify columns to use in MBSF
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    col_use = ['BENE_ID', 'BENE_DEATH_DT', 'ESRD_IND', 'AGE_AT_END_REF_YR',
               'RTI_RACE_CD', 'BENE_ENROLLMT_REF_YR', 'SEX_IDENT_CD',
               'ENTLMT_RSN_ORIG', 'ENTLMT_RSN_CURR'] + dual + hmo
    ## read in MBSF
    df_MBSF = dd.read_parquet(mbsfPath.format(year))

    ## select FFS benes for a whole year
    df_MBSF = df_MBSF[df_MBSF[hmo].isin(['0', '4']).all(axis=1)]

    ## rename columns
    df_MBSF = df_MBSF.rename(columns={'BENE_DEATH_DT': 'death_dt',
                                      'AGE_AT_END_REF_YR': 'age',
                                      'RTI_RACE_CD': 'race_RTI',
                                      'SEX_IDENT_CD': 'sex'})

    for health_outcome in outcome:
        for ctype in claims_type:
            print(year, health_outcome, ctype)
            ## read in medpar UTI and Pneumonia claims
            df_claims = dd.read_parquet(
                inputPath + 'SNF{0}{1}_indexed/{2}'.format(
                    ctype, health_outcome, year)
            )
            # merge with MBSF
            df_claims_mbsf = df_claims.merge(df_MBSF, left_index=True, right_index=True, how="inner")
            ## write merged data to parquet
            df_claims_mbsf.to_parquet(
                writePath + "SNF{0}{1}_MBSF/{2}/".format(
                    ctype, health_outcome, year)
            )


