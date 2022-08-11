## This script merge MBSF Base file with infection claims for beneficiaries' characteristics and
## subset the sample to only fee-for-service beneficiaries
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:57018")

mbsfPath = '/gpfs/data/cms-share/data/medicare/{}/mbsf/mbsf_abcd/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/MBSF/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'

years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]
outcome = ["UTI", "PNEU"]


# <editor-fold desc="MERGE WITH MBSF">
# ### MERGE WITH MBSF #########################################################################
for year in years:

    ## identify columns to use in MBSF
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    col_use = ['BENE_ID', 'BENE_DEATH_DT', 'ESRD_IND', 'AGE_AT_END_REF_YR',
               'RTI_RACE_CD', 'BENE_ENROLLMT_REF_YR', 'SEX_IDENT_CD',
               'ENTLMT_RSN_ORIG', 'ENTLMT_RSN_CURR'] + dual + hmo
    ## read in MBSF
    df_MBSF = dd.read_parquet(mbsfPath.format(year))
    df_MBSF = df_MBSF.reset_index()
    df_MBSF = df_MBSF[col_use]

    ## subset to benes who were ffs benes for a whole year
    df_MBSF = df_MBSF[df_MBSF[hmo].isin(['0', '4']).all(axis=1)]

    ## rename columns
    df_MBSF = df_MBSF.rename(columns={'BENE_DEATH_DT': 'death_dt',
                                      'AGE_AT_END_REF_YR': 'age',
                                      'RTI_RACE_CD': 'race_RTI',
                                      'SEX_IDENT_CD': 'sex'})

    for health_outcome in outcome:
        for ctype in claims_type:
            ## read in medpar UTI and Pneumonia claims
            df_claims = dd.read_parquet(
                inputPath + '{0}{1}/{2}'.format(
                    ctype, health_outcome, year)
            )

            ## merge with MBSF
            df_claims_mbsf = df_claims.merge(df_MBSF, on="BENE_ID", how="inner")
            ## write merged data to parquet
            df_claims_mbsf.to_parquet(
                writePath + "{0}{1}_MBSF/{2}/".format(
                    ctype, health_outcome, year)
            )
# ##############################################################################################
# # </editor-fold>
