## merge UTI and Pneumonia hospital claims with MDS
## keep only hospital claims linked to a discharge MDS within 1 day prior to hospital admission

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:57018")

claimsPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/MBSF/'
mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]
outcome = ["UTI", "PNEU"]

# # # <editor-fold desc="MERGE WITH MDS AND SELECT CLAIMS MERGED WITH DISCHARGE ASSESSMENTS WITHIN 1 DAY BEFORE HOSPITALIZATION">
## This step is to identify residents discharged from nursing home and admitted to a hospital within 1 day
for year in years:
    ## define MDS items to use
    infection_cols = ['I2300_UTI_CD', 'I2000_PNEUMO_CD']
    other_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'A0310A_FED_OBRA_CD',
                  'A0310B_PPS_CD', 'A0310C_PPS_OMRA_CD', 'A0310D_SB_CLNCL_CHG_CD', 'A0310E_FIRST_SINCE_ADMSN_CD',
                  'A0310F_ENTRY_DSCHRG_CD', 'A1600_ENTRY_DT', 'A1700_ENTRY_TYPE_CD', 'A1800_ENTRD_FROM_TXT',
                  'A1900_ADMSN_DT', 'A2000_DSCHRG_DT', 'A2100_DSCHRG_STUS_CD', 'A2300_ASMT_RFRNC_DT']
    ## read in mds
    mds = dd.read_parquet(mdsPath.format(year))
    ## exclude mds with missing BENE_ID
    mds = mds[~mds.BENE_ID.isna()]
    ## turn all columns to upper case
    cols = [col.upper() for col in mds.columns]
    mds.columns = cols
    ## replace special characters
    mds = mds.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
    ## select columns to use
    mds_use = mds[other_cols + infection_cols]
    ## change data type
    mds_use = mds_use.astype({'A2000_DSCHRG_DT': 'datetime64[ns]',
                              'A1600_ENTRY_DT': 'string',
                              'TRGT_DT': 'string'})
    ## change date columns to datetime format
    mds_use['A1600_ENTRY_DT'] = dd.to_datetime(mds_use['A1600_ENTRY_DT'], infer_datetime_format=True)
    mds_use['TRGT_DT'] = dd.to_datetime(mds_use['TRGT_DT'], infer_datetime_format=True)
    ## finish cleaning MDS
    del mds
    for ctype in claims_type:
        for health_outcome in outcome:
            ## read in hospital claims data
            claims = dd.read_parquet(claimsPath + '{0}{1}_MBSF/{2}/'.format(
                ctype, health_outcome, year
            ))
            ## merge with MDS assessments by bene_id
            merge_mds = claims.merge(mds_use, on='BENE_ID', how="left")

            merge_mds = merge_mds.astype({'ADMSN_DT': 'datetime64[ns]',
                                          'A0310F_ENTRY_DSCHRG_CD': 'str',
                                          'A2100_DSCHRG_STUS_CD': 'str'})
            ## calculate the number of days between hospital admission date and MDS target date
            merge_mds['days'] = (merge_mds['ADMSN_DT'] - merge_mds['TRGT_DT']).dt.days
            ## select claims merged with discharge assessment
            merge_mds_dischr = merge_mds[merge_mds['A0310F_ENTRY_DSCHRG_CD'].isin(["10", "11"])]
            del merge_mds
            ## select claims merged with a discharge MDS assessments within 1 day before hospital admission
            ## meaning the nursing home resident was hospitalized within 1 day of nursing home discharge
            merge_mds_dischr1day = merge_mds_dischr[(merge_mds_dischr['days']<=1) &
                                                    (merge_mds_dischr['days']>=0)]
            del merge_mds_dischr
            # ## select claims merged with a discharge MDS assessment where the discharge destination is a hospital
            merge_mds_dischr1day_tohospital = merge_mds_dischr1day[merge_mds_dischr1day['A2100_DSCHRG_STUS_CD'].isin(["03", "09"])]
            merge_mds_dischr1day_tohospital.to_parquet(
                writePath + '{0}{1}/{2}/'.format(ctype, health_outcome, year)
            )
# # </editor-fold>

# ### for flow chart ###############################################
for year in years:
    for ctype in claims_type:
        for health_outcome in outcome:
            df = dd.read_parquet(writePath + '{0}{1}/{2}/'.format(ctype, health_outcome, year))
            claims_count_dischrg1day[ctype][health_outcome].append(
                df.MEDPAR_ID.nunique().compute()
            )
pd.DataFrame(claims_count_dischrg1day).applymap(sum).to_csv(
    analysisPath + 'flowchart_countNHresDischrg1day.csv'
)

##############################################################################################
##############################################################################################

# # ### <editor-fold desc="FOR UTI CLAIMS, SELECT UNIQUE MDS DISCHARGE ASSESSMENT MERGED WITH CLAIMS">
## MDS discharge assessment requires the reporting of UTI, so we keep the closest MDS discharge assessments merged with the hospital claim;
## If multiple MDS with the same target date were merged with the same claim, we count the UTI as being reported if the UTI item was reported on any MDS
for year in years:
    for ctype in claims_type:
        print(year, ctype, "UTI")
        ## read in claims merged with discharge assessments from last step
        df = dd.read_parquet(
            writePath + '{0}{1}/{2}/'.format(ctype, "UTI", year)
        )

        ## some claims are merged with multiple discharge assessments within 1 day: keep the assessment closest to hospital admission date
        closest = df.groupby('MEDPAR_ID')['days'].min().reset_index()
        DFclosest = df.merge(closest, on=['MEDPAR_ID', 'days'])
        # print(DFclosest.shape[0].compute())

        ## if claims are merged with multiple assessments which have the same discharge(the same as target date) date;
        # combine the information of UTI in those assessments, and keep only one record
        # to ensure there are only one-to-one matching between claims and assessments
        ## fewer than 0.2% claims were matched with multiple discharge MDS with the same target date

        # remove MDS if UTI items are coded as "not applicable"
        DFclosest = DFclosest[DFclosest['I2300_UTI_CD'] != 'not_applicable']
        # print(DFclosest.shape[0].compute())
        DFclosest = DFclosest.astype({'I2300_UTI_CD': 'int'})
        ## we count the UTI as being reported if any of the matched discharge MDS reported UTI
        combineDischrg = DFclosest.groupby('MEDPAR_ID').agg({'I2300_UTI_CD': 'max'}).reset_index()
        ## keep unique one-to-one claim-MDS matching
        DFunique = DFclosest.drop_duplicates(subset='MEDPAR_ID')
        DFunique = DFunique.drop(columns=['I2300_UTI_CD', 'I2000_PNEUMO_CD'])
        ## merge the aggregated info on I2300_UTI_CD back to the main table
        DFfinal = DFunique.merge(combineDischrg, on='MEDPAR_ID', how='left')
        # print(DFfinal.shape[0].compute())

        DFfinal.repartition(npartitions=10).to_parquet(
            writePath + 'UNIQUE/{0}{1}/{2}/'.format(ctype, "UTI", year)
        )
# # ## </editor-fold>


















