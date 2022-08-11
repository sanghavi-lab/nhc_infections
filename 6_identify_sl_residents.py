## THIS SCRIPT IDENTIFY SHORT-STAY VS LONG-STAY RESIDENTS BY FINDING A 5-DAY PPS MDS ASSESSMENT
## WITHIN 100 DAYS PRIOR TO NURSING HOME DISCHARGE/ HOSPITAL ADMISSION

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:39748")

mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
mergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SAMENH_CC/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "secondary"]
outcome = ["UTI", "PNEU"]

## create a function to identify long-stay vs short-stay residents and write the new data to parquet
def identify_sl_residents(analysisDF, mds, output_path=None): ## analysisDF is the analytical sample data
    ## merge MDS with analytical sample
    merge = analysisDF.merge(mds, on='BENE_ID', how='left', suffixes=['', '_sl'])
    ## select MDS assessments within 100 days before the discharge MDS assessment
    merge100days = merge[((merge['TRGT_DT'] - merge['TRGT_DT_sl']).dt.days < 101) &
                          ((merge['TRGT_DT'] - merge['TRGT_DT_sl']).dt.days >= 0)]
    ##create a binary indicator for the existence of a 5-day PPS assessment
    merge100days['short_stay'] = merge100days['A0310B_PPS_CD_sl']=='01'
    ## each claim, if it was merged with a 5-day PPS assessment, it belongs to a short-stay nursing home resident
    short_stay = merge100days.groupby('MEDPAR_ID')['short_stay'].max().reset_index()
    ## merge aggregate short_stay indicator to analytical sample data
    analysisDF = analysisDF.merge(short_stay, on='MEDPAR_ID', how='left')
    ## write to parquet
    analysisDF.to_parquet(output_path)

## prepare MDS data to merge with hospital infection claims
for year in years:

    for health_outcome in outcome:
        ## prepare analytical data for use

        ## aggregate the secondary and the second
        primarydf = dd.read_parquet(mergePath + '{0}{1}/{2}/'.format('primary', health_outcome, year))
        second = dd.read_parquet(mergePath + '{0}{1}/{2}/'.format('second', health_outcome, year))
        secondary = dd.read_parquet(mergePath + '{0}{1}/{2}/'.format('secondary', health_outcome, year))
        secondarydf = dd.concat([second, secondary])
        ## read MDS in the current year and the previous year except for 2011;
        if year==2011:
            mds = dd.read_parquet(mdsPath.format(year))
            date = datetime.datetime(2011, 1, 1) + datetime.timedelta(days=100)
            ## remove the first 100 days of data in 2011 because we need to look back 100 days to determine if the resident is short or long-stay
            primarydf = primarydf[primarydf['TRGT_DT'] > date]
            secondarydf = secondarydf[secondarydf['TRGT_DT'] > date]
            mdsyear = mds ## for later adding more UTI related MDS items
        if (year<=2015) & (year>2011):
            ## read in two years of MDS
            mdsyear = dd.read_parquet(mdsPath.format(year))
            mdsyearprior = dd.read_parquet(mdsPath.format(year-1))
            mds = dd.concat([mdsyear, mdsyearprior])
        if year>2015:
            mdsyear = dd.read_parquet(mdsPath.format(year))
            mdsyearprior = dd.read_parquet(mdsPath.format(year-1))

            mdsyear.columns = [col.upper() for col in mdsyear.columns]
            mdsyearprior.columns = [col.upper() for col in mdsyearprior.columns]
            ## concat two years of MDS
            mds = dd.concat([mdsyear, mdsyearprior])
    #
        ## define MDS items to use
        use_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'A0310B_PPS_CD']
        ## exclude mds with missing BENE_ID
        mds = mds[~mds.BENE_ID.isna()]
        ## replace special characters
        mds = mds.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
        ## select columns to use
        mds_use = mds[use_cols]
        ## change data type
        mds_use = mds_use.astype({'TRGT_DT': 'str',
                                  'A0310B_PPS_CD': 'str'})
        ## change date columns to datetime format
        mds_use['TRGT_DT'] = dd.to_datetime(mds_use['TRGT_DT'], infer_datetime_format=True)
        ## apply the function to identify long-stay vs short-stay residents
        identify_sl_residents(primarydf, mds_use, writePath + "{0}{1}/{2}".format("primary", health_outcome, year))
        identify_sl_residents(secondarydf, mds_use, writePath + "{0}{1}/{2}".format("secondary", health_outcome, year))

        ## add MDS H0100A for indwelling catheter and
        ## I8000 for diagnosis code in MDS using unique MDS identifier MDS_ASMT_ID
        p_sl = dd.read_parquet(writePath + "{0}{1}/{2}".format("primary", health_outcome, year))
        s_sl = dd.read_parquet(writePath + "{0}{1}/{2}".format("secondary", health_outcome, year))
        mdsyear = mdsyear.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
        p_sl = p_sl.merge(mdsyear[['MDS_ASMT_ID', 'H0100A_INDWLG_CTHTR_CD', 'I8000A_ICD_1_CD', 'I8000B_ICD_2_CD', 'I8000C_ICD_3_CD', 'I8000D_ICD_4_CD', 'I8000E_ICD_5_CD', 'I8000F_ICD_6_CD', 'I8000G_ICD_7_CD', 'I8000H_ICD_8_CD', 'I8000I_ICD_9_CD', 'I8000J_ICD_10_CD']],
                          on='MDS_ASMT_ID')
        s_sl = s_sl.merge(mdsyear[['MDS_ASMT_ID', 'H0100A_INDWLG_CTHTR_CD', 'I8000A_ICD_1_CD', 'I8000B_ICD_2_CD', 'I8000C_ICD_3_CD', 'I8000D_ICD_4_CD', 'I8000E_ICD_5_CD', 'I8000F_ICD_6_CD', 'I8000G_ICD_7_CD', 'I8000H_ICD_8_CD', 'I8000I_ICD_9_CD', 'I8000J_ICD_10_CD']],
                          on='MDS_ASMT_ID')
        ## clean I8000 code
        for col in ['I8000A_ICD_1_CD', 'I8000B_ICD_2_CD', 'I8000C_ICD_3_CD', 'I8000D_ICD_4_CD', 'I8000E_ICD_5_CD', 'I8000F_ICD_6_CD', 'I8000G_ICD_7_CD', 'I8000H_ICD_8_CD', 'I8000I_ICD_9_CD', 'I8000J_ICD_10_CD']:
            p_sl[col] = p_sl[col].str.replace('^', '')
            s_sl[col] = s_sl[col].str.replace('^', '')
            p_sl[col] = p_sl[col].str.replace('.', '')
            s_sl[col] = s_sl[col].str.replace('.', '')
            p_sl[col] = p_sl[col].astype('str')
            s_sl[col] = s_sl[col].astype('str')
        ## write to csv
        p_sl.to_parquet(writePath + "{0}{1}/{2}".format("primary", health_outcome, year))
        s_sl.to_parquet(writePath + "{0}{1}/{2}".format("secondary", health_outcome, year))

