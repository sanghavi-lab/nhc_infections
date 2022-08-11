## MERGE LTCFocus data and CASPER data for nurisng home characteristics

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:39748")

mergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_FAC/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "secondary"]
outcome = ["UTI", "PNEU"]

## merge casper data in
### 1) use facility.csv from LTCFocus to crosswalk provider number
casper = pd.read_csv('/gpfs/data/cms-share/data/casper/April 2018/STRIP183/PART2.CSV', low_memory=False)  ## read in capser data
facility = pd.read_csv('/gpfs/data/sanghavi-lab/DESTROY/MDS/raw/mds/facility.csv')  ## read in facility data

## create new column -fac_state_id- that concatenates FACILITY_INTERNAL_ID and STATE_ID
facility = facility.astype({'FACILITY_INTERNAL_ID': 'str',
                            'STATE_ID': 'str'})
## fill FACILITY_INTERNAL_ID with leading zeros
facility['FACILITY_INTERNAL_ID'] = facility['FACILITY_INTERNAL_ID'].str.zfill(10)
## combine FACILITY_INTERNAL_ID and STATE_ID to create fac_state_id
facility['fac_state_id'] = facility['FACILITY_INTERNAL_ID'] + facility['STATE_ID']

## clean CASPER data
casper = casper.rename(columns={'PRVDR_NUM': 'MCARE_ID'})
casper['CRTFCTN_DT'] = pd.to_datetime(casper['CRTFCTN_DT'].astype(str), infer_datetime_format=True)
## create casper_year to indicate the year of the survey results
casper['casper_year'] = casper['CRTFCTN_DT'].dt.year

## keep the latest snap shot of NH characteristics for each year
casper_latest = casper.groupby(['MCARE_ID', 'casper_year'])['CRTFCTN_DT'].max().reset_index()
casper = casper.merge(casper_latest, on=['MCARE_ID', 'casper_year', 'CRTFCTN_DT'], how='inner')

## create a function to merge LTCFocus and Casper data to the denominator file
def merge_facility(analysis_data, facility, casper, sameyear_path=None, notsameyear_path=None):
    ## rename the columns due to merging post-hospitalization MDS assessments for PNEU claims
    if 'FAC_PRVDR_INTRNL_ID' not in analysis_data.columns:
        analysis_data = analysis_data.rename(columns={'FAC_PRVDR_INTRNL_ID_dischrg': 'FAC_PRVDR_INTRNL_ID'})
    if 'STATE_CD' not in analysis_data.columns:
        analysis_data = analysis_data.rename(columns={'STATE_CD_dischrg': 'STATE_CD'})
    ## create unique facility id fac_state_id by concatenating two columns
    analysis_data['FAC_PRVDR_INTRNL_ID'] = analysis_data['FAC_PRVDR_INTRNL_ID'].astype('float').astype('Int64')

    analysis_data = analysis_data.astype({'FAC_PRVDR_INTRNL_ID': 'str',
                                          'TRGT_DT': 'datetime64[ns]'})
    analysis_data['FAC_PRVDR_INTRNL_ID'] = analysis_data['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
    analysis_data['fac_state_id'] = analysis_data['FAC_PRVDR_INTRNL_ID'] + analysis_data['STATE_CD']

    ## merge facility data and denominator file on the fac_state_id column
    analysis_data = analysis_data.merge(facility[
                                              ['fac_state_id', 'MCARE_ID', 'NAME', 'ADDRESS', 'FAC_CITY', 'STATE_ID', 'FAC_ZIP', 'CATEGORY',
                                               'CLOSEDDATE']],
                                        on='fac_state_id',
                                        how='left')
    print('what is the percentage of mds-medpar records didn not match to a MCARE_ID?')
    print(analysis_data['MCARE_ID'].isna().mean().compute())


    ## 2) merge the latest NH characteristics from CASPER
    df_casper = analysis_data.merge(casper[['STATE_CD', 'MCARE_ID', 'CRTFCTN_DT', 'casper_year',
                                             'GNRL_CNTL_TYPE_CD', 'CNSUS_RSDNT_CNT', 'CNSUS_MDCD_CNT',
                                             'CNSUS_MDCR_CNT', 'CNSUS_OTHR_MDCD_MDCR_CNT']],
                                     on='MCARE_ID',
                                     how='left',
                                     suffixes=['', '_casper'])
    # check
    print('how many claims are not matched with casper records?')
    print(df_casper[df_casper.CRTFCTN_DT.isna()].MEDPAR_ID.unique().size.compute())
    ## drop the Unnamed: 0 column if it is in the data (usually casued by reading in the csv data with unnamed index)
    if sum(['Unnamed: 0' == col for col in df_casper.columns]) > 0:
        df_casper = df_casper.drop(columns=['Unnamed: 0'])

    ## drop missing values on casper_year
    df_casper = df_casper[~df_casper['casper_year'].isna()]

    ## select mds-medpar data matched with same year casper and write to parquet
    df_casper = df_casper.astype({'MEDPAR_YR_NUM': 'int',
                                  'casper_year': 'int'})
    df_casper_matched = df_casper[df_casper.MEDPAR_YR_NUM == df_casper.casper_year]
    df_casper_matched.to_parquet(sameyear_path)


    ## calculate the number of years between casper and the target date in MDS discharge assessments
    df_casper['days_casper'] = (df_casper['TRGT_DT'] - df_casper['CRTFCTN_DT']).dt.days
    df_casper['abs_days_casper'] = abs(df_casper['days_casper'])
    df_casper['years_casper'] = df_casper['days_casper'] / 365

    ## select casper within two years of the MDS discharge assessment
    df_casper_nonmatched = df_casper[~df_casper.MEDPAR_ID.isin(list(df_casper_matched.MEDPAR_ID))]
    df_casper_nonmatched = df_casper_nonmatched[((df_casper_nonmatched['years_casper'] >= 0) &
                                                 (df_casper_nonmatched['years_casper'] <= 2))]

    ## select the closest casper linked to mds-medpar denominator
    df_casper_closest_casper = \
        df_casper_nonmatched. \
            groupby('MEDPAR_ID')['abs_days_casper']. \
            min(). \
            rename('closest_casper').reset_index()
    ## get all other columns for the closest casper
    df_casper_nonmatched = df_casper_nonmatched.merge(df_casper_closest_casper[['MEDPAR_ID', 'closest_casper']],
                                                      on='MEDPAR_ID',
                                                      how='inner')
    ## for claims not matched with casper at the year of hospitalization, select the closest previous casper within 2 years
    df_casper_nonmatched = df_casper_nonmatched[
        df_casper_nonmatched.abs_days_casper == df_casper_nonmatched.closest_casper]
    ## drop unnecessary columns
    df_casper_nonmatched = df_casper_nonmatched.drop(
        columns=['days_casper', 'abs_days_casper', 'years_casper', 'closest_casper'])
    ## write to parquet
    df_casper_nonmatched.to_parquet(notsameyear_path)

## These code will by applied for pneumonia denominator file as well
## substitute UTI for PNEU and run the same function for pneumonia hospital claims sample

primary_path = ['/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/primaryUTI/2011',
                '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/primaryUTI/2012',
                '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/primaryUTI/2013',
                '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/primaryUTI/2014',
                '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/primaryUTI/2015',
                '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/primaryUTI/2016',
                '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/primaryUTI/2017']

secondary_path = ['/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/secondaryUTI/2011',
                  '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/secondaryUTI/2012',
                  '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/secondaryUTI/2013',
                  '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/secondaryUTI/2014',
                  '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/secondaryUTI/2015',
                  '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/secondaryUTI/2016',
                  '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/secondaryUTI/2017']

## read in sample data
primary = [dd.read_parquet(path) for path in primary_path]
primary_df = dd.concat(primary)

secondary = [dd.read_parquet(path) for path in secondary_path]
secondary_df = dd.concat(secondary)

## apply function to merge in LTCFocus and CASPER for UTI hospital claims sample
merge_facility(primary_df, facility, casper,
               sameyear_path=writePath + 'sameyear/primaryUTI',
               notsameyear_path=writePath + 'notsameyear/primaryUTI')

merge_facility(secondary_df, facility, casper,
               sameyear_path=writePath + 'sameyear/secondaryUTI',
               notsameyear_path=writePath + 'notsameyear/secondaryUTI')


# print(dd.read_parquet(writePath + 'sameyear/primaryUTI').shape[0].compute())
# print(dd.read_parquet(writePath + 'notsameyear/primaryUTI').shape[0].compute())


