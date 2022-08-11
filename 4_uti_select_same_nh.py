## SELECT RESIDENTS WHO RETURNED TO THE SAME NURSING HOME AFTER HOSPITAL DISCHARGE WITHIN 1 DAY
## This script is used for UTI hospital claims only


import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:49727")



mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
mergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SAMENH/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]

for year in years:
    ## Clean MDS
    ## define MDS items to use
    use_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'A0310E_FIRST_SINCE_ADMSN_CD', 'A0310F_ENTRY_DSCHRG_CD']
    ## read in mds
    mds = dd.read_parquet(mdsPath.format(year), usecols=use_cols)
    ## exclude mds with missing BENE_ID
    mds = mds[~mds.BENE_ID.isna()]
    ## turn all columns to upper case
    cols = [col.upper() for col in mds.columns]
    mds.columns = cols
    mds = mds[use_cols]
    ## replace special characters
    mds = mds.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
    ## change data type
    mds = mds.astype({'TRGT_DT': 'str',
                      'A0310F_ENTRY_DSCHRG_CD': 'str',
                      'A0310E_FIRST_SINCE_ADMSN_CD': 'str'})
    ## change date columns to datetime format
    mds['TRGT_DT'] = dd.to_datetime(mds['TRGT_DT'], infer_datetime_format=True)
    ## subset to the entry tracking record
    mds_first = mds[mds['A0310F_ENTRY_DSCHRG_CD']=='01']
    ## concate nh provider id with state code to create fac_state_id - unique nursing home identifier
    mds_first['FAC_PRVDR_INTRNL_ID'] = mds_first['FAC_PRVDR_INTRNL_ID'].astype('float').astype('Int64')
    mds_first = mds_first.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
    mds_first['FAC_PRVDR_INTRNL_ID'] = mds_first['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
    mds_first['fac_state_id'] = mds_first['FAC_PRVDR_INTRNL_ID'] + mds_first['STATE_CD']

    for ctype in claims_type:
        print(year, ctype)
        health_outcome="UTI"
        ## read in claims merged with discharge assessments (one-on-one merge)
        Cdischrg = dd.read_parquet(
            mergePath + 'UNIQUE/{0}{1}/{2}/'.format(ctype, "UTI", year)
        )
        ## concate nursing home provider id with state code to create unique provider id from discharge assessment
        Cdischrg['FAC_PRVDR_INTRNL_ID'] = Cdischrg['FAC_PRVDR_INTRNL_ID'].astype('float').astype('Int64')

        Cdischrg = Cdischrg.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
        Cdischrg['FAC_PRVDR_INTRNL_ID'] = Cdischrg['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
        Cdischrg['fac_state_id'] = Cdischrg['FAC_PRVDR_INTRNL_ID'] + Cdischrg['STATE_CD']
        ## merge with mds entry-tracking record
        Cdischrg_mds = Cdischrg.merge(mds_first, on='BENE_ID', how='left', suffixes=['', '_reenter'])
        ## calculate the number of days between the target date of entry-tracking record and hospital discharge date
        Cdischrg_mds = Cdischrg_mds.astype({'DSCHRG_DT': 'datetime64[ns]'})
        Cdischrg_mds['days_reenter'] = (Cdischrg_mds['TRGT_DT_reenter'] - Cdischrg_mds['DSCHRG_DT']).dt.days
        ## select assessments performed within 1 day after hospital discharge
        Cdischrg_mds_after = Cdischrg_mds[(Cdischrg_mds['days_reenter'] >= 0) & (Cdischrg_mds['days_reenter'] < 2)]
        ## select the earliest MDS assessment merged with claims after hospital admission
        closest_mds = Cdischrg_mds_after.groupby('MEDPAR_ID')['days_reenter'].min().reset_index()
        Cdischrg_mds_closest = Cdischrg_mds_after.merge(closest_mds, on=['MEDPAR_ID', 'days_reenter'], how='left')

        ## make sure all mds newly merged with claims are from the same nursing home
        multiprvdr = Cdischrg_mds_closest.groupby('MEDPAR_ID')['fac_state_id'].nunique().reset_index()
        nmultiprvdr = multiprvdr[multiprvdr['fac_state_id']!=1].shape[0].compute()
        if nmultiprvdr==0:
            Cdischrg_mds_unique = Cdischrg_mds_closest.drop_duplicates(subset='MEDPAR_ID')
        else:
            print('nursing homes merged are not unique')
            print(nmultiprvdr)
            break

        ## select residents who returned to the same nursing home
        samenh = Cdischrg_mds_unique[Cdischrg_mds_unique['fac_state_id'] == Cdischrg_mds_unique['fac_state_id_reenter']]
        print(samenh.shape[0].compute())
       ## remove columns newly merged from re-enter MDS assessment
        samenh = samenh.drop(columns=[col for col in samenh.columns if col.endswith('_reenter')])
        ## write data to parquet
        samenh.to_parquet(
            writePath + '{0}{1}/{2}'.format(ctype, health_outcome, year)
        )







