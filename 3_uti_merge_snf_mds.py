## this code merge snf UTI claims with MDS,
## select MDS assessment within the snf stay covered by the claim

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:57057")

def assign_end_date(row):
    ## assign either the discharge date or the care through date as the end date of a snf stay
    row['end_date'] = row['DSCHRG_DT']

    if pd.isnull(row['DSCHRG_DT']):
        row['end_date'] = row['CVRD_LVL_CARE_THRU_DT']

    return row

mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/MBSF/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/SMDS/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]
outcome = ["UTI", "PNEU"]

for year in years:

    ## define MDS items to use
    infection_cols = ['I2300_UTI_CD', 'I2000_PNEUMO_CD']
    other_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'A0310E_FIRST_SINCE_ADMSN_CD',
                  'A0310F_ENTRY_DSCHRG_CD', 'A1600_ENTRY_DT', 'A1700_ENTRY_TYPE_CD',
                  'A1900_ADMSN_DT', 'A2000_DSCHRG_DT', 'A2100_DSCHRG_STUS_CD']
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
    ## only keep MDS with eligible UTI item to merge with SNF claims
    mds_use = mds_use[mds_use['I2300_UTI_CD']!='not_applicable']

    del mds
    for ctype in claims_type:
        ## read in SNF claims
        snf = dd.read_parquet(inputPath + 'SNF{0}UTI_MBSF/{1}/'.format(
            ctype, year))

        snf = snf.astype({'ADMSN_DT': 'datetime64[ns]',
                          'DSCHRG_DT': 'datetime64[ns]',
                          'CVRD_LVL_CARE_THRU_DT': 'datetime64[ns]'})
        snf = snf.reset_index()
        ## merge mds with snf
        merge_mds = snf.merge(mds_use, on='BENE_ID', how="left")
        ## apply the function to assign end date of each nursing home stay
        merge_mds = merge_mds.map_partitions(lambda ddf: ddf.apply(assign_end_date, axis=1))

        merge_mds = merge_mds.astype({'end_date': 'datetime64[ns]',
                                      'I2300_UTI_CD': 'float'})

        ## select mds merged within the stay of snf claims
        snf_mds_within = merge_mds[(merge_mds['TRGT_DT'] >= merge_mds['ADMSN_DT']) &
                                   (merge_mds['TRGT_DT'] <= merge_mds['end_date'])]
        ## remove entry/death record (has no UTI item in mds)
        snf_mds_within = snf_mds_within[~snf_mds_within.A0310F_ENTRY_DSCHRG_CD.isin([1, 12, "1", "12", "01"])]

        ## remove mds if it is a discharge assessment and its target date is on the same date as SNF admission date
        ## because in this case I would assume the discharge assessment is from last nursing home from which the resident is discharged
        snf_mds_within_no_entry = snf_mds_within[~((snf_mds_within.A0310F_ENTRY_DSCHRG_CD.isin([10, 11, "10", "11"])) & (snf_mds_within.TRGT_DT == snf_mds_within.ADMSN_DT))]

        ## calculate the number of SNF claims that has mds from multiple NHs within the stay
        wstay_nh = snf_mds_within_no_entry.groupby('MEDPAR_ID')['FAC_PRVDR_INTRNL_ID'].nunique().reset_index()
        wstay_nh_multiple = wstay_nh[wstay_nh['FAC_PRVDR_INTRNL_ID'] > 1]

        ## remoeve SNF claims matched with mds from multiple NHs
        ## ensure that all SNF claims are merged with only one nursing home with reporting responsibility,
        ## i.e. the same nursing home that submits the MedPAR claim
        snf_mds_within_no_entry = snf_mds_within_no_entry[~snf_mds_within_no_entry.MEDPAR_ID.isin(list(wstay_nh_multiple.MEDPAR_ID))]

        ## create indicators to suggest if the first assessment are within 30 days of admission date
        ## because UTI item on MDS has a 30-day look-back period
        snf_mds_within_no_entry['within_30_admission'] = (snf_mds_within_no_entry['TRGT_DT'] - snf_mds_within_no_entry['ADMSN_DT']).dt.days <= 30
        ## calculate the number of days between the last assessment and the end date of nursing home stay from the claim (not used)
        snf_mds_within_no_entry['days_dischrg'] = (snf_mds_within_no_entry['end_date'] - snf_mds_within_no_entry['TRGT_DT']).dt.days

        ## aggregate the information of I2300_UTI_CD for all MDS linked with the same SNF claim
        report = snf_mds_within_no_entry.groupby('MEDPAR_ID')[['I2300_UTI_CD', 'within_30_admission', 'days_dischrg']].max().reset_index()
        snf_mds_within_no_entry = snf_mds_within_no_entry.drop(columns=['I2300_UTI_CD', 'within_30_admission', 'days_dischrg'])
        ## keep unique SNF claims
        snf_mds_unique = snf_mds_within_no_entry.drop_duplicates(subset='MEDPAR_ID')
        ## merge unique SNF claims with aggregated I2300_UTI_CD item
        snf_mds_unique = snf_mds_unique.merge(report, on='MEDPAR_ID')
        ## write to parquet
        snf_mds_unique.repartition(npartitions=20).to_parquet(
            writePath + '{0}UTI/{1}'.format(ctype, year)
        )



