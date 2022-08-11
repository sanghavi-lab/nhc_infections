## identify UTI and PNEUMONIA claims from MedPAR according to diagnosis code and its location, and POA indicator
## Categorize UTI and PNEUMONIA claims into primary, second and secondary claims (the second and secondary claims were combined to secondary claims sample later)

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:46810")


def identify_uti_claims(row):
    ## This function create an indicator "claim_typeUTI" flagging claims as primary UTI, second UTI, secondary UTI or not_uti claims
    ## by using all 25 diagnosis codes and their corresponding POA indicator

    ## define diagnosis code columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    ## define poa indicator columns
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1, 26))]

    uti_claims = row[dcode[2:]].isin(icd[icd['outcome']=="uti"]['icd']) ##create a list of true and false to determine if any of 23 diagnosis codes (3 - 25) is related to uti code
    poa = row[poacode[2:]].str.startswith('Y', na=False)  ##create a list of true and false to determine if the diagnosis is present-on-admission to hospital

    if (any(row['DGNS_1_CD']==code for code in icd[icd['outcome'] == "uti"]['icd'])) & (row.POA_DGNS_1_IND_CD == "Y"):
        row['claim_typeUTI'] = 'primary' ## if the primary diagnosis code is related to UTI and is present-on-admission, it is a primary UTI claim
    elif (any(row['DGNS_2_CD']==code for code in icd[icd['outcome'] == "uti"]['icd'])) & (row.POA_DGNS_2_IND_CD == "Y"):
        row['claim_typeUTI'] = 'second' ## if the second diagnosis code is related to UTI and is present-on-admission, it is a second UTI claim
    elif any([True for i,j in zip(uti_claims, poa) if i&j]):
        row['claim_typeUTI'] = 'secondary' #### if any of the remaining secondary diagnosis code is related to UTI and is present-on-admission, it is a secondary UTI claim
    else:
        row['claim_typeUTI'] = 'not_uti' ## else the claims is not related to UTI
    return row

def identify_pneu_claims(row): ## a function to identify pneumonia claims similar to the function to identify UTI claims

    ## define diagnosis code columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    ## define poa indicator columns
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1, 26))]

    pneumonia_claims = row[dcode[2:]].isin(icd[icd['outcome']=="pneumonia"]['icd']) ##determines if diagnosis code is related to pneumonia
    poa = row[poacode[2:]].str.startswith('Y', na=False)  ##determines if the diagnosis is present-on-admission to hospital

    if (any(row['DGNS_1_CD']==code for code in icd[icd['outcome'] == "pneumonia"]['icd'])) & (row.POA_DGNS_1_IND_CD == "Y"):
        row['claim_typePNEU'] = 'primary'
    elif (any(row['DGNS_2_CD']==code for code in icd[icd['outcome'] == "pneumonia"]['icd'])) & (row.POA_DGNS_2_IND_CD == "Y"):
        row['claim_typePNEU'] = 'second'
    elif any([True for i,j in zip(pneumonia_claims, poa) if i&j]):
        row['claim_typePNEU'] = 'secondary'
    else:
        row['claim_typePNEU'] = 'not_pneumonia'

    return row


years = range(2011, 2018)
medparPath = "/gpfs/data/cms-share/data/medicare/{}/medpar/parquet" ## raw MedPAR data path
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/new/'
## read in icd codes for UTI and Pneumonia
icd = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/gitlab_code/nhc_infections/code/initial_analysis/icd.csv')
icd = icd.astype({'icd': 'str'})

## loop through each year of data
for year in years:

    print(year)
    ## read in MedPAR data
    df = dd.read_parquet(medparPath.format(year), engine="fastparquet")
    df = df.reset_index()
    ## identify columns to use in medpar
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1, 26))]
    dvcode = ['DGNS_VRSN_CD_{}'.format(i) for i in list(range(1, 26))]

    ## define columns used for analysis
    col_use = ['BENE_ID', 'MEDPAR_ID', 'MEDPAR_YR_NUM', 'PRVDR_NUM', 'ADMSN_DT', 'DSCHRG_DT',
               'DSCHRG_DSTNTN_CD', 'SS_LS_SNF_IND_CD', 'BENE_DSCHRG_STUS_CD', 'DRG_CD',
               'ADMTG_DGNS_CD'] + dcode + poacode + dvcode
    df = df[col_use] ## subset data to only useful columns

    ## using only hospital admission data - excluding SNF claims
    hospital = df[df.SS_LS_SNF_IND_CD.isin(['S', "L"])]

    del df
    ## apply the function to identify UTI-related claims
    hospital_uti = hospital.map_partitions(lambda ddf: ddf.apply(identify_uti_claims, axis=1))
    ## write UTI-related claims to parquet files
    hospital_uti[hospital_uti['claim_typeUTI'] == "primary"].repartition(npartitions=20).to_parquet(
        writePath + 'primaryUTI/{}/'.format(year)
    )
    hospital_uti[hospital_uti['claim_typeUTI'] == "second"].repartition(npartitions=20).to_parquet(
        writePath + 'secondUTI/{}/'.format(year)
    )
    hospital_uti[hospital_uti['claim_typeUTI'] == "secondary"].repartition(npartitions=40).to_parquet(
        writePath + 'secondaryUTI/{}/'.format(year)
    )

    del hospital_uti
    ## apply the function to identify pneumonia-related claims
    hospital_pneu = hospital.map_partitions(lambda ddf: ddf.apply(identify_pneu_claims, axis=1))
    ## write pneumonia-related claims to parquet files
    hospital_pneu[hospital_pneu['claim_typePNEU'] == "primary"].repartition(npartitions=20).to_parquet(
        writePath + 'primaryPNEU/{}/'.format(year)
    )
    hospital_pneu[hospital_pneu['claim_typePNEU'] == "second"].repartition(npartitions=20).to_parquet(
        writePath + 'secondPNEU/{}/'.format(year)
    )
    hospital_pneu[hospital_pneu['claim_typePNEU'] == "secondary"].repartition(npartitions=40).to_parquet(
        writePath + 'secondaryPNEU/{}/'.format(year)
    )

