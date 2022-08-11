## THIS SCRIPT IDENTIFY UTI/PNEUMONIA IN SNF CLAIMS FROM MEDPAR

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:56321")

def identify_uti_claims(row):
    ## a function to identify UTI claims using diagnosis code
    ## define diagnosis code columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]

    uti_claims = row[dcode[2:]].isin(icd[icd['outcome']=="uti"]['icd']) ##determines if diagnosis code is related to uti

    if (any(row['DGNS_1_CD']==code for code in icd[icd['outcome'] == "uti"]['icd'])):
        row['claim_typeUTI'] = 'primary' ## if the first diagnosis code is related to UTI
    elif (any(row['DGNS_2_CD']==code for code in icd[icd['outcome'] == "uti"]['icd'])):
        row['claim_typeUTI'] = 'second' ## if the second diagnosis code is related to UTI
    elif any(uti_claims):
        row['claim_typeUTI'] = 'secondary' ## if any remaining secondary diagnosis code is related to UTI
    else:
        row['claim_typeUTI'] = 'not_uti'
    return row

def identify_pneu_claims(row):
    ## a function to identify pneumonia claims using diagnosis code
    ## define diagnosis code columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]

    pneumonia_claims = row[dcode[2:]].isin(icd[icd['outcome']=="pneumonia"]['icd']) ##determines if diagnosis code is related to pneumonia

    if (any(row['DGNS_1_CD']==code for code in icd[icd['outcome'] == "pneumonia"]['icd'])):
        row['claim_typePNEU'] = 'primary'
    elif (any(row['DGNS_2_CD']==code for code in icd[icd['outcome'] == "pneumonia"]['icd'])):
        row['claim_typePNEU'] = 'second'
    elif any(pneumonia_claims):
        row['claim_typePNEU'] = 'secondary'
    else:
        row['claim_typePNEU'] = 'not_pneumonia'

    return row


years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]

medparPath = "/gpfs/data/cms-share/data/medicare/{}/medpar/parquet"
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
# ## read in icd codes for UTI and Pneumonia
icd = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/gitlab_code/nhc_infections/code/initial_analysis/icd.csv')
icd = icd.astype({'icd': 'str'})

for year in years:

    ## define diagnosis columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    ## define poa indicator columns (not used)
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1, 26))]
    ## define diagnosis version columns
    dvcode = ['DGNS_VRSN_CD_{}'.format(i) for i in list(range(1, 26))]
    ## define columns selected for the analysis
    col_use = ['BENE_ID', 'MEDPAR_ID', 'MEDPAR_YR_NUM', 'PRVDR_NUM', 'ADMSN_DT', 'DSCHRG_DT',
               'DSCHRG_DSTNTN_CD', 'SS_LS_SNF_IND_CD', 'BENE_DSCHRG_STUS_CD', 'DRG_CD',
               'ADMTG_DGNS_CD', 'CVRD_LVL_CARE_THRU_DT'] + dcode + poacode + dvcode

    ## read in raw MedPAR data
    medpar = dd.read_parquet(medparPath.format(year), engine="fastparquet")
    medpar = medpar.reset_index()

    ## select columns and rows
    ## 1) select inpatient data
    ## 2) select columns
    ## 3) select pu related claims

    ## 1) select SNF claims
    snf = medpar[medpar.SS_LS_SNF_IND_CD == 'N']
    ## 2) select columns
    snf = snf[col_use]
    ## apply the function to identify UTI SNF claims
    snf_uti = snf.map_partitions(lambda ddf: ddf.apply(identify_uti_claims, axis=1))

    snf_uti[snf_uti['claim_typeUTI'] == "primary"].repartition(npartitions=20).to_parquet(
        writePath + 'SNFprimaryUTI/{}/'.format(year)
    )
    snf_uti[snf_uti['claim_typeUTI'] == "second"].repartition(npartitions=20).to_parquet(
        writePath + 'SNFsecondUTI/{}/'.format(year)
    )
    snf_uti[snf_uti['claim_typeUTI'] == "secondary"].repartition(npartitions=40).to_parquet(
        writePath + 'SNFsecondaryUTI/{}/'.format(year)
    )
    ## apply the function to identify pneumonia SNF claims
    snf_pneu = snf.map_partitions(lambda ddf: ddf.apply(identify_pneu_claims, axis=1))

    snf_pneu[snf_pneu['claim_typePNEU'] == "primary"].repartition(npartitions=20).to_parquet(
        writePath + 'SNFprimaryPNEU/{}/'.format(year)
    )
    snf_pneu[snf_pneu['claim_typePNEU'] == "second"].repartition(npartitions=20).to_parquet(
        writePath + 'SNFsecondPNEU/{}/'.format(year)
    )
    snf_pneu[snf_pneu['claim_typePNEU'] == "secondary"].repartition(npartitions=40).to_parquet(
        writePath + 'SNFsecondaryPNEU/{}/'.format(year)
    )
#

## set BENE_ID as index for SNF claims
for year in years:
    for ctype in claims_type:
        df = dd.read_parquet(writePath + 'SNF{0}UTI/{1}/'.format(ctype, year))
        df_index = df.set_index('BENE_ID')
        df_index.to_parquet(writePath + 'SNF{0}UTI_indexed/{1}/'.format(ctype, year))



for year in years:
    for ctype in claims_type:
        df = dd.read_parquet(writePath + 'SNF{0}PNEU/{1}/'.format(ctype, year))
        df_index = df.set_index('BENE_ID')
        df_index.to_parquet(writePath + 'SNF{0}PNEU_indexed/{1}/'.format(ctype, year))
