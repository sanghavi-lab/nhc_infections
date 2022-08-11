## this code is to identify residents return to the same nursing home after pneumonia hospitalization within 1 day,
## and have an assessment within 7 days of hospital discharge that has a valid Pneumonia item (some MDS doesn't report pneumonia item).
## This attempts to solve the issue that discharge assessments after 2011 don't require Pneumonia item.


import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:33425")

claimsPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/constructed_data2/MBSF/'
mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
mergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CREENTER/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]

for year in years:
    ## define MDS items to use
    cols_use = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'A0310A_FED_OBRA_CD',
                  'A0310B_PPS_CD', 'A0310C_PPS_OMRA_CD', 'A0310D_SB_CLNCL_CHG_CD', 'A0310E_FIRST_SINCE_ADMSN_CD',
                  'A0310F_ENTRY_DSCHRG_CD', 'A1600_ENTRY_DT', 'A1700_ENTRY_TYPE_CD', 'A1800_ENTRD_FROM_TXT',
                  'A1900_ADMSN_DT', 'A2000_DSCHRG_DT', 'A2100_DSCHRG_STUS_CD', 'A2300_ASMT_RFRNC_DT', 'I2000_PNEUMO_CD']
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
    mds_use = mds[cols_use]
    ## change data type
    mds_use = mds_use.astype({'A2000_DSCHRG_DT': 'datetime64[ns]',
                              'A1600_ENTRY_DT': 'string',
                              'TRGT_DT': 'string'})
    ## change date columns to datetime format
    mds_use['A1600_ENTRY_DT'] = dd.to_datetime(mds_use['A1600_ENTRY_DT'], infer_datetime_format=True)
    mds_use['TRGT_DT'] = dd.to_datetime(mds_use['TRGT_DT'], infer_datetime_format=True)

    del mds

    for ctype in claims_type:
        ## read in Pnuemonia claims merged with MDS discharge assessment within 1 day of hospital admission
        claims = dd.read_parquet(mergePath + '{0}{1}/{2}/'.format(ctype, 'PNEU', year))
        ## drop duplicated MedPAR claims (one medpar can be linked to multiple discharge assessments with the same target date)
        claims_unique = claims.drop_duplicates(subset='MEDPAR_ID')
        ## drop most MDS columns from discharge assessments (do not need them)
        infection_cols = ['I2300_UTI_CD', 'I2000_PNEUMO_CD']
        other_cols = ['MDS_ASMT_ID', 'A0310A_FED_OBRA_CD',
                      'A0310B_PPS_CD', 'A0310C_PPS_OMRA_CD', 'A0310D_SB_CLNCL_CHG_CD', 'A0310E_FIRST_SINCE_ADMSN_CD',
                      'A0310F_ENTRY_DSCHRG_CD', 'A1600_ENTRY_DT', 'A1700_ENTRY_TYPE_CD', 'A1800_ENTRD_FROM_TXT',
                      'A1900_ADMSN_DT', 'A2000_DSCHRG_DT', 'A2100_DSCHRG_STUS_CD', 'A2300_ASMT_RFRNC_DT']
        claims_unique = claims_unique.drop(columns=[col for col in claims.columns if col in infection_cols+other_cols])

        del claims

        ## merge claims with MDS assessments
        claims_mds = claims_unique.merge(mds_use, on='BENE_ID', how='left', suffixes=['_dischrg', '_reenter'])
        ## calculate the number of days between hospital discharge and nursing home readmission
        claims_mds = claims_mds.astype({'DSCHRG_DT': 'datetime64[ns]',
                                        'TRGT_DT_reenter': 'datetime64[ns]'})
        claims_mds['days_reenter'] = (claims_mds['TRGT_DT_reenter'] - claims_mds['DSCHRG_DT']).dt.days

        ## keep those returned to the same nursing home
        claims_mds_samenh = claims_mds[
            (claims_mds['FAC_PRVDR_INTRNL_ID_dischrg'] == claims_mds['FAC_PRVDR_INTRNL_ID_reenter']) &
            (claims_mds['STATE_CD_dischrg'] == claims_mds['STATE_CD_reenter'])]

        ## create an indicator to show if the resident has an MDS completed within 1 day after hospital discharge
        claims_mds_samenh['1day'] = (claims_mds_samenh['days_reenter'] <= 1) & (claims_mds_samenh['days_reenter'] >= 0)
        claims_1day = claims_mds_samenh.groupby('MEDPAR_ID')['1day'].max().reset_index()

        ## select MDS within 7 days of hospital discharge
        claims_count_samenh = claims_mds_samenh[(claims_mds_samenh['days_reenter'] <= 7) &
                                                (claims_mds_samenh['days_reenter'] >= 0)]


        ## keep claims merged with MDS with a valid Pneumonia item on the reenter assessment
        claims_pneumonia = claims_mds_samenh[claims_mds_samenh['I2000_PNEUMO_CD'].isin(['0', '1', 0, 1])]
        ## aggregate the Pneumonia info for all assessments merged with each MedPAR claim
        claims_pneumonia = claims_pneumonia.astype({'I2000_PNEUMO_CD': 'int'})
        pneumonia_info = claims_pneumonia.groupby('MEDPAR_ID').agg({'I2000_PNEUMO_CD': 'max'}).reset_index()
        del claims_mds
        del claims_mds7days

        ## keep only unique pneumonia claims merged with an eligible MDS
        claims_final = claims_pneumonia.drop_duplicates(subset='MEDPAR_ID')
        ## drop original I2000_PNEUMO_CD
        claims_final = claims_final.drop(columns=['I2000_PNEUMO_CD'])
        ## merge aggregated I2000_PNEUMO_CD info with unique pneumonia claims
        claims_mds_final = claims_final.merge(pneumonia_info, on='MEDPAR_ID', how='left')
        ## rename columns
        claims_mds_final = claims_mds_final.rename(columns={'TRGT_DT_dischrg': 'TRGT_DT'})

        ## merge the "1day" indicator back
        claims_mds_final = claims_mds_final.merge(claims_1day, on='MEDPAR_ID')
        ## subset cliams to those with an MDS completed from the same nursing home within 1 day after hospitalization
        claims_mds_final = claims_mds_final[claims_mds_final['1day']==1]

        claims_mds_final.repartition(npartitions=20).to_parquet(
            writePath + '{0}{1}/{2}/'.format(ctype, 'PNEU', year)
        )


## flowchart calculation
# claims_count_final \
#         = {'primary': {'dischrg': [],
#                        'Seven': [],
#                        'Five': []},
#            'second': {'dischrg': [],
#                        'Seven': [],
#                        'Five': []},
#            'secondary': {'dischrg': [],
#                        'Seven': [],
#                        'Five': []}
#            }
# for year in years:
#     for ctype in claims_type:
#         dischrg = dd.read_parquet(mergePath + '{0}{1}/{2}/'.format(ctype, 'PNEU', year))
#         claims_count_final[ctype]['dischrg'].append(dischrg.MEDPAR_ID.nunique().compute())
#         df = dd.read_parquet(writePath + '{0}{1}/{2}/'.format(ctype, 'PNEU', year))
#         claims_count_final[ctype]['Seven'].append(df.MEDPAR_ID.nunique().compute())
#         df2 = dd.read_parquet(writePath + 'FIVEDAYS/{0}{1}/{2}/'.format(ctype, 'PNEU', year))
#         claims_count_final[ctype]['Five'].append(df2.MEDPAR_ID.nunique().compute())
# print(pd.DataFrame(claims_count_final).applymap(sum))
#          primary  second  secondary
# dischrg   370760  458997     358082
# Seven     206912  235741     139434
# Five       43078   50569      33796


