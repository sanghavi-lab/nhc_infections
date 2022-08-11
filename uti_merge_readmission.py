## FOR UTI HOSPITAL CLAIMS SAMPLE, MERGE THE FIRST POST-HOSPITALIZATION MDS WIHTIN 30 DAYS
## AFTER HOSPITAL DISCHARGE COMPLETED BY THE SAME NURSING HOME TO CALCULATE ADDITIONAL REPORTING LATER

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:39748")

mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_STAR/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CREADMISSION_UTI/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "secondary"]

for ctype in claims_type:
    ## read in UTI hospital claims sample data from last step (merge star ratings)
    df = dd.read_parquet(inputPath + '{0}{1}'.format(ctype, 'UTI'))
    df = df.astype({'DSCHRG_DT': 'datetime64[ns]',
                    'fac_state_id': 'str'})
    # print(df.shape[0].compute())#473912
    for year in years:
        ## subset UTI hospital claims sample to each year
        df_year = df[df['MEDPAR_YR_NUM']==year]
        df_year = df_year.repartition(npartitions=20)

        ## read and clean MDS to merge with UTI hospital claims sample later
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

        mds_use['FAC_PRVDR_INTRNL_ID'] = mds_use['FAC_PRVDR_INTRNL_ID'].astype('float').astype('Int64')

        mds_use = mds_use.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
        mds_use['FAC_PRVDR_INTRNL_ID'] = mds_use['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
        mds_use['fac_state_id'] = mds_use['FAC_PRVDR_INTRNL_ID'] + mds_use['STATE_CD']

        del mds

        ## merge mds with claims sample; add _readmission to the name of new mds columns to indicate post-hospitalization MDS;
        ## add _dischrg to the name of old mds columns to indicate discharge assessment
        cmds = df_year.merge(mds_use, on='BENE_ID', suffixes=['_dischrg', '_readmission'])
        ## calculate days between hospital discharge and new MDS
        cmds['days_readmission'] = (cmds['TRGT_DT_readmission'] - cmds['DSCHRG_DT']).dt.days
        ## select the MDS from the same nursing home within 30 days after hospital discharge
        creadmission = cmds[(cmds['days_readmission']>=0) & (cmds['fac_state_id_dischrg']==cmds['fac_state_id_readmission']) & (cmds['days_readmission']<=30)]

        ## only keep MDS with valid UTI items
        creadmission_uti = creadmission[creadmission['I2300_UTI_CD_readmission'].isin([0, 1, '0', '1'])]
        ## select the MDS closest to hospital discharge
        creadmission_uti_closest = creadmission_uti.groupby('MEDPAR_ID')['days_readmission'].min().reset_index()
        creadmission_uti = creadmission_uti.merge(creadmission_uti_closest, on=['MEDPAR_ID', 'days_readmission'])

        ## there could be multiple MDS with the same target date; aggregate the UTI information for these MDS
        agg_uti = creadmission_uti.groupby(['MEDPAR_ID'])['I2300_UTI_CD_readmission'].max().reset_index()
        ## drop columns from discharge MDS; also drop the original I2300_UTI_CD_readmission
        creadmission_uti = creadmission_uti.drop(columns=['I2300_UTI_CD_readmission'] + [col for col in creadmission_uti.columns if col.endswith('_dischrg')])
        ## keep only one MDS per hospital claim
        creadmission_uti_unique = creadmission_uti.drop_duplicates(subset='MEDPAR_ID')
        ## merge the aggregated I2300_UTI_CD back in
        creadmission_uti_unique = creadmission_uti_unique.merge(agg_uti, on='MEDPAR_ID')
        ## write to partition
        creadmission_uti_unique.repartition(npartitions=20).to_parquet(
            writePath + '{0}/{1}'.format(ctype, year)
        )


