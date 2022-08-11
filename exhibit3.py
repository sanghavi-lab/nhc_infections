## CALCULATE REPORTING RATE FOR HOSPITAL CLAIMS AND SNF CLAIMS SAMPLE BY LONG-STAY VS SHORT-STAY RESIDENTS

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime
from dask.distributed import Client
client = Client("10.50.86.251:40707")
pd.set_option('display.max_columns', 500)

def create_nh_reporting(data, mds_item, output_path):
    ## a function to calculate nursing home reporting rates by long-stay vs short-stay residents,
    ## and nursing home claim weight
    nh_claims = data.groupby(['short_stay', 'MCARE_ID'])['BENE_ID'].count().rename('nh_nclaims').reset_index()
    nclaims = data.groupby('short_stay')['BENE_ID'].count().rename('nclaims').reset_index()
    ## calculate the reporting rate
    nh_reporting = data.groupby(['short_stay', 'MCARE_ID'])[mds_item].mean().rename(
        'reporting').reset_index()

    nh_reporting = nh_reporting.merge(nclaims, on='short_stay')
    nh_reporting = nh_reporting.merge(nh_claims, on=['short_stay', 'MCARE_ID'])
    ## calculate the claim weight
    nh_reporting['weight'] = nh_reporting['nh_nclaims'] / nh_reporting['nclaims']

    nh_reporting.to_csv(output_path, index=False)


inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/exhibits/infection/FINAL/'

years = range(2011, 2018)
claims_type = ["primary", "secondary"]
outcome = {"UTI": ["I2300_UTI_CD", "I2300_UTI_CD_add_readmission"],
           "PNEU": ["I2000_PNEUMO_CD"]}

## <editor-fold desc="Main Reporting Rates">
reporting_lst = []
claims_count_lst = []


for diagnosis in outcome.keys():
    for ctype in claims_type:

        ## read in final dataset
        df = pd.read_csv(inputPath + 'new/{0}{1}.csv'.format(ctype, diagnosis), low_memory=False)
        # ## appendix only ##########################################################
        # df = df[df['MEDPAR_YR_NUM']>2015]
        # ###########################################################################
        ## main reporting rate
        reporting = \
            df.groupby('short_stay')[outcome.get(diagnosis)[0]].mean().\
                rename('{0}{1}_reporting'.format(ctype, diagnosis))
        reporting_lst.append(reporting)

        claims_count = df.groupby('short_stay')['MEDPAR_ID'].count().rename('{0}{1}_claims_count'.format(ctype, diagnosis))
        claims_count_lst.append(claims_count)
        ## reporting rate for claims matched with infection diagnosis in MDS I8000
        reporting_mds = \
            df[df['mds_{}_diag'.format(diagnosis.lower())]==1].groupby('short_stay')[outcome.get(diagnosis)[0]].mean().\
                rename('{0}{1}_reporting_mds_diagnosis'.format(ctype, diagnosis))
        reporting_lst.append(reporting_mds)

        claims_count_lst.append(
            df[df['mds_{}_diag'.format(diagnosis.lower())] == 1].groupby('short_stay')['MEDPAR_ID'].count().\
                rename('{0}{1}_claims_count_mds_diagnosis'.format(ctype, diagnosis))
        )

        if diagnosis=='UTI':
            ## reporting for MDS indwelling catheter
            reporting_mds = \
                df[df['H0100A_INDWLG_CTHTR_CD'] == '1'].groupby('short_stay')[
                    outcome.get(diagnosis)[0]].mean(). \
                    rename('{0}{1}_reporting_catheter'.format(ctype, diagnosis))
            reporting_lst.append(reporting_mds)

            claims_count_lst.append(
                df[df['H0100A_INDWLG_CTHTR_CD'] == '1'].groupby('short_stay')['MEDPAR_ID'].count(). \
                    rename('{0}{1}_claims_count_catheter'.format(ctype, diagnosis))
            )
            ## calculate readmission + discharge MDS reporting rate for UTI
            df_readmission = pd.read_csv(inputPath + 'new/{0}{1}_readmission.csv'.format(ctype, diagnosis), low_memory=False)
            # ## appendix only ##########################################################
            # df_readmission = df_readmission[df_readmission['MEDPAR_YR_NUM'] > 2015]
            # ###########################################################################
            df_readmission = df.merge(df_readmission[['MEDPAR_ID', 'I2300_UTI_CD']], on='MEDPAR_ID',
                                      suffixes=['', '_readmission'],
                                      how='left')
            df_readmission['I2300_UTI_CD_readmission'] = \
                df_readmission['I2300_UTI_CD_readmission'].fillna(0)
            # claims_count_lst.append(
            #     df_readmission.groupby('short_stay')['MEDPAR_ID'].count().rename('{0}{1}_claims_count_UTI_readmission'.format(ctype, diagnosis))
            # )
            df_readmission['I2300_UTI_CD_add_readmission'] = df_readmission[
                ['I2300_UTI_CD', 'I2300_UTI_CD_readmission']].max(axis=1)

            reporting_lst.append(
                df_readmission.groupby('short_stay')[outcome.get(diagnosis)[1]].mean().rename(
                    '{0}{1}_reporting_readmission'.format(ctype, diagnosis))
            )

            reporting_mds = \
                df_readmission[df_readmission['mds_{}_diag'.format(diagnosis.lower())] == 1].groupby('short_stay')[
                    outcome.get(diagnosis)[1]].mean(). \
                    rename('{0}{1}_reporting_mds_diagnosis_readmission'.format(ctype, diagnosis))
            reporting_lst.append(reporting_mds)


            reporting_mds = \
                df_readmission[df_readmission['H0100A_INDWLG_CTHTR_CD'] == '1'].groupby('short_stay')[
                    outcome.get(diagnosis)[1]].mean(). \
                    rename('{0}{1}_reporting_catheter_readmission'.format(ctype, diagnosis))
            reporting_lst.append(reporting_mds)


pd.concat(claims_count_lst, axis=1).to_csv(
    writePath + 'exhibit3_final/hos_claims_count.csv'
)
pd.concat(reporting_lst, axis=1).to_csv(
    writePath + 'exhibit3_final/hos_claims_reporting.csv'
)
# # </editor-fold>

# # ## calculate the NH-level reporting rate
for diagnosis in outcome.keys():
    for ctype in claims_type:

        # ## read in final dataset
        df = pd.read_csv(inputPath + 'new/{0}{1}.csv'.format(ctype, diagnosis), low_memory=False)
        create_nh_reporting(df, outcome.get(diagnosis)[0], writePath + 'exhibit3_final/{}{}_NHReporting.csv'.format(ctype, diagnosis))
        create_nh_reporting(df[df['mds_{}_diag'.format(diagnosis.lower())]==1],
                            outcome.get(diagnosis)[0],
                            writePath + 'exhibit3_final/{}{}_NHReporting_MDSDiag.csv'.format(ctype, diagnosis))
#
        if diagnosis=='UTI':
            # create_nh_reporting(df[df['H0100A_INDWLG_CTHTR_CD']=='1'],
            #                     outcome.get(diagnosis)[0],
            #                     writePath + 'exhibit3_final/{}{}_NHReporting_Catheter.csv'.format(ctype, diagnosis))

            ## read in readmission
            df_readmission = pd.read_csv(inputPath + 'new/{0}{1}_readmission.csv'.format(ctype, diagnosis),
                                         low_memory=False)
            df_readmission = df.merge(df_readmission[['MEDPAR_ID', 'I2300_UTI_CD']], on='MEDPAR_ID',
                                      suffixes=['', '_readmission'],
                                      how='left')
            df_readmission['I2300_UTI_CD_readmission'] = \
                df_readmission['I2300_UTI_CD_readmission'].fillna(0)
            ## create a new indicator for whether the UTI was reported on discharge or post-hospitalization MDS assessment
            df_readmission['I2300_UTI_CD_add_readmission'] = df_readmission[
                ['I2300_UTI_CD', 'I2300_UTI_CD_readmission']].max(axis=1)

            create_nh_reporting(df_readmission, outcome.get(diagnosis)[1],
                                writePath + 'exhibit3_final/{}{}_NHReporting_Readmission.csv'.format(ctype, diagnosis))
            create_nh_reporting(df_readmission[df_readmission['mds_{}_diag'.format(diagnosis.lower())] == 1],
                                outcome.get(diagnosis)[1],
                                writePath + 'exhibit3_final/{}{}_NHReporting_MDSDiag_Readmission.csv'.format(ctype, diagnosis))

            create_nh_reporting(df_readmission[df_readmission['H0100A_INDWLG_CTHTR_CD'] == '1'],
                                outcome.get(diagnosis)[1],
                                writePath + 'exhibit3_final/{}{}_NHReporting_Catheter_Readmission.csv'.format(ctype, diagnosis))


## SNF reporting
for diagnosis in outcome.keys():
    print(diagnosis)
    df_lst = []
    for year in years:
        df_year = dd.read_parquet(inputPath + 'SMDS/{0}{1}/{2}'.format('primary', diagnosis, year))
    print(df[outcome.get(diagnosis)[0]].mean().compute())
    # ## appendix only ##########################################################
    # df_lst = []
    # df_lst_icd10 = []
    # for year in years:
    #     if year<=2015:
    #         df_year = dd.read_parquet(inputPath + 'SMDS/{0}{1}/{2}'.format('primary', diagnosis, year))
    #         df_lst.append(df_year)
    #     else:
    #         df_year = dd.read_parquet(inputPath + 'SMDS/{0}{1}/{2}'.format('primary', diagnosis, year))
    #         df_lst_icd10.append(df_year)
    # df = dd.concat(df_lst)
    # print(df.shape[0].compute())
    # print(df[outcome.get(diagnosis)[0]].mean().compute())  # 0.7904478131931889
    #
    # df_cdi10 = dd.concat(df_lst_icd10)
    # print(df_cdi10.shape[0].compute())
    # print(df_cdi10[outcome.get(diagnosis)[0]].mean().compute())  # 0.7904478131931889
    #############################################################################

    ## calculate nursing home-level SNF reporting rates
    nh_claims = df.groupby(['STATE_CD', 'FAC_PRVDR_INTRNL_ID'])['MEDPAR_ID'].count().rename('nh_nclaims').reset_index()
    all_claims = df.shape[0].compute()

    nh_claims['nclaims'] = all_claims
    nh_claims['nh_claims_weight'] = nh_claims['nh_nclaims']/nh_claims['nclaims']

    nh_reporting = df.groupby(['STATE_CD', 'FAC_PRVDR_INTRNL_ID'])[outcome.get(diagnosis)[0]].mean().reset_index()
    nh_reporting_weight = nh_reporting.merge(nh_claims, on=['STATE_CD', 'FAC_PRVDR_INTRNL_ID'], how='inner')

    nh_reporting_weight.compute().to_csv(
        writePath + 'exhibit3_final/SNF{0}{1}ReportingByNH.csv'.format('primary', diagnosis)
    )
#
#