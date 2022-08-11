## THIS SCRIPT MERGES MBSF CHRONIC CONDITIONS DATA WITH HOSPITAL INFECTION CLAIMS SAMPLE
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client

client = Client("10.50.86.250:35086")

mbsfccPath = '/gpfs/data/cms-share/data/medicare/{}/mbsf/mbsf_cc/parquet/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SAMENH_CC/'
UTImergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SAMENH/'
PNEUmergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CREENTER/'

analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "second", "secondary"]
outcome = ["UTI", "PNEU"]


def add_chronic_condition_code(row):
    ## this function will determine the chronic condition for each beneficiary:
    ## if the beneficiary has ever been diagnosed with the chronic condition prior to hospital admission
    ## the indicator for the chronic condition (**_final) equals 1 and 0 otherwise

    ## define chronic condition columns
    cc_code = ['AMI', 'ALZH', 'ALZH_DEMEN', 'ATRIAL_FIB', 'CATARACT',
               'CHRONICKIDNEY', 'COPD', 'CHF', 'DIABETES', 'GLAUCOMA',
               'HIP_FRACTURE', 'ISCHEMICHEART', 'DEPRESSION',
               'OSTEOPOROSIS', 'RA_OA', 'STROKE_TIA', 'CANCER_BREAST',
               'CANCER_COLORECTAL', 'CANCER_PROSTATE', 'CANCER_LUNG', 'CANCER_ENDOMETRIAL', 'ANEMIA',
               'ASTHMA', 'HYPERL', 'HYPERP', 'HYPERT', 'HYPOTH']

    for cc in cc_code:
        colname = cc + '_final'
        cc_ever = cc + '_EVER'
        if (row[cc_ever] <= row['ADMSN_DT']):
            row[colname] = 1
        else:
            row[colname] = 0
    return row

## define chronic condition columns
cc_code = ['AMI', 'ALZH', 'ALZH_DEMEN', 'ATRIAL_FIB', 'CATARACT',
           'CHRONICKIDNEY', 'COPD', 'CHF', 'DIABETES', 'GLAUCOMA',
           'HIP_FRACTURE', 'ISCHEMICHEART', 'DEPRESSION',
           'OSTEOPOROSIS', 'RA_OA', 'STROKE_TIA', 'CANCER_BREAST',
           'CANCER_COLORECTAL', 'CANCER_PROSTATE', 'CANCER_LUNG', 'CANCER_ENDOMETRIAL', 'ANEMIA',
           'ASTHMA', 'HYPERL', 'HYPERP', 'HYPERT', 'HYPOTH']
## read in mbsf chronic condition data
for year in years:
    cc = dd.read_parquet(mbsfccPath.format(year))
    cc = cc.astype(dict(zip([c + "_EVER" for c in cc_code],
                            ['datetime64[ns]'] * 27)))
    cc = cc.reset_index()
    for ctype in claims_type:
        for health_outcome in outcome:
             ## read in mds-hospital_claims data for each year
            if health_outcome=='UTI':
                df_year = dd.read_parquet(UTImergePath + '{0}{1}/{2}'.format(ctype, health_outcome, year))
            if health_outcome=='PNEU':
                df_year = dd.read_parquet(PNEUmergePath + '{0}{1}/{2}'.format(ctype, health_outcome, year))

            ## merge mbsfcc chronic conditions with hospital infection claims sample data for each year
            merge = df_year.merge(cc[['BENE_ID'] + [c + "_EVER" for c in cc_code]],
                                  on='BENE_ID', how='inner')
            ## apply the function to create 27 chronic condition binary indicators for whether the resident had the chronic condition
            merge = merge.apply(add_chronic_condition_code, axis=1)
            ## drop columns
            merge = merge.drop(columns=[c + "_EVER" for c in cc_code])
            # write to parquet
            merge.to_parquet(writePath + '{0}{1}/{2}'.format(ctype, health_outcome, year))
