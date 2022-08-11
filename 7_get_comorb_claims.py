## EXTRACT ALL DIAGNOSIS CODES FOR EACH PATIENT WITHIN A YEAR OF HOSPITALIZATION FOT CALCULATING COMORBIDITY SCORE
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:35086")

medparPath = "/gpfs/data/cms-share/data/medicare/{}/medpar/parquet/"
slPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_SL/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "secondary"]
outcome = ["UTI", "PNEU"]

def get_diagnosis(analysisDF, medpar, intermediate_output_path=None, output_path=None):
    ## drop diagnosis columns in analytical sample data
    dx_cols = ['DGNS_{}_CD'.format(i) for i in range(1, 26)]
    analysisDF = analysisDF.drop(columns=dx_cols)
    ## merge analytical sample with raw medpar data
    merge = analysisDF.merge(medpar, on='BENE_ID', how='left', suffixes=['', '_comorb'])
    ## keep raw medpar data with admission dates within 1 year before the admission date on hospital infection claims
    merge1year = merge[(merge['ADMSN_DT']>=merge['ADMSN_DT_comorb']) &
                       ((merge['ADMSN_DT'] - merge['ADMSN_DT_comorb']).dt.days <=365)]

    ## separate icd 10 and icd 9 claims (useful for 2015 data)
    merge_icd9 = merge1year[merge1year['ADMSN_DT_comorb'] < datetime.datetime(2015, 10, 1)]
    merge_icd10 = merge1year[merge1year['ADMSN_DT_comorb'] >= datetime.datetime(2015, 10, 1)]
    ## reshape data from wide to long: for each hospital infection claim, there are multiple rows of diagnosis code
    dx_icd9 = merge_icd9[['MEDPAR_ID'] + dx_cols].\
        melt(id_vars='MEDPAR_ID', value_vars=dx_cols, var_name='n', value_name='DX')
    ## create an indicator to indicate ICD-9-CM vs ICD-10-CM coding system
    dx_icd9['Dx_CodeType'] = "09"
    dx_icd10 = merge_icd10[['MEDPAR_ID'] + dx_cols].\
        melt(id_vars='MEDPAR_ID', value_vars=dx_cols, var_name='n', value_name='DX')
    dx_icd10['Dx_CodeType'] = "10"
    ## concat icd9 and icd10
    dx = dd.concat([dx_icd9, dx_icd10])
    dx = dx.rename(columns={"MEDPAR_ID": "patid"})

    dx.to_parquet(output_path)


primary_dx_all = []
secondary_dx_all = []
for year in years:

    ## read in raw medpar data for current year and the prior year
    use_cols = ['BENE_ID', 'ADMSN_DT'] + ['DGNS_{}_CD'.format(i) for i in range(1, 26)]
    medpar_year = dd.read_parquet(medparPath.format(year))
    medpar_prior = dd.read_parquet(medparPath.format(year - 1))
    ## concat two years of medpar data
    if year==2011:
        medpar_year = medpar_year.reset_index()
        medpar = dd.concat([medpar_year, medpar_prior])

    if year==2017:
        medpar_prior = medpar_prior.reset_index()
        medpar = dd.concat([medpar_year, medpar_prior])

    if year in range(2012, 2017):
        medpar = dd.concat([medpar_year, medpar_prior])
        medpar = medpar.reset_index()
    ## subset to only useful columns
    medpar = medpar[use_cols]
    medpar = medpar.astype({'ADMSN_DT': 'datetime64[ns]'})

    ## read in hospital infection claims sample
    primary = dd.read_parquet(slPath + "{0}{1}/{2}".format("primary", "UTI", year))
    secondary = dd.read_parquet(slPath + "{0}{1}/{2}".format("secondary", "UTI", year))
    ## apply the function to select all diagnosis codes from hospital claims within a year of hospitalization
    get_diagnosis(primary, medpar,
                  output_path=writePath + "DX/{0}{1}/{2}".format("primary", "UTI", year))
    get_diagnosis(secondary, medpar,
                  output_path=writePath + "DX/{0}{1}/{2}".format("secondary", "UTI", year))
    ## read the diagnosis data back
    primary_dx = dd.read_parquet(
        writePath + "DX/{0}{1}/{2}".format("primary", "UTI", year)
    )
    secondary_dx = dd.read_parquet(
         writePath + "DX/{0}{1}/{2}".format("secondary", "UTI", year)
        )
    ## remove missing diagnosis code and keep only unique diagnosis code for each claim
    primary_dx = primary_dx[~primary_dx['DX'].isna()]
    primary_dx_unique = primary_dx[['patid', 'DX', 'Dx_CodeType']].drop_duplicates()
    secondary_dx = secondary_dx[~secondary_dx['DX'].isna()]
    secondary_dx_unique = secondary_dx[['patid', 'DX', 'Dx_CodeType']].drop_duplicates()
    ## convert parquet file to csv
    primary_dx_df = primary_dx_unique.compute()
    secondary_dx_df = secondary_dx_unique.compute()
    primary_dx_df.to_csv(writePath + "DX/{0}{1}{2}.csv".format("primary", "UTI", year), index=False)
    secondary_dx_df.to_csv(writePath + "DX/{0}{1}{2}.csv".format("secondary", "UTI", year), index=False)
    ## append years of diagnosis code to a list
    primary_dx_all.append(pd.read_csv(writePath + "DX/{0}{1}{2}.csv".format("primary", "UTI", year), low_memory=False))
    secondary_dx_all.append(pd.read_csv(writePath + "DX/{0}{1}{2}.csv".format("secondary", "UTI", year), low_memory=False))
primary_dx_all_df = pd.concat(primary_dx_all)
secondary_dx_all_df = pd.concat(secondary_dx_all)
primary_dx_all_df = primary_dx_all_df.sort_values(by='patid')
secondary_dx_all_df = secondary_dx_all_df.sort_values(by='patid')
## write all years of data to csv
primary_dx_all_df.to_csv(writePath + "DX/{0}{1}.csv".format("primary", "UTI"), index=False)
secondary_dx_all_df.to_csv(writePath + "DX/{0}{1}.csv".format("secondary", "UTI"), index=False)













