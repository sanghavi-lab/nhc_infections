import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
# client = Client("10.50.86.250:39748")

mdsPath = '/gpfs/data/cms-share/data/mds/{}/xwalk/parquet/'
mergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_FAC/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_STAR/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "secondary"]
outcome = ["UTI", "PNEU"]

# read in self-constructed data for nursing home characteristics such as Medicare population, dual population, race components...
nh_variables = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/nh_variables_capser.csv',
                             low_memory=False)
nh_variables = nh_variables.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
nh_variables = nh_variables.replace({np.nan: 0})

## read in five-star ratings file
star_09_15 = pd.read_sas('/gpfs/data/konetzka-lab/Data/5stars/NHCRatings/SAS/nhc_2009_2015.sas7bdat',
                         format='sas7bdat', encoding='latin-1')
star16_17 = [pd.read_csv(path) for path in
             ['/gpfs/data/cms-share/duas/55378/FiveStar/Rating/ProviderInfo_2016.csv',
              '/gpfs/data/cms-share/duas/55378/FiveStar/Rating/ProviderInfo_2017.csv']]

## read in Quality Measures (QMs) data
qm_11_17 = pd.read_csv('/gpfs/data/cms-share/duas/55378/FiveStar/QM/quality_measure_2011_2017.csv')
## subset to long-stay UTI measure
qm_uti = qm_11_17[qm_11_17['msr_cd']==407]

qm_uti = qm_uti.rename(columns={'MEASURE_SCORE': 'long_stay_uti'})

## Since these are quarterly QMs, we used the average ratings and QMs

## calculate average QMs each year for each nh
qm_uti_avg = \
    qm_uti.groupby(['provnum', 'year']) \
        ['long_stay_uti'].mean().reset_index()

# ## pre-process Five-star ratings data

## separate time column into year and quarter columns
star_09_15[['year', 'quarter']] = star_09_15.time.str.split('_', expand=True)
## remove the character "Q" from quarter column
star_09_15['quarter'] = star_09_15['quarter'].str.replace('Q', '')

star_09_15['year'] = pd.to_numeric(star_09_15['year'], errors='coerce')
star_09_15['quarter'] = pd.to_numeric(star_09_15['quarter'], errors='coerce')
star_09_15 = star_09_15.astype({'provnum': 'str'})

## select ratings form 2011 - 2015
star_11_15 = star_09_15[star_09_15['year']>=2011]

## calculate the average rating within each year for providers
## also record the latest rating within each year (normally the 4th quarter) for providers

## calculate average ratings each year
star_11_15_avg = \
    star_11_15.groupby(['provnum', 'year']) \
        [['overall_rating', 'quality_rating', 'survey_rating', 'staffing_rating', 'rn_staffing_rating']].\
        mean().reset_index()

## pre-process ratings in 2016-2017
star16_17 = pd.concat(star16_17)
star16_17.columns = [i.lower() for i in star16_17.columns]
## separate quarter column into year and quarter columns
star16_17[['year', 'quarter']] = star16_17.quarter.str.split('Q', expand=True)

star16_17['year'] = pd.to_numeric(star16_17['year'], errors='coerce')
star16_17['quarter'] = pd.to_numeric(star16_17['quarter'], errors='coerce')
star16_17 = star16_17.astype({'provnum': 'str'})

## calcualte average rating each year
star16_17_avg = \
    star16_17.groupby(['provnum', 'year'])\
        [['overall_rating', 'quality_rating', 'survey_rating', 'staffing_rating', 'rn_staffing_rating']].\
        mean().reset_index()

## combine ratings from 2011 - 2017
star = pd.concat([star_11_15_avg, star16_17_avg])

## merge all nursing homes NHC measures and medicare population count
nh_variables_new = nh_variables.merge(qm_uti_avg,
                                      left_on=['MCARE_ID', 'MEDPAR_YR_NUM'],
                                      right_on=['provnum', 'year'],
                                      how='left')
nh_variables_new = nh_variables_new.drop(columns=['provnum', 'year'])
nh_variables_new = nh_variables_new.merge(star,
                                          left_on=['MCARE_ID', 'MEDPAR_YR_NUM'],
                                          right_on=['provnum', 'year'],
                                          how='left')
print(nh_variables_new.shape[0]) #10992
nh_variables_new = nh_variables_new.drop(columns=['provnum', 'year'])
nh_variables_new.to_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/' + 'nh_variables_nhc_measures.csv',
                        index=False)
#

for ctype in claims_type:
    for health_outcome in outcome:
        ## read in hospital infection claims sample file
        df_lst = [dd.read_parquet(path) for path in
                  [mergePath + 'sameyear/{0}{1}'.format(ctype, health_outcome),
                   mergePath + 'notsameyear/{0}{1}'.format(ctype, health_outcome)]]
        df = dd.concat(df_lst)
        ## merge with nursing home characteristics for quality measures, ratings...
        df_ratings = df.merge(nh_variables_new,
                              on=['MCARE_ID', 'MEDPAR_YR_NUM'],
                              how='left',
                              suffixes=['', '_dup']) ## there might be columns with the same name
        df_ratings = df_ratings.drop(columns=[col for col in df_ratings.columns if col.endswith('_dup')])
        ## calculate the number of UTI/PNEU claims for each NH each year to calculate claims-based infection rates
        claims_rate = df.groupby(['MCARE_ID', 'MEDPAR_YR_NUM'])['MEDPAR_ID'].count().rename('nclaims').reset_index()

        df_ratings = df_ratings.merge(claims_rate, on=['MCARE_ID', 'MEDPAR_YR_NUM'], how='left')
        ## calculate claims-based UTI/PNEU rate for each NH each year
        df_ratings['{}_rate_medicare'.format(health_outcome.lower())] = df_ratings['nclaims']/df_ratings['medicare_count']

        # exclude claims with a pu_rate larger than 1
        print(df_ratings[df_ratings['{}_rate_medicare'.format(health_outcome.lower())] > 1].shape[0].compute())  # 0 claims primary; 17 for secondary
        df_ratings = df_ratings[df_ratings['{}_rate_medicare'.format(health_outcome.lower())] <= 1]

        df_ratings.to_parquet(writePath + '{0}{1}'.format(ctype, health_outcome))

