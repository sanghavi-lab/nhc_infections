
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
from scipy import stats
from dask.distributed import Client
client = Client("10.50.86.250:53871")


pd.set_option('display.max_columns', 500)

years = range(2011, 2018)
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/exhibits/infection/FINAL/'

## read in final dataset
df = pd.read_csv(inputPath + 'new/primaryUTI.csv', low_memory=False)
df = df.astype({'MCARE_ID': 'str'})

# # <editor-fold desc="CALCULATE CORRELATION BETWEEN CLAIMS-BASED PU RATES AND NHC COMPARE MEASURES">
## define QM and rating related columns
qmcols = ['long_stay_uti', 'overall_rating', 'quality_rating', 'survey_rating']

## remove missing values
df = df.dropna(subset=qmcols, how='any')
# print(df.shape[0]) #96950
## keep unique nursing home-year data
df = df.drop_duplicates(subset=['MCARE_ID', 'MEDPAR_YR_NUM'])

## divide claims-based uti/pneumonia rate to quintiles;
## calculate mean and 10th/90th percentile within each quintile
## compute percent of NHs with 4/5 star Overall/Quality ratings within each quintile
## compute average Overall and Quality star-rating, and MDS-based UTI scores within each quintile
for n in range(len(years)):
    ## select data for each year
    rating = df[df.MEDPAR_YR_NUM==years[n]][qmcols + ['MCARE_ID', 'uti_rate_medicare']]
    ## divide claims-based pressure ulcer rate into 5 quantiles
    rating_quintile = rating['uti_rate_medicare'].quantile([0.2, 0.4, 0.6, 0.8]).to_list()

    conditions_quintile = [(rating['uti_rate_medicare'] < rating_quintile[0]),
                            ((rating['uti_rate_medicare'] >= rating_quintile[0]) & (rating['uti_rate_medicare'] < rating_quintile[1])),
                            ((rating['uti_rate_medicare'] >= rating_quintile[1]) & (rating['uti_rate_medicare'] < rating_quintile[2])),
                            ((rating['uti_rate_medicare'] >= rating_quintile[2]) & (rating['uti_rate_medicare'] < rating_quintile[3])),
                            (rating['uti_rate_medicare'] >= rating_quintile[3])
                            ]
    ## assign quintile names
    values_quintile = ['quintile1', 'quintile2', 'quintile3', 'quintile4', 'quintile5']
    rating['quintile'] = np.select(conditions_quintile, values_quintile)
    ## calculate the number of nursing homes within each quintile for 2014 and 2017
    if (n==3) | (n==6):
        rating.groupby(['quintile'])['MCARE_ID'].count().to_csv(writePath + 'exhibit5_nh_size{}.csv'.format(years[n]))

    ## calculate the mean claims-based infection rate and NHC measures within each quintile
    rating_mean = rating.groupby('quintile').mean().reset_index()
    rating_mean.columns = ['quintile', 'long_stay_uti_mean', 'overall_rating_mean', 'quality_rating_mean', 'survey_rating_mean',
                           'uti_rate_medicare_mean']

    ## calculate the 10th and 90th percentile of claims-based rate within each quintile
    rating_10_90 = rating.groupby('quintile')['uti_rate_medicare'].quantile([0.1, 0.9]).reset_index()
    rating_10_90 = rating_10_90.pivot(index='quintile', columns='level_1').reset_index()
    rating_10_90.columns = rating_10_90.columns.to_flat_index()
    col = rating_10_90.columns
    col = pd.Index([str(e[0]) + str(e[1]) for e in col.tolist()])
    rating_10_90.columns = col

    # ## calculate the percentage of nursing homes with 4- or 5-star overall rating/quality rating within each quintile
    rating_percent_highstar = \
        rating.groupby('quintile') \
            [['overall_rating', 'quality_rating', 'survey_rating']].apply(lambda x: (x>=4).sum()/x.count()).reset_index()
    rating_percent_highstar.columns = \
        ['quintile', 'overall_rating_percent_4_or_5_star', 'quality_rating_percent_4_or_5_star', 'survey_rating_percent_4_or_5_star']
    ## merge all results data
    rating_table_final = pd.merge(pd.merge(rating_mean, rating_10_90, on='quintile', suffixes=['_mean', '']), rating_percent_highstar, on='quintile')
    rating_table_final.to_csv(writePath + 'exhibit5_main_uti_rate_medicare{}_quintile_QM.csv'.format(years[n]))
# # # </editor-fold>

## select nursing homes with no infection claims and calculate their mean NHC measures

## read in nursing home population data
nh_population = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/' + 'nh_variables_nhc_measures.csv',
                            low_memory=False)

## drop nurisng homes with missing MCARE_ID
nh_population = nh_population.dropna(subset=['MCARE_ID'])


## drop nursing homes with missing NHC measures
nh_population = nh_population.dropna(subset=qmcols)
print(nh_population.shape[0])#91842

## merge with primary infection hospital claims sample data
nh_population_merge = \
    nh_population[['MCARE_ID', 'MEDPAR_YR_NUM', 'overall_rating', 'quality_rating', 'survey_rating',
                   'long_stay_uti']]. \
    merge(df[['MCARE_ID', 'MEDPAR_YR_NUM', 'MEDPAR_ID','uti_rate_medicare']], on=['MCARE_ID', 'MEDPAR_YR_NUM'],
          how='left')
## fill missing uti_rate_medicare with 0
nh_population_merge['uti_rate_medicare'] = nh_population_merge['uti_rate_medicare'].fillna(value=0)
print(nh_population_merge['MEDPAR_YR_NUM'].value_counts())

# ## calculate the correlation between uti_rate_medicare and NHC measures for all nursing homes
nh_population_merge_2014 = nh_population_merge[nh_population_merge['MEDPAR_YR_NUM']==2014]
nh_population_merge_2017 = nh_population_merge[nh_population_merge['MEDPAR_YR_NUM']==2017]
#
print(stats.spearmanr(nh_population_merge_2014['uti_rate_medicare'], nh_population_merge_2014['overall_rating']))
print(stats.spearmanr(nh_population_merge_2014['uti_rate_medicare'], nh_population_merge_2014['quality_rating']))
print(stats.spearmanr(nh_population_merge_2014['uti_rate_medicare'], nh_population_merge_2014['survey_rating']))
print(stats.pearsonr(nh_population_merge_2014['uti_rate_medicare'], nh_population_merge_2014['long_stay_uti']))

print(stats.spearmanr(nh_population_merge_2017['uti_rate_medicare'], nh_population_merge_2017['overall_rating']))
print(stats.spearmanr(nh_population_merge_2017['uti_rate_medicare'], nh_population_merge_2017['quality_rating']))
print(stats.spearmanr(nh_population_merge_2017['uti_rate_medicare'], nh_population_merge_2017['survey_rating']))
print(stats.pearsonr(nh_population_merge_2017['uti_rate_medicare'], nh_population_merge_2017['long_stay_uti']))
#
#
## select nursing homes with no primary UTI hospital claims
nh_population_nopu = nh_population_merge[nh_population_merge['MEDPAR_ID'].isna()]

## calculcate the number of nursing homes with no UTI claims
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')['MCARE_ID'].count())

## calculate mean NHC measures
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')\
          [qmcols].\
      mean())
## calculate percentage 4- or 5-star
nh_population_nopu['high_star_overall'] = nh_population_nopu['overall_rating']>=4
nh_population_nopu['high_star_quality'] = nh_population_nopu['quality_rating']>=4
nh_population_nopu['high_star_survey'] = nh_population_nopu['survey_rating']>=4
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')[['high_star_overall', 'high_star_quality', 'high_star_survey']].mean())


## repeat the above steps for pneumonia sample

## read in final dataset
df = pd.read_csv(inputPath + 'primaryPNEU.csv', low_memory=False)
df = df.astype({'MCARE_ID': 'str'})

## define QM and rating related columns
qmcols = ['overall_rating', 'quality_rating']


## remove missing values
df = df.dropna(subset=qmcols, how='any')
## keep unique nursing home-year data
df = df.drop_duplicates(subset=['MCARE_ID', 'MEDPAR_YR_NUM'])
#
## divide claims-based pu rate to quintiles;
## calculate mean and 10th/90th percentile within each quintile
## compute percent of NHs with 4/5 star Overall/Quality ratings within each quintile
## compute average Overall and Quality star-rating, and MDS-based pressure ulcer scores within each quintile
for n in range(len(years)):
    ## select data for each year
    rating = df[df.MEDPAR_YR_NUM==years[n]][qmcols + ['MCARE_ID', 'pneu_rate_medicare']]
    ## divide claims-based pressure ulcer rate into 5 quantiles
    rating_quintile = rating['pneu_rate_medicare'].quantile([0.2, 0.4, 0.6, 0.8]).to_list()

    conditions_quintile = [(rating['pneu_rate_medicare'] < rating_quintile[0]),
                            ((rating['pneu_rate_medicare'] >= rating_quintile[0]) & (rating['pneu_rate_medicare'] < rating_quintile[1])),
                            ((rating['pneu_rate_medicare'] >= rating_quintile[1]) & (rating['pneu_rate_medicare'] < rating_quintile[2])),
                            ((rating['pneu_rate_medicare'] >= rating_quintile[2]) & (rating['pneu_rate_medicare'] < rating_quintile[3])),
                            (rating['pneu_rate_medicare'] >= rating_quintile[3])
                            ]
    ## assign quintile names
    values_quintile = ['quintile1', 'quintile2', 'quintile3', 'quintile4', 'quintile5']
    rating['quintile'] = np.select(conditions_quintile, values_quintile)
    ## calculate the number of nursing homes within each quintile for 2014 and 2017
    if (n==3) | (n==6):
        rating.groupby(['quintile'])['MCARE_ID'].count().to_csv(writePath + 'exhibit5_pneu_nh_size{}.csv'.format(years[n]))
    ## calculate the mean claims-based pu rate and NHC measures within each quintile
    rating_mean = rating.groupby('quintile').mean().reset_index()
    rating_mean.columns = ['quintile',  'overall_rating_mean', 'quality_rating_mean',
                           'pneu_rate_medicare']

    ## calculate the 10th and 90th percentile of claims-based rate within each quintile
    rating_10_90 = rating.groupby('quintile')['pneu_rate_medicare'].quantile([0.1, 0.9]).reset_index()
    rating_10_90 = rating_10_90.pivot(index='quintile', columns='level_1').reset_index()
    rating_10_90.columns = rating_10_90.columns.to_flat_index()
    col = rating_10_90.columns
    col = pd.Index([str(e[0]) + str(e[1]) for e in col.tolist()])
    rating_10_90.columns = col

    ## calculate the percentage of nursing homes with 4- or 5-star overall rating/quality rating within each quintile
    rating_percent_highstar = \
        rating.groupby('quintile') \
            [['overall_rating', 'quality_rating']].apply(lambda x: (x>=4).sum()/x.count()).reset_index()
    rating_percent_highstar.columns = \
        ['quintile', 'overall_rating_percent_4_or_5_star', 'quality_rating_percent_4_or_5_star']
    ## merge all results data
    rating_table_final = pd.merge(pd.merge(rating_mean, rating_10_90, on='quintile', suffixes=['_mean', '']), rating_percent_highstar, on='quintile')
    rating_table_final.to_csv(writePath + 'exhibit5_main_pneu_rate_medicare{}_quintile_QM.csv'.format(years[n]))


## read in nursing home population data
nh_population = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/' + 'nh_variables_nhc_measures.csv',
                            low_memory=False)

## drop nurisng homes with missing MCARE_ID
nh_population = nh_population.dropna(subset=['MCARE_ID'])


## drop nursing homes with missing NHC measures
nh_population = nh_population.dropna(subset=['overall_rating', 'quality_rating'])
print(nh_population.shape[0])#107988

## merge with primary pneumonia hospital claims analytical sample data
nh_population_merge = \
    nh_population[['MCARE_ID', 'MEDPAR_YR_NUM', 'overall_rating', 'quality_rating']]. \
    merge(df[['MCARE_ID', 'MEDPAR_YR_NUM', 'MEDPAR_ID','pneu_rate_medicare']], on=['MCARE_ID', 'MEDPAR_YR_NUM'],
          how='left')
## fill missing pneumonia_rate_medicare with 0
nh_population_merge['pneu_rate_medicare'] = nh_population_merge['pneu_rate_medicare'].fillna(value=0)
print(nh_population_merge['MEDPAR_YR_NUM'].value_counts())

## calculate the correlation between pneumonia_rate_medicare and NHC measures for all nursing homes
nh_population_merge.\
    groupby('MEDPAR_YR_NUM')\
    [['pneu_rate_medicare', 'overall_rating', 'quality_rating']].\
    corr(method='spearman').to_csv(
    writePath + 'main_spearmancorr_pneu_rate_medicare_and_qm_score_byyear_all_nh.csv'
)

nh_population_merge_2014 = nh_population_merge[nh_population_merge['MEDPAR_YR_NUM']==2014]
nh_population_merge_2017 = nh_population_merge[nh_population_merge['MEDPAR_YR_NUM']==2017]

print(stats.spearmanr(nh_population_merge_2014['pneu_rate_medicare'], nh_population_merge_2014['overall_rating']))
print(stats.spearmanr(nh_population_merge_2014['pneu_rate_medicare'], nh_population_merge_2014['quality_rating']))

print(stats.spearmanr(nh_population_merge_2017['pneu_rate_medicare'], nh_population_merge_2017['overall_rating']))
print(stats.spearmanr(nh_population_merge_2017['pneu_rate_medicare'], nh_population_merge_2017['quality_rating']))


## select nursing homes with no primary pneumonia claims
nh_population_nopu = nh_population_merge[nh_population_merge['MEDPAR_ID'].isna()]

## calculcate the number of nursing homes with no pneumonia claims
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')['MCARE_ID'].count())

## calculate mean NHC measures
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')\
          [qmcols].\
      mean())
## calculate percentage 4- or 5-star
nh_population_nopu['high_star_overall'] = nh_population_nopu['overall_rating']>=4
nh_population_nopu['high_star_quality'] = nh_population_nopu['quality_rating']>=4
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')[['high_star_overall', 'high_star_quality']].mean())

