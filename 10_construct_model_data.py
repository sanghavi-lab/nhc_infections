## CREATE FINAL ANALYTICAL DATA IN CSV FOR HOSPITAL CLAIMS SAMPLE
## BY CREATING RESIDENT-LEVEL AND NURSING HOME-LEVEL VARIABLES

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
import datetime

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:39748")

mdsPath = '/gpfs/data/cms-share/data/mds/{}/xwalk/parquet/'
mergePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CDISCHRG_STAR/'
readmPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/CREADMISSION_UTI/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/new/'
analysisPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/infection/initial_analysis/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/test/'

years = range(2011, 2018)
claims_type = ["primary", "secondary"]
outcome = ["UTI", "PNEU"]

## read in state code cross walk
statecode = pd.read_csv('/gpfs/data/sanghavi-lab/Pan/NH/data/statexwalk/statecode.csv')

for health_outcome in outcome:
    ## read in comorbidity score for primary and secondary hospital infection claims sample
    comorbidity_p = pd.read_csv(
        '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/' + 'DX/{}pcomorbidity.csv'.format(
            health_outcome))
    comorbidity_s = pd.read_csv(
        '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/' + 'DX/{}scomorbidity.csv'.format(
            health_outcome))
    comorbidity = pd.concat([comorbidity_p, comorbidity_s])
    for ctype in claims_type:
        print(ctype, health_outcome)

        ## read in hospital claims sample data
        df = dd.read_parquet(mergePath + '{0}{1}'.format(ctype, health_outcome))
        # print(df.shape[0].compute())

        # ## this block of code is only used for UTI readmission data #####################
        # df = dd.read_parquet([readmPath + '{0}/{1}'.format(ctype, year) for year in range(2011, 2018)])
        # readmission_cols = [col for col in df.columns if (col.endswith('_readmission') & (col!='days_readmission'))]
        # rename_cols = [col.replace('_readmission', '') for col in readmission_cols if col!='days_readmission']
        # rename_col_dict = dict(zip(readmission_cols, rename_cols))
        # print(rename_col_dict)
        # df = df.rename(columns=rename_col_dict)
        ##################################################################################

        ## create variable for length of stay
        df['los'] = (df['DSCHRG_DT'] - df['ADMSN_DT']).dt.days

        ## define diagnosis code items in MDS
        mds_icd_items = ['I8000A_ICD_1_CD', 'I8000B_ICD_2_CD', 'I8000C_ICD_3_CD', 'I8000D_ICD_4_CD',
                         'I8000E_ICD_5_CD', 'I8000F_ICD_6_CD', 'I8000G_ICD_7_CD', 'I8000H_ICD_8_CD',
                         'I8000I_ICD_9_CD', 'I8000J_ICD_10_CD']

        ## read in icd codes for identifying UTI/pneumonia hospitalization and see how many of them are coded in MDS discharge/post-hospitalization assessments
        icd = pd.read_csv(
            '/gpfs/data/cms-share/duas/55378/Zoey/gardner/gitlab_code/nhc_infections/code/initial_analysis/icd.csv')
        icd = icd.astype({'icd': 'str'})

        if health_outcome == 'UTI':
            ## subset to UTI diagosis code
            icd_uti = icd[icd['outcome'] == 'uti']
            ## create an indicator for whether MDS has diagnosis codes in I8000 that are related to UTI
            df['mds_uti_diag'] = df[mds_icd_items].isin(list(icd_uti['icd'])).any(axis=1)
            # print(df['mds_uti_diag'].mean().compute()) # only about 3% have a uti diagnosis coding in MDS
            dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
        elif health_outcome == 'PNEU':
            ## create an indicator for whether MDS has diagnosis codes in I8000 that are related to pneumonia
            icd_pneu = icd[icd['outcome'] == 'pneumonia']
            df['mds_pneu_diag'] = df[mds_icd_items].isin(list(icd_pneu['icd'])).any(axis=1)

        ## remove the last three-month of data in 2015 to allow for error during diagnosis coding transition
        df = df[~((df['ADMSN_DT'] < datetime.datetime(2016, 1, 1)) & (df['ADMSN_DT'] > datetime.datetime(2015, 10, 1)))]
        # print(df.shape[0].compute())
        ## merge sample data with comorbidity score
        df = df.merge(comorbidity, left_on='MEDPAR_ID', right_on='patid')
        # print(df.shape[0].compute())
        ## merges sample data with state code crosswalk
        df = df.merge(statecode, left_on='STATE_CD', right_on='state', how='left')
        df['state_code'] = df['state_code'].apply(lambda x: '{:02d}'.format(x))

        ## 1) create individual-level variables
        # create dual indicator;n
        # a patient is considered a dual if he/she is a full dual in any month of hospital admission year for infection;
        dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]

        ## dual is True if the patient is a full dual in any month of the hospitalization year
        df['dual'] = df[dual].isin(['02', '04', '08']).any(axis=1)

        ## create female and disability indicators
        df['female'] = df['sex'] == '2'
        df['disability'] = df['ENTLMT_RSN_CURR'] == '1'

        ## remove missing values in important covariates
        cc_code = ['AMI', 'ALZH', 'ALZH_DEMEN', 'ATRIAL_FIB', 'CATARACT',
                   'CHRONICKIDNEY', 'COPD', 'CHF', 'DIABETES', 'GLAUCOMA',
                   'HIP_FRACTURE', 'ISCHEMICHEART', 'DEPRESSION',
                   'OSTEOPOROSIS', 'RA_OA', 'STROKE_TIA', 'CANCER_BREAST',
                   'CANCER_COLORECTAL', 'CANCER_PROSTATE', 'CANCER_LUNG', 'CANCER_ENDOMETRIAL', 'ANEMIA',
                   'ASTHMA', 'HYPERL', 'HYPERP', 'HYPERT', 'HYPOTH']
        cc_code_final = [cc + '_final' for cc in cc_code]

        df = df.dropna(
            subset=['BENE_ID', 'MEDPAR_ID', 'MEDPAR_YR_NUM', 'MCARE_ID', 'race_RTI', 'female', 'dual', 'age',
                    'disability', 'combinedscore', 'short_stay', 'state_code', '{}_rate_medicare'.format(health_outcome.lower())] + cc_code_final)

        ## clean the race variable:
        ## remove missing race
        df = df[df['race_RTI'] != '0']
        ## rename race variable
        replace_race = {'1': 'white', '2': 'black', '3': 'other', '4': 'asian', '5': 'hispanic', '6': 'american_indian'}

        df = df.assign(race_name=df['race_RTI'].replace(replace_race))

        ## create 3 equal age_bins out of the continuous age variable according to age distribution
        df = df.astype({'age': 'int'})
        bins_age = df['age'].compute().quantile([0.25, 0.5, 0.75]).to_list()
        bins_age.insert(0, 0)
        bins_age.append(140)

        label_age = ['<={}'.format(bins_age[1]),
                     '{0}_{1}'.format(bins_age[1] + 1, bins_age[2]),
                     '{0}_{1}'.format(bins_age[2] + 1, bins_age[3]),
                     '>={}'.format(bins_age[3] + 1)]

        df['age_bin'] = df['age'].map_partitions(pd.cut, bins=bins_age, labels=label_age)
        df = df.astype({'age_bin': 'str'})

        ## categorize nursing homes into small, medium and large
        ## according to their total number of residents

        bins_size = df['CNSUS_RSDNT_CNT'].compute().quantile([0.33, 0.66]).to_list()
        bins_size.insert(0, 0)
        bins_size.append(10000)

        labels_size = ['s',
                       'm',
                       'l']

        df['size'] = df['CNSUS_RSDNT_CNT'].map_partitions(pd.cut, bins=bins_size, labels=labels_size)

        ## categorize nursing homes into northeast, midwest, south or west nursign homes by their location
        replace_region = {"09": 'northeast', "23": "northeast", "25": 'northeast', "33": 'northeast', '34': 'northeast', '36': 'northeast', '42': 'northeast', '44': 'northeast', '50': 'northeast',
                          '17': 'midwest', '18': 'midwest', '19': 'midwest', '26': 'midwest', '39': 'midwest', '55': 'midwest', '20': 'midwest', '27': 'midwest', '29': 'midwest', '31': 'midwest', '38': 'midwest', '46': 'midwest',
                          '01': 'south', '05': 'south', '22': 'south', '10': 'south', '11': 'south', '12': 'south', '13': 'south', '21': 'south', '24': 'south', '28': 'south', '37': 'south', '40': 'south', '45': 'south', '47': 'south','48': 'south', '51': 'south', '54': 'south', '78': 'south',
                          '02': 'west', '06': 'west', '04': 'west', '08': 'west', '15': 'west', '16': 'west', '30':'west', '32': 'west', '35': 'west', '41': 'west', '49': 'west', '53': 'west', '56': 'west'}
        df = df.assign(region=df['state_code'].replace(replace_region))
        # print(df['region'].unique().compute()) ## secondary claims has an unassigned value - 72

        # ## categorize nursing homes into for-profit, non-profit and government based on their ownership
        replace_ownership = {1: 'for-profit', 2: 'for-profit', 3: 'for-profit', 13: 'for-profit',
                             4: 'non-profit', 5: 'non-profit', 6: 'non-profit',
                             7: 'government' , 8: 'government', 9: 'government', 10: 'government', 11: 'government', 12: 'government'
                             }
        df = df.assign(ownership=df['GNRL_CNTL_TYPE_CD'].replace(replace_ownership))

        # ## 2.1) create facility-level variables for race and dual,
        # ##    e.g. the percentage of dual residents in a nursing home
        ## create race dummy variable
        race_dummy_cols = ['white', 'black', 'other', 'asian', 'hispanic', 'american_indian']
        for col in race_dummy_cols:
            df[col] = df['race_name'] == col
        race_mix_cols = ['white_percent', 'black_percent', 'other_percent', 'asian_percent', 'hispanic_percent',
                         'american_indian_percent']
        race_dev_cols = ['{}_dev'.format(i) for i in race_dummy_cols]

        # ## drop observations if all race mix is zero
        # print('number of claims where all race mix zero:')
        # print(df[(df[race_mix_cols]==0).all(axis=1)].shape[0])

        # df = df[~(df[race_mix_cols]==0).all(axis=1)]
        # construct race_deviation and dual_deviation variable, which are resident-level variables for race and dual status
        df['dual_dev'] = df['dual'] - df['dual_percent']
        df['white_dev'] = df['white'] - df['white_percent']
        df['black_dev'] = df['black'] - df['black_percent']
        df['asian_dev'] = df['asian'] - df['asian_percent']
        df['hispanic_dev'] = df['hispanic'] - df['hispanic_percent']
        df['other_dev'] = df['other'] - df['other_percent']
        df['american_indian_dev'] = df['american_indian'] - df['american_indian_percent']
        ## create a list of quality measure columns
        quality_cols = ['long_stay_uti', 'overall_rating', 'quality_rating', 'survey rating']

        UTI_cols = ['I2300_UTI_CD', 'mds_uti_diag', 'major_uti', 'severe_uti', 'def_uti', 'H0100A_INDWLG_CTHTR_CD']
        PNEU_cols = ['I2000_PNEUMO_CD', 'mds_pneu_diag']

        ## create a list of columns for UTI or PNEUMONIA final data
        use_cols = ['BENE_ID', 'MEDPAR_ID', 'los', 'MDS_ASMT_ID', 'MEDPAR_YR_NUM', 'MCARE_ID', 'race_name', 'female', 'age',
                    'dual', 'dual_dev', 'age_bin', 'disability', 'combinedscore', 'short_stay', 'CNSUS_MDCR_CNT',
                    'CNSUS_RSDNT_CNT', 'medicare_count', 'dual_percent', 'size', 'region', 'ownership',
                    'nclaims', '{}_rate_medicare'.format(health_outcome.lower())] + \
                   cc_code_final + race_mix_cols + race_dev_cols
        ## select a subset of columns for final analytical sample data
        if health_outcome=='PNEU':
            df_model_final = df[use_cols + quality_cols + PNEU_cols]
        if health_outcome=='UTI':
            df_model_final = df[use_cols + quality_cols + UTI_cols]
        # print(df['H0100A_INDWLG_CTHTR_CD'].value_counts().compute())
        ## the name of output data has "_readmission" only if it is the readmission MDS data for UTI
        df_model_final.compute().to_csv(writePath + "{0}{1}.csv".format(ctype, health_outcome))





