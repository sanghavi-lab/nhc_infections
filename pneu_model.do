local indir "//prfs.cri.uchicago.edu/cms-share/duas/55378/Zoey/gardner/data/merge_output/infection/medpar_mds/FINAL/new/"

*log using `"`logdir'/Exhibit4.log"', text replace

cd "`indir'"

*load analysis dataset
import delimited using primaryPNEU, clear

*this line of code is only for pneumonia; 72 is porte rico; not occured in other health outcomes
// drop if region=="72"
*set up factor variables

*region
encode region, gen(region_n)

*size
egen size_n=group(size)
label define size_n 1 "large" 2 "medium" 3 "small"
label values size_n size_n

*ownership
encode ownership, gen(ownership_n)

*age
encode age_bin, gen(age_n)

*race
egen race_n=group(race_name)
label define race_n 1 "american_indian" 2 "asian" 3 "black" 4 "hispanic" 5 "other" 6 "white"
labe values race_n race_n

*female
egen female_n=group(female)
label define female_n 1 "male" 2 "female"
label values female_n female_n

*short_stay
egen short_stay_n=group(short_stay)
label define short_stay_n 1 "long-stay" 2 "short-stay"
label values short_stay_n short_stay_n

*disability
egen disability_n=group(disability)
label define disability_n 1 "no-disability" 2 "disability"
label values disability_n disability_n

*set panel variable
encode mcare_id, gen(prvdrnum)
xtset prvdrnum



*create varlists
global race_dev "american_indian_dev asian_dev black_dev hispanic_dev other_dev"
global race_percentage "american_indian_percent asian_percent black_percent hispanic_percent other_percent"

global cc "ami_final alzh_final alzh_demen_final atrial_fib_final cataract_final chronickidney_final copd_final chf_final diabetes_final glaucoma_final hip_fracture_final ischemicheart_final depression_final osteoporosis_final ra_oa_final stroke_tia_final cancer_breast_final   cancer_colorectal_final cancer_prostate_final cancer_lung_final cancer_endometrial_final anemia_final asthma_final hyperl_final hyperp_final hypert_final hypoth_final"
global other ///
	  i.female_n ib(last).age_n i.disability_n dual_dev combinedscore i.medpar_yr_num ///
	  ami_final alzh_final alzh_demen_final atrial_fib_final cataract_final chronickidney_final ///
	  copd_final chf_final diabetes_final glaucoma_final hip_fracture_final ischemicheart_final ///
	  depression_final osteoporosis_final ra_oa_final stroke_tia_final cancer_breast_final ///
	  cancer_colorectal_final cancer_prostate_final cancer_lung_final cancer_endometrial_final ///
	  anemia_final asthma_final hyperl_final hyperp_final hypert_final hypoth_final ///	
	  dual_percent ib(first).ownership_n ib(first).region_n ib(first).size_n

*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------;
*short-stay
					
// *run CMC full model	
eststo shortstay_logit: melogit i2000_pneumo_cd $race_dev $race_percentage $other ///
						if short_stay_n==2 || prvdrnum:
						
esttab shortstay_logit using pneu_shortstaylogit_regression.csv, se nogap label	replace	

*predit reporting probability
foreach black_percent of numlist 0 1 5{
	*white resident
	local black_dev0=-`black_percent'*0.1 
	*black resident
	local black_dev1=1-`black_percent'*0.1
	local pblack=`black_percent'*0.1
	
	*predict using logit model
	estimates restore shortstay_logit
	eststo plogit`black_percent': ///
		margins, at(black_dev=(`black_dev0' `black_dev1') american_indian_dev=0 asian_dev=0 hispanic_dev=0 other_dev=0 ///
					black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.00 other_percent=0) post
}

esttab plogit0 plogit1 plogit5 using PNEUshortstay_predictions.csv, replace nostar nogap ci	

*calculate difference between black and white estimates
estimates restore plogit0
lincom _b[1bn._at]-_b[2._at]

estimates restore plogit1
lincom _b[1bn._at]-_b[2._at]

estimates restore plogit5
lincom _b[1bn._at]-_b[2._at]

			  
*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------;

*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------;
*long-stay

eststo longstay_logit: melogit i2000_pneumo_cd $race_dev $race_percentage $other ///
						if short_stay_n==1 || prvdrnum:
					
esttab longstay_logit using pneu_longstaylogit_regression.csv, se nogap label replace


* CALCULATE PREDICTIVE REPORTING RATES FOR LONG-STAY RESIDENTS

foreach black_percent of numlist 0 1 5{
	*white resident
	local black_dev0=-`black_percent'*0.1 
	*black resident
	local black_dev1=1-`black_percent'*0.1
	local pblack=`black_percent'*0.1
	
	*predict using logit model
	estimates restore longstay_logit
	eststo pllogit`black_percent': ///
		margins, at(black_dev=(`black_dev0' `black_dev1') american_indian_dev=0 asian_dev=0 hispanic_dev=0 other_dev=0 ///
					black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.00 other_percent=0) post
}

esttab pllogit0 pllogit1 pllogit5 using PNEUlongstay_predictions.csv, replace nostar nogap ci	

*calculate difference between black and white estimates
estimates restore pllogit0
lincom _b[1bn._at]-_b[2._at]

estimates restore pllogit1
lincom _b[1bn._at]-_b[2._at]

estimates restore pllogit5
lincom _b[1bn._at]-_b[2._at]


