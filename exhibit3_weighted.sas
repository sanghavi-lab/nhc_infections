*THIS CODE IS TO CALCULATE WEIGHTED 25TH AND 75TH PERCENTILE OF NURSING HOME REPORTING RATE FOR EXHIBIT 3;
*https://blogs.sas.com/content/iml/2016/08/29/weighted-percentiles.html;

proc import datafile="\\prfs.cri.uchicago.edu\cms-share\duas\55378\Zoey\gardner\data\exhibits\infection\FINAL\exhibit3_final\SNFprimaryUTIReportingByNH.csv"
		out=main
		dbms=csv
		replace;
		guessingrows=7000;
run;

proc print data=main(obs=3);
run;

ods csvall file='\\prfs.cri.uchicago.edu\cms-share\duas\55378\Zoey\gardner\data\exhibits\infection\FINAL\quintileSNFPrimaryUTIReporting.csv';
proc means data=main p25 p75;
	class race_name;
	var I2300_UTI_CD;
	weight nh_claims_weight;
run;
ods csvall close;
