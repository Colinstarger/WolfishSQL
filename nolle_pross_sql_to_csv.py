#python3
#Nolle Pross SQL to CSV
import pandas as pd
from math import pi
import mysql.connector, datetime, calendar, random, sys
from clue_passwords import * # This gets username and password for CLUE

#Hard Code
hostname = 'clue.ctdfdskvfoc5.us-east-1.rds.amazonaws.com'
DISTRICT = 'criminal_district_regular'

jurisdictions = ("Baltimore City", )
circuit_db = {"Baltimore City": "criminal_circuit_baltimore_city", "Baltimore County":"criminal_circuit_county_regular", "Montgomery County": "criminal_circuit_montgomery", "Prince George's County":"criminal_circuit_pg"}

circuit_prefix = {"Baltimore City": "balt_city_circuit_", "Baltimore County":"balt_county_circuit_", "Montgomery County": "mont_county_circuit_", "Prince George's County":"pg_county_circuit_"}

myDistrictConnection = mysql.connector.connect( host=hostname, user=username, passwd=password, db=DISTRICT)


#Normalize Data Dictionaries
race_normalize = {"nan": "nan", "African American":"BLACK" , "White":"WHITE" , "Unknown":"UNKNOWN" , "Other":"OTHER" , "Native American": "INDIAN", "Caucasian": "WHITE", "Other Asian": "Other Asian", "Hispanic": "Hispanic" }

sex_normalize = {'M':"MALE", 'F':"FEMALE", "nan":"nan", 'U':"UNKNOWN", 'f':"FEMALE", 'm':"MALE", 'Male': "MALE", 'Female': 'FEMALE'}

dispo_normalize = {
'6-220 PBJ': 'PROBATION BEFORE JUDGEMENT', 
'PBJ (6-220)': 'PROBATION BEFORE JUDGEMENT', 
'PROBATION BEFORE JUDGMENT': 'PROBATION BEFORE JUDGEMENT',
'Stet': 'STET', 
'Nolle Prosequi': 'NOLLE PROSEQUI', 
'Nolle Pros': 'NOLLE PROSEQUI', 
'Guilty': 'CLOSED - JEOPARDY OR OTHER CONVICTION', 
'GUILTY': 'CLOSED - JEOPARDY OR OTHER CONVICTION', 
'Not Guilty': 'NOT GUILTY', 
'Appeal Withdrawn': 'APPEAL WITHDRAWN', 
'Abated By Death': 'ABATED BY DEATH', 
'Abate by Death': 'ABATED BY DEATH',  
'Appeal Dismissed': 'APPEAL DISMISSED',
'Appeal Dismissed/Court': 'APPEAL DISMISSED',  
"nan":"nan", 
'Aquitted':'ACQUITTAL JUDGMENT GRANTED',
'Judgment Acquittal':'ACQUITTAL JUDGMENT GRANTED',
'ACQUITTED': 'ACQUITTAL JUDGMENT GRANTED',  
'No Verdict': 'VERDICT NOT RENDERED', 
'No Finding': 'VERDICT NOT RENDERED', 
'Not Criminally Responsibl':'NOT CRIMINALLY RESPONSIBLE - COMMITTED',
'Not Criminally Responsible':'NOT CRIMINALLY RESPONSIBLE - COMMITTED',
'Dismissed':'DISMISSED', 
'Dismissed as Duplicate':'DISMISSED', 
'Dismissed With Prejudice':'DISMISSED', 
'Remanded To District Cour':'REMANDED TO DISTRICT COURT', 
'Remand - District Court':'REMANDED TO DISTRICT COURT',  
'CASE RETURNED TO DISTRICT COURT': 'REMANDED TO DISTRICT COURT',
'Incompetent To Stand Tria': 'INCOMPETENT TO STAND TRIAL - COMMITTED', 
'Merged':'MERGED WITH OTHER COUNTS', 
'Reverse Waiver': 'REVERSE WAIVER', 
'PBJ 641':'PROBATION BEFORE JUDGEMENT', 
'Mistrial': 'MISTRIAL',
'WITHDRAWN BY DEF. JUDGMENT OF DIST. CT.': 'WITHDRAWN',
'GUILTY - DOMESTIC RLTD': 'GUILTY',
'DISMISSED BY COURT JUDGMENT OF DIST. CT.': 'DISMISSED',
'MERGED': 'MERGED WITH OTHER COUNTS',
'MORT-EST': 'MORT-EST',
'NOT CRIMINALLY RESPONSIBLE': 'NOT CRIMINALLY RESPONSIBLE - COMMITTED',
'FINED': 'FINED',
'REMANDED TO THE JUVENILE COURT': 'REMANDED TO THE JUVENILE COURT',
'RETURNED TO JUVENILE COURT': 'REMANDED TO THE JUVENILE COURT',
'Remand Juvenile':'REMANDED TO THE JUVENILE COURT',
'VOP WITHDRAWN/DISMISSED': 'VOP WITHDRAWN/DISMISSED',
'APPEAL WITHDRAWN BY DEFENDANT': 'APPEAL WITHDRAWN',
'PROBATION BEFORE JUDGMENT - DOMESTIC RLTD': 'PROBATION BEFORE JUDGEMENT',
'CHANGE OF VENUE': 'CHANGE OF VENUE',
'Vacated': 'VACATED', 
'NOLO CONTENDERE': 'NOLO CONTENDERE'
}

 


charge_normalize= {
"ASSAULT-SEC DEGREE":"ASSAULT-SEC DEGREE",
"Assault 2nd Degree":"ASSAULT-SEC DEGREE",
"ASSAULT-SECOND DEGREE":"ASSAULT-SEC DEGREE",
"Assault-Second Degree": "ASSAULT-SEC DEGREE",
"THEFT: LESS $1,000 VALUE": "THEFT: LESS $1,000 VALUE",
"THEFT UNDER $1,000": "THEFT LESS THAN $1000.00",
"Theft: Less $1,000 Value": "THEFT LESS THAN $1000.00",
"Theft of property or services with a value of less than $1000": "THEFT LESS THAN $1000.00",
"CDS Possess/Marijuana":"CDS: POSSESSION-MARIHUANA",
"Cds:Poss W/Intent Dist: Narc":"CDS-POSS W/I MANUF/DIS/DISP-NARC" ,
"CDS:POSSESS-NOT MARIJUANA":"CDS: POSSESS-NOT MARIJUANA",
"CDS Possess/Not Marijuana":"CDS: POSSESS-NOT MARIJUANA",
"CDS:Possess-Not Marijuana": "CDS: POSS-NOT MARIHUANA",
"Burglary-First Degree": "BURGLARY-FIRST DEGREE",
"Assault-First Degree": "ASSAULT-FIRST DEGREE",
"Assault 1st Degree": "ASSAULT-FIRST DEGREE",
"ASSAULT-FIRST DEGREE": "ASSAULT-FIRST DEGREE",
"Cds Possess With Intent To Distribute":"CDS-POSS W/I MANUF/DIS/DISP-NARC",
"CDS Possess With Intent To Distribute":"CDS-POSS W/I MANUF/DIS/DISP-NARC",
"CDS POSSESS WITH INTENT TO DISTRIBUTE":"CDS-POSS W/I MANUF/DIS/DISP-NARC",
"CDS:POSS W/INTENT DIST: NARC":"CDS-POSS W/I MANUF/DIS/DISP-NARC",
"Armed Robbery": "ARMED ROBBERY",
"ARMED ROBBERY": "ARMED ROBBERY",
"Robbery W/DW": "ARMED ROBBERY",
"Robbery": "ROBBERY",
"ROBBERY": "ROBBERY",
"DRIVING VEHICLE WHILE UNDER THE INFLUENCE OF ALCOHOL": "DWI",
"THEFT: $1,000 TO UNDER $10,000":"THEFT: $1,000 TO UNDER $10,000",
"Theft of property or services with a value more than $1000 less than $10,000":"THEFT: $1,000 TO UNDER $10,000",
"PERSON DRIVING MOTOR VEHICLE ON HIGHWAY OR PUBLIC USE PROPERTY ON SUSPENDED LICENSE AND PRIVILEGE":"PERSON DRIVING MOTOR VEHICLE ON HIGHWAY OR PUBLIC USE PROPERTY ON SUSPENDED LICENSE AND PRIVILEGE",
"Burglary/Second Degree/General":"Burglary/Second Degree/General",
"Driving Veh. On Hwy. At Speed Exceeding Limit":"SPEED TICKET",
"DRIVING VEH. ON HWY. AT SPEED EXCEEDING LIMIT":"SPEED TICKET",
"Speeding": 'SPEED TICKET',
"Cds Distribute-Narcotic":"CDS DIST-NARC",
"CDS DISTRIBUTE-NARCOTIC":"CDS DIST-NARC",
"Theft: $1,000 To Under $10,000":"THEFT: $1,000 TO UNDER $10,000",
"THEFT: $1,000 TO UNDER $10,000":"THEFT: $1,000 TO UNDER $10,000",
"Driving/Attempting Drive Motor Veh. On Hwy W/O Req. License And A":"Driving/Attempting Drive Motor Veh. On Hwy W/O Req. License And A",
"Manufact/Distribute/Poss/PWID":"CDS-POSS W/I MANUF/DIS/DISP-NARC",
"(Driving, Attempting To Drive) Vehicle While Underthe Influence O": "DWI",
"Drive: Under Influence Alcohol":"DWI",
"Driving Motor Veh. On Suspended License And Privilege": "Driving Motor Veh. On Suspended License And Privilege",
"(DRIVING, ATTEMPTING TO DRIVE) VEHICLE WHILE UNDER THE INFLUENCE OF ALCOHOL": "DWI",
"(DRIVING, ATTEMPTING TO DRIVE) VEHICLE WHILE UNDERTHE INFLUENCE OF ALC": "DWI",
"DRIVING, ATTEMPTING TO DRIVE) VEH. WHILE IMPAIRED BY ALCOHOL": "DWI",
"Driving While Impaired-Alcohol": "DWI",
'Driving While Suspended':'DRIVE SUSPENDED',
'Negligent Driving':'NEGLIGENT DRIVING',
"Reckless Driving":"RECKLESS DRIVING",
"Disorderly Conduct":"DISORDERLY CONDUCT",
"DISORDERLY CONDUCT":"DISORDERLY CONDUCT",
"Prostitution General":"PROSTITUTION GENERAL",
"VIOLATION OF PROBATION": "VIOLATION OF PROBATION",
"BURGLARY-FIRST DEGREE": "BURGLARY-FIRST DEGREE",
"BURGLARY/SECOND DEGREE/GENERAL":"BURGLARY-SECOND DEGREE"
}




def createCircuitMegaPandasGeneric(county, sql_file, output_file, begin_year, end_year, num_date_queries=1):

	CIRCUIT = circuit_db[county]
	dbconnection = mysql.connector.connect( host=hostname, user=username, passwd=password, db=CIRCUIT)
	
	begin = datetime.datetime(begin_year,1,1)
	end = datetime.datetime(end_year, 12, 31)
	print("Excuting making circuit mega Pandas for", output_file)

	output_path = "../NewData/"
	output_file = output_path + output_file

	sqlStr = open(sql_file).read()
	print("Opened SQL file", sql_file)
	#dbcursor = myCircuitConnection.cursor(dictionary=True)
	dbcursor = dbconnection.cursor(dictionary=True)
	
	#Just going to do this ugly
	if (num_date_queries==1):
		dbcursor.execute(sqlStr, (begin, end))
	elif (num_date_queries==2):
		dbcursor.execute(sqlStr, (begin, end, begin, end))
	elif (num_date_queries==3):
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end))
	elif (num_date_queries==4):
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end, begin, end))
	else:
		#assume 5 is limit!
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end, begin, end, begin, end))
	print("Executed SQL")
	results = dbcursor.fetchall()
	print("Fetched all")
	dbcursor.close()
	df = pd.DataFrame(results)
	print("Converted to dataframe")
	df.to_csv(output_file)

	dbconnection.close()

def createMegaPandasGeneric(sql_file, output_file, begin_year, end_year, num_date_queries=1):
	
	begin = datetime.datetime(begin_year,1,1)
	end = datetime.datetime(end_year, 12, 31)
	print("Excuting making mega Pandas for", output_file)

	output_path = "../NewData/"
	output_file = output_path + output_file

	sqlStr = open(sql_file).read()
	print("Opened SQL file", sql_file)
	dbcursor = myDistrictConnection.cursor(dictionary=True)
	#Just going to do this ugly
	if (num_date_queries==1):
		dbcursor.execute(sqlStr, (begin, end))
	elif (num_date_queries==2):
		dbcursor.execute(sqlStr, (begin, end, begin, end))
	elif (num_date_queries==3):
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end))
	elif (num_date_queries==4):
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end, begin, end))
	else:
		#assume 5 is limit!
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end, begin, end, begin, end))
	print("Executed SQL")
	results = dbcursor.fetchall()
	print("Fetched all")
	dbcursor.close()
	df = pd.DataFrame(results)
	print("Converted to dataframe")
	df.to_csv(output_file)

def genericMergeBaseline(target_file, append_file, new_output_file):

	input_path = '../NewData/'
	target_file = input_path + target_file
	append_file = input_path + append_file

	masterdf = pd.read_csv(target_file)
	print("Rows in", target_file, len(masterdf))
	appenddf = pd.read_csv(append_file)
	print("Rows in", append_file,  len(appenddf))

	masterdf = pd.merge(masterdf, appenddf, how='inner', on = 'casenumber')
	print("Rows in merged master is", len(masterdf))
	#masterdf.to_csv('../NewData/All_counties_baseline_2014_2018.csv')
	new_output_file = input_path+new_output_file
	masterdf.to_csv(new_output_file)

def createAndMergeDistrictBaseline(begin_year, end_year, baseline_trials=True, all_np=True, held_np=True):

	if (baseline_trials):
		print("Executing baseline from", begin_year, "to", end_year)
		
		print("Creating baseline...")
		base_file = "baseline_trials_"+str(begin_year)+"_"+str(end_year)+".csv"
		sqlFile = "baseline_district_trial_pd.sql"
		createMegaPandasGeneric(sqlFile, base_file, begin_year, end_year)
		
		print("Creating numcharges...")
		numcharges_file = base_file.replace(".csv", "_numcharges.csv")
		sqlFile = "baseline_district_trials_num_charges_pd.sql"
		createMegaPandasGeneric(sqlFile, numcharges_file, begin_year, end_year, 2)
		
		print("Creating final file...")
		final_file = "all_counties_baseline_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
		genericMergeBaseline(base_file, numcharges_file, final_file)

	if (all_np):
		print("Executing all_np from", begin_year, "to", end_year)
		
		print("Creating all_np...")
		base_file = "all_np_"+str(begin_year)+"_"+str(end_year)+".csv"
		sqlFile = "baseline_district_trial_all_npd_pd.sql"
		createMegaPandasGeneric(sqlFile, base_file, begin_year, end_year, 2)
		
		print("Creating all_np numcharges...")
		numcharges_file = base_file.replace(".csv", "_numcharges.csv")
		sqlFile = "baseline_district_trials_all_npd_num_charges_pd.sql"
		createMegaPandasGeneric(sqlFile, numcharges_file, begin_year, end_year, 3)
		
		print("Creating final file...")
		final_file = "all_counties_all_np_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
		genericMergeBaseline(base_file, numcharges_file, final_file)
		
	if (held_np):
		print("Executing held_np from", begin_year, "to", end_year)
		
		print("Creating held_np...")
		base_file = "held_np_"+str(begin_year)+"_"+str(end_year)+".csv"
		sqlFile = "baseline_all_npd_held_pd.sql"
		createMegaPandasGeneric(sqlFile, base_file, begin_year, end_year, 5)
		
		print("Creating held_np numcharges...")
		numcharges_file = base_file.replace(".csv", "_numcharges.csv")
		sqlFile = "baseline_all_npd_held_num_charges_pd.sql"
		createMegaPandasGeneric(sqlFile, numcharges_file, begin_year, end_year, 5)
		
		print("Creating final file...")
		final_file = "all_counties_held_np_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
		genericMergeBaseline(base_file, numcharges_file, final_file)


def createAndMergeCircuitBaseline(county, begin_year, end_year, baseline_trials=True, all_np=True):

	prefix = circuit_prefix[county]

	if (baseline_trials):
		print("Executing baseline from", begin_year, "to", end_year)
		
		print("Creating baseline...")
		base_file = prefix+"baseline_trials_"+str(begin_year)+"_"+str(end_year)+".csv"
		sqlFile = "baseline_"+prefix+"trial_pd.sql"
		createCircuitMegaPandasGeneric(county, sqlFile, base_file, begin_year, end_year)
		
		print("Creating numcharges...")
		numcharges_file = base_file.replace(".csv", "_numcharges.csv")
		sqlFile = "baseline_"+prefix+"num_charges_pd.sql"
		createCircuitMegaPandasGeneric(county, sqlFile, numcharges_file, begin_year, end_year)
		
		print("Creating final file...")
		final_file = prefix + "baseline_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
		genericMergeBaseline(base_file, numcharges_file, final_file)

	if (all_np):
		print("Executing all_np from", begin_year, "to", end_year)
		
		print("Creating all_np...")
		base_file = prefix+ "all_np_"+str(begin_year)+"_"+str(end_year)+".csv"
		#sqlFile = "baseline_balt_city_circuit_allnp_pd.sql"
		sqlFile = "baseline_"+prefix+"allnp_pd.sql"
		createCircuitMegaPandasGeneric(county, sqlFile, base_file, begin_year, end_year, 2)
		
		print("Creating all_np numcharges...")
		numcharges_file = base_file.replace(".csv", "_numcharges.csv")
		#sqlFile = "baseline_balt_city_circuit_allnp_num_charges_pd.sql"
		sqlFile = "baseline_"+prefix+"allnp_num_charges_pd.sql"
		createCircuitMegaPandasGeneric(county, sqlFile, numcharges_file, begin_year, end_year, 2)
		
		print("Creating final file...")
		final_file = prefix+"all_np_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
		genericMergeBaseline(base_file, numcharges_file, final_file)
		

def mergeCircuitCSVFiles(begin_year, end_year, all_np=False, balt_county=True, mont_county=True, pg_county=True):

	#Assume the "base" is Baltimore City
	print("All NP = ", all_np)
	prefix = circuit_prefix["Baltimore City"]
	input_path = '../NewData/'
	base_file = ""
	if not(all_np):
		#This is baseline
		base_file = input_path+ prefix + "baseline_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
	else:
		#This is all_np
		base_file = input_path+ prefix + "all_np_merged_"+str(begin_year)+"_"+str(end_year)+".csv"

	print("Looking for", base_file)
	basedf = pd.read_csv(base_file)
	print("Rows in base", base_file, len(basedf))

	#Normalize rows - casenumber to string
	basedf['casenumber'] = basedf['casenumber'].apply(str)
	basedf['zipcode'] = basedf['zipcode'].apply(str)


	for circuit in circuit_prefix:

		#It may not be pretty, but a way to only merge those that are true
		if circuit=="Baltimore City":
			continue #This is always base
		if ((circuit== "Baltimore County") and not(balt_county)):
				continue
		if ((circuit== "Montgomery County") and not(mont_county)):
				continue
		if ((circuit== "Prince George's County") and not(pg_county)):
				continue

	#if (balt_county):
		prefix = circuit_prefix[circuit]
		#prefix = circuit_prefix["Baltimore County"]
		append_file=""
		if not(all_np):
			append_file = input_path+prefix + "baseline_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
		else:
			append_file = input_path+prefix + "all_np_merged_"+str(begin_year)+"_"+str(end_year)+".csv"

		appenddf = pd.read_csv(append_file)
		print("Rows in", append_file, len(appenddf))

		#Coversions
		appenddf= appenddf.fillna("nan")
		appenddf['race']=appenddf['race'].apply(convertRace)
		appenddf['sex']=appenddf['sex'].apply(convertSex)
		appenddf['top_disposition']=appenddf['top_disposition'].apply(convertDispo)
		appenddf['top_charge']=appenddf['top_charge'].apply(convertCharge)
		appenddf['zipcode']=appenddf['zipcode'].apply(convertZip)

		basedf = pd.merge(basedf, appenddf, how='outer')
		print("Rows in merged base", len(basedf))
		#END OF FOR LOOP
	
	new_output_file=""
	if not(all_np):
		print("Merging circuit baseline...")
		new_output_file = "circuit_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
	else:
		print("Merging circuit all np...")
		new_output_file = "circuit_all_np_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
	
	new_output_file = input_path+new_output_file
	basedf.to_csv(new_output_file)

def isolateCategories(cat, juris="Baltimore County"):

	begin_year=2013
	end_year=2018
	top_num=25

	#Assume the "base" is Baltimore City
	prefix = circuit_prefix["Baltimore City"]
	input_path = '../NewData/'
	base_file = input_path+ prefix + "baseline_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
	basedf = pd.read_csv(base_file)

	print("unique category", cat, "in", base_file)
	if (len(basedf[cat].unique())<=25):
		print(basedf[cat].unique())
	else:
		groupdf = basedf.groupby(cat).size().reset_index(name="count").sort_values(['count'], ascending=False)
		for i in range(0,top_num):
			category = groupdf[cat].values[i]
			print(category)

	#prefix = circuit_prefix["Baltimore County"]
	prefix = circuit_prefix[juris]
	append_file = input_path+prefix + "baseline_merged_"+str(begin_year)+"_"+str(end_year)+".csv"
	appenddf = pd.read_csv(append_file)

	print("unique category", cat, "in", append_file)
	if (len(appenddf[cat].unique())<=25):
		print(appenddf[cat].unique())
	else:
		groupdf = appenddf.groupby(cat).size().reset_index(name="count").sort_values(['count'], ascending=False)
		for i in range(0,top_num):
			category = groupdf[cat].values[i]
			print(category)

def convertRace(racein):
	return race_normalize[racein]

def convertSex(sexin):
	return sex_normalize[sexin]

def convertDispo(dispoin):
	normal = ""
	try:
		normal= dispo_normalize[dispoin]
	except KeyError:
		if dispoin in dispo_normalize.values():
			normal = dispoin
		else:
			print("Key not found", dispoin)
			sys.exit(1)

	#return dispo_normalize[dispoin]
	return normal

def convertCharge(chargein):
	answer=""
	try:
		answer=charge_normalize[chargein]
	except KeyError:
		r = random.randint(1, 999)
		answer="OTHER_"+str(r)

	return answer

def convertZip(zipin):
	if zipin is None:
		return None
	if zipin=="nan":
		return "nan"
	if (zipin=='-'):
		return "nan"
	if ((zipin=='Unkno-wn') or (zipin=='unkno-wn') or (zipin=='UNKNO-WN') or (zipin=='DC')):
		return "nan"

	zipout = int(zipin[:5])
	return(zipout)



def main():
	
	createAndMergeDistrictBaseline(2013,2017, True, True, True)
	#createAndMergeCircuitBaseline(2013,2018, False, True)
	#createAndMergeCircuitBaseline("Baltimore County", 2013, 2018, False, True)
	#createAndMergeCircuitBaseline("Montgomery County", 2013, 2018, False, True)
	#createAndMergeCircuitBaseline("Prince George's County", 2013, 2018, False, True)
	#mergeCircuitCSVFiles(2013,2018, True, True, True, True)

	#isolateCategories("top_charge", "Montgomery County")


main()
myDistrictConnection.close()
