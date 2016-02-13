import pymysql.cursors
import pprint
import csv

connection = pymysql.connect(host="localhost",            
                     user="sun_h",                
                      passwd="82562533s",                 
                      db="sun_h_final", 
                      autocommit=True,
                      cursorclass=pymysql.cursors.DictCursor)

#connect to the server and read the first csv file
cursor = connection.cursor()
with open('SQ1_1948_2015.csv') as csvfile:
	myCSVReader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
	
	#for the state table, choose the state GeoFips as the state_id and the primary key and put the state_id and state_name to state table
	for row in myCSVReader:
		if  row["GeoFIPS"].isdigit() and int(row["GeoFIPS"]) >= 1000 and int(row["GeoFIPS"]) <=56000:
			state_sql = "INSERT INTO state( state_name, state_id) VALUE (%(dict_name)s, %(dict_stateid)s)"
			param_dict = { "dict_name":row["GeoName"], "dict_stateid":row["GeoFIPS"]}
			cursor.execute(state_sql, param_dict)

			#for the table us_state_personal_income, choose the year  from 1997 to 2013 increasing 1 per time
			#the statistics about the personal income is for the season and count the four quarters of the income for the whole year
			#input the state_id, year and the personal income of states to the table
			for year in range(1997, 2013, 1):
				income = 0
				for session in {0.1, 0.2, 0.3, 0.4}	:
					if  year + session > 1997 and year + session < 2013:
						income += int(row[str(year + session)])
				income_sql = "INSERT INTO us_state_personal_income(state_id, year, personal_income) VALUE (%(dict_stateid)s, %(dict_year)s, %(dict_income)s)"
				param_dict = {"dict_stateid":row["GeoFIPS"], "dict_year":year, "dict_income":income}
				cursor.execute(income_sql, param_dict)


#read the PCEState csv and skip the first 4 lines since they are the basic information of the csv and not useful for the database
dict_expense = list()	
with open('PCEbyState.csv') as csvfile:
	csvReader = csv.reader(csvfile, delimiter = ",", quotechar = "|")
	for i in range(4) :
		line = next(csvReader)
		
	#for the expenditure type table, there are 28 kinds of expenditure  and  I put the type in the expenditure table
	myCSVReader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
	for row in myCSVReader:
		if  row["GeoFips"].isdigit() and int(row["GeoFips"]) >= 1000 and int(row["GeoFips"]) <=56000:
			if row["Linetitle"] not in dict_expense:
				dict_expense.append(row["Linetitle"])
				income_sql = "INSERT INTO expenditureType(expenditureId, name) VALUE (%(dict_expenseId)s, %(dict_expense)s)"
				param_dict = {"dict_expenseId":row["Linenumber"], "dict_expense":row["Linetitle"]}
				cursor.execute(income_sql, param_dict)
			
			#for the expenditure table, the data is large since each record is differnt in state_id, expenditure type and year and the year is from 1997 to 2012  
			for year in range(1997, 2013, 1):
				state_sql2 = "INSERT INTO expenditure(state_id, year, enpenditure, enpenditureType_id) VALUE (%(dict_stateid)s, %(dict_year)s, 
				"%(dict_expenditure)s, %(dict_expenditure_type)s)"
				param_dict2 = {"dict_stateid":row["GeoFips"], "dict_year": year, "dict_expenditure": row[str(year)], "dict_expenditure_type": int(row["Linenumber"])}
				cursor.execute(state_sql2, param_dict2)
			
	