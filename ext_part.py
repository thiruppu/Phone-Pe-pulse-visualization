
import pandas as pd
import json
import os
import streamlit as st
import mysql.connector as SQLC

mydb = SQLC.connect(
    host = "localhost",
    user = "root",
    password = "",
    database ="project02test"
)
Cursor = mydb.cursor()
Cursor.execute("CREATE DATABASE IF NOT EXISTS project02test;")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.A_T_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),TRANSACTION VARCHAR(50),CATEGORY VARCHAR(50),COUNT INT(50),AMOUNT INT(60));")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.A_U_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),REGISTERED_USERS INT(50),APPOPENS INT(10),USERS_BRAND VARCHAR(50),USERS_COUNT INT(10),USERS_PERCENTAGE DOUBLE);")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.A_I_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),TRANSACTION VARCHAR(50),CATEGORY VARCHAR(50),COUNT INT(50),AMOUNT INT(60));")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.M_T_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),DISTRICT VARCHAR(50),TRANSACTION VARCHAR(50),COUNT INT(10),AMOUNT DOUBLE);")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.M_U_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),DISTRICT VARCHAR(50),REGISTERED_USERS INT(50),APP_OPENS INT(10))")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.M_I_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),DISTRICT VARCHAR(50),TRANSACTION VARCHAR(50),COUNT INT(10),AMOUNT DOUBLE);")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.T_T_D_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),DISTRICT VARCHAR(50),TRANSACTION VARCHAR(50),COUNT INT(10),AMOUNT DOUBLE);")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.T_T_P_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),PINCODE VARCHAR(50),TRANSACTION VARCHAR(50),COUNT INT(10),AMOUNT DOUBLE);")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.T_U_D_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),DISTRICT VARCHAR(50),REGISTERED_USERS VARCHAR(50));")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.T_U_P_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),PINCODE VARCHAR(50),REGISTERED_USERS VARCHAR(50));")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.T_I_D_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),DISTRICT VARCHAR(50),TRANSACTION VARCHAR(50),COUNT INT(50),AMOUNT INT(50));")
Cursor.execute("CREATE TABLE IF NOT EXISTS project02test.T_I_P_PATH (STATE VARCHAR(50),YEAR VARCHAR(50),QUATER INT(10),PINCODE VARCHAR(50),TRANSACTION VARCHAR(50),COUNT INT(50),AMOUNT INT(50));")


'''Aggregated Transaction'''
a_t_path= r"D:/Data Science/Project 2/pulse-master/data/aggregated/transaction/country/india/state/"

Agg_state_list=os.listdir(a_t_path)

a_t_clm={'a_t_State':[], 'a_t_Year':[],'a_t_Quater':[],'a_t_Transaction_type':[],'a_t_Transacion_category':[], 'a_t_Transacion_count':[], 'a_t_Transacion_amount':[]}

for state in Agg_state_list:
    state_path=a_t_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            for i in Aggr_file["data"]["transactionData"]:
                a_t_state= state
                #print(a_t_state)
                a_t_year = years
                #print(a_t_year)
                a_t_qtr = filelink.strip('.json')
                #print(a_t_qtr)
                a_t_category=i["name"]
                #print(a_t_category)
                a_t_type=i["paymentInstruments"][0]["type"]
                #print(a_t_type)
                a_t_count=i["paymentInstruments"][0]["count"]
                #print(a_t_count)
                a_t_amt =i["paymentInstruments"][0]["amount"]


                a_t_clm["a_t_State"].append(a_t_state)
                a_t_clm["a_t_Year"].append(a_t_year)
                a_t_clm["a_t_Quater"].append(a_t_qtr)
                a_t_clm["a_t_Transacion_category"].append(a_t_category)
                a_t_clm["a_t_Transaction_type"].append(a_t_type)
                a_t_clm["a_t_Transacion_count"].append(a_t_count)
                a_t_clm["a_t_Transacion_amount"].append(a_t_amt)

                a_t_sql = "INSERT INTO project02test.A_T_PATH (STATE,YEAR,QUATER,TRANSACTION,CATEGORY,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                a_t_val = (a_t_state,a_t_year,a_t_qtr,a_t_type,a_t_category,a_t_count,a_t_amt)
                Cursor.execute(a_t_sql,a_t_val)
                
a_t_df=pd.DataFrame(a_t_clm)
st.write(a_t_df)

'''Aggregated User'''
a_u_path=r"D:/Data Science/Project 2/pulse-master/data/aggregated/user/country/india/state/"
Agg_state_list=os.listdir(a_u_path)

a_u_clm={'a_u_State':[], 'a_u_Year':[],'a_u_Quater':[],'a_u_Registered_Users':[],'a_u_App_Opens':[],'a_u_User_Brand':[],'a_u_User_Count':[],'a_u_User_Percentage':[]}

for state in Agg_state_list:
    state_path=a_u_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            #print(Aggr_file)
            try:
              a_u_userbydevice = Aggr_file['data']['usersByDevice']
              for i in a_u_userbydevice:
              #data.get('data', {}).get('usersByDevice', [])
                  a_u_state=state
                  a_u_clm['a_u_State'].append(a_u_state)
                  a_u_year = years
                  a_u_clm['a_u_Year'].append(a_u_year)
                  a_u_qtr = filelink.strip('.json')
                  a_u_clm['a_u_Quater'].append(a_u_qtr)
                  a_u_registeredusers=Aggr_file['data']['aggregated']['registeredUsers']
                  a_u_clm['a_u_Registered_Users'].append(a_u_registeredusers)
                  a_u_appOpens=Aggr_file['data']["aggregated"]["appOpens"]
                  a_u_clm['a_u_App_Opens'].append(a_u_appOpens)
                  a_u_userbydevice = Aggr_file['data']['usersByDevice']
                  a_u_brand=i['brand']
                  a_u_clm['a_u_User_Brand'].append(a_u_brand)
                  a_u_count=i['count']
                  a_u_clm['a_u_User_Count'].append(a_u_count)
                  a_u_percentage=i['percentage']
                  a_u_clm['a_u_User_Percentage'].append(a_u_percentage)

                  a_u_sql = "INSERT INTO project02test.A_U_PATH (STATE,YEAR,QUATER,REGISTERED_USERS,APPOPENS,USERS_BRAND,USERS_COUNT,USERS_PERCENTAGE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                  a_u_val = (a_u_state,a_u_year,a_u_qtr,a_u_registeredusers,a_u_appOpens,a_u_brand,a_u_count,a_u_percentage)
                  Cursor.execute(a_u_sql,a_u_val)
            except:
              pass


a_u_clm_df=pd.DataFrame(a_u_clm)
st.write(a_u_clm_df)

'''Aggregated Insurance'''
a_i_clm = {'a_i_State': [], 'a_i_Year': [], 'a_i_Quater': [], 'a_i_Payment_type': [], 'a_i_Payment_category': [], 'a_i_Payment_count': [], 'a_i_Payment_amount': []}
a_i_path = r"D:/Data Science/Project 2/pulse-master/data/aggregated/insurance/country/india/state/"
Agg_state_list = os.listdir(a_i_path)
for state in Agg_state_list:
    state_path=a_i_path+state+"/"
    #print(state_path)
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            for k in Aggr_file["data"]["transactionData"]:
                a_i_state= state
                #print(a_i_state)
                a_i_year = years
                #print(a_i_year)
                a_i_qtr = filelink.strip('.json')
                #print(a_i_qtr)
                a_i_category=k['name']
                a_i_paymenttype=k['paymentInstruments'][0]['type']
                a_i_paymentcount=k['paymentInstruments'][0]['count']
                a_i_paymentamount=k['paymentInstruments'][0]['amount']
                a_i_clm['a_i_State'].append(a_i_state)
                a_i_clm['a_i_Year'].append(a_i_year)
                a_i_clm['a_i_Quater'].append(a_i_qtr)
                a_i_clm['a_i_Payment_category'].append(a_i_category)
                a_i_clm['a_i_Payment_type'].append(a_i_paymenttype)
                a_i_clm['a_i_Payment_count'].append(a_i_paymentcount)
                a_i_clm['a_i_Payment_amount'].append(a_i_paymentamount)

                a_i_sql = "INSERT INTO project02test.A_I_PATH (STATE,YEAR,QUATER,TRANSACTION,CATEGORY,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                a_i_val = (a_i_state,a_i_year,a_i_qtr,a_i_paymenttype,a_i_category,a_i_paymentcount,a_i_paymentamount)
                Cursor.execute(a_i_sql,a_i_val)
a_i_df=pd.DataFrame(a_i_clm)
st.write(a_i_df)

'''Map Transaction'''
m_t_path=r"D:/Data Science/Project 2/pulse-master/data/map/transaction/hover/country/india/state/"
Agg_state_list=os.listdir(m_t_path)

m_t_clm={'m_t_State':[],'m_t_District':[], 'm_t_Year':[],'m_t_Quater':[],'m_t_Transaction_type':[], 'm_t_Transaction_count':[], 'm_t_Transaction_amount':[]}

for state in Agg_state_list:
    state_path=m_t_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            for l in Aggr_file['data']['hoverDataList']:
                m_t_district=l['name']
                m_t_clm['m_t_District'].append(m_t_district)
                m_t_state=state
                m_t_clm['m_t_State'].append(m_t_state)
                m_t_years=years
                m_t_clm['m_t_Year'].append(m_t_years)
                m_t_qtr=filelink.strip('.json')
                m_t_clm['m_t_Quater'].append(m_t_qtr)
                m_t_transactiontype=l['metric'][0]['type']
                m_t_clm['m_t_Transaction_type'].append(m_t_transactiontype)
                m_t_transactioncount=l['metric'][0]['count']
                m_t_clm['m_t_Transaction_count'].append(m_t_transactioncount)
                m_t_transactionamount=l['metric'][0]['amount']
                m_t_clm['m_t_Transaction_amount'].append(m_t_transactionamount)

                m_t_sql = "INSERT INTO project02test.M_T_PATH (STATE,YEAR,QUATER,DISTRICT,TRANSACTION,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                m_t_val = (m_t_state,m_t_years,m_t_qtr,m_t_district,m_t_transactiontype,m_t_transactioncount,m_t_transactionamount)
                Cursor.execute(m_t_sql,m_t_val)
m_t_clm_df=pd.DataFrame(m_t_clm)
st.write(m_t_clm_df)

'''Map User '''
m_u_path=r"D:/Data Science/Project 2/pulse-master/data/map/user/hover/country/india/state/"
Agg_state_list=os.listdir(m_u_path)

m_u_clm={'m_u_State':[],'m_u_District':[], 'm_u_Year':[],'m_u_Quater':[],'m_u_Registered_Users':[], 'm_u_App_Opens':[]}

for state in Agg_state_list:
    state_path=m_u_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            try:
              Aggr_file=json.load(open_file)
              b=Aggr_file['data']['hoverData']
              for a in b.items():
                m_u_state=state
                m_u_clm['m_u_State'].append(m_u_state)
                m_u_year=years
                m_u_clm['m_u_Year'].append(m_u_year)
                m_u_qtr=filelink.strip('.json')
                m_u_clm['m_u_Quater'].append(m_u_qtr)
                m_u_district=a[0]
                m_u_clm['m_u_District'].append(m_u_district)
                m_u_registeredusers=a[1]['registeredUsers']
                m_u_clm['m_u_Registered_Users'].append(m_u_registeredusers)
                m_u_appopens=a[1]['appOpens']
                m_u_clm['m_u_App_Opens'].append(m_u_appopens)

                m_u_sql = "INSERT INTO project02test.M_U_PATH (STATE,YEAR,QUATER,DISTRICT,REGISTERED_USERS,APP_OPENS) VALUES (%s, %s, %s, %s, %s, %s)"
                m_u_val = (m_u_state,m_u_year,m_u_qtr,m_u_district,m_u_registeredusers,m_u_appopens)
                Cursor.execute(m_u_sql,m_u_val)
            except:
              pass
#print(m_u_clm)
m_u_clm_df=pd.DataFrame(m_u_clm)
st.write(m_u_clm_df)

'''Map Insurance'''
m_i_path=r"D:/Data Science/Project 2/pulse-master/data/map/insurance/hover/country/india/state/"
Agg_state_list=os.listdir(m_i_path)

m_i_clm={'m_i_State':[],'m_i_District':[], 'm_i_Year':[],'m_i_Quater':[],'m_i_Transaction_type':[], 'm_i_Transaction_count':[], 'm_i_Transaction_amount':[]}

for state in Agg_state_list:
    state_path=m_i_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            for m in Aggr_file['data']['hoverDataList']:
                m_i_state = state
                m_i_clm['m_i_State'].append(m_i_state)
                m_i_district=m['name']
                m_i_clm['m_i_District'].append(m_i_district)
                m_i_year=years
                m_i_clm['m_i_Year'].append(m_i_year)
                m_i_qtr=filelink.strip('.json')
                m_i_clm['m_i_Quater'].append(m_i_qtr)
                m_i_type=m['metric'][0]['type']
                m_i_clm['m_i_Transaction_type'].append(m_i_type)
                m_i_count=m['metric'][0]['count']
                m_i_clm['m_i_Transaction_count'].append(m_i_count)
                m_i_amount=m['metric'][0]['amount']
                m_i_clm['m_i_Transaction_amount'].append(m_i_amount)

                m_i_sql = "INSERT INTO project02test.M_I_PATH (STATE,YEAR,QUATER,DISTRICT,TRANSACTION,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                m_i_val = (m_i_state,m_i_year,m_i_qtr,m_i_district,m_i_type,m_i_count,m_i_amount)
                Cursor.execute(m_i_sql,m_i_val)
m_i_clm_pd=pd.DataFrame(m_i_clm)
st.write(m_i_clm_pd)


'''Top Transaction District'''

##District
t_t_dist_path=r"D:/Data Science/Project 2/pulse-master/data/top/transaction/country/india/state/"
Agg_state_list=os.listdir(t_t_dist_path)


t_t_dist_clm={'t_t_dist_State':[], 't_t_dist_Year':[],'t_t_dist_Quater':[],'t_t_dist_District':[],'t_t_dist_Transaction_type':[], 't_t_dist_Transaction_count':[], 't_t_dist_Transaction_amount':[]}

for state in Agg_state_list:
    state_path=t_t_dist_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)-
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            for n in Aggr_file['data']['districts']:
                t_t_dist_states=state
                t_t_dist_clm['t_t_dist_State'].append(t_t_dist_states)
                t_t_dist_year=years
                t_t_dist_clm['t_t_dist_Year'].append(t_t_dist_year)
                t_t_dist_qtr=filelink.strip('.json')
                t_t_dist_clm['t_t_dist_Quater'].append(t_t_dist_qtr)
                t_t_dist_dist=n['entityName']
                t_t_dist_clm['t_t_dist_District'].append(t_t_dist_dist)
                t_t_dist_type=n['metric']['type']
                t_t_dist_clm['t_t_dist_Transaction_type'].append(t_t_dist_type)
                t_t_dist_count=str(n['metric']['count'])
                t_t_dist_clm['t_t_dist_Transaction_count'].append(t_t_dist_count)
                t_t_dist_amount=str(n['metric']['amount'])
                t_t_dist_clm['t_t_dist_Transaction_amount'].append(t_t_dist_amount)

                t_t_d_sql = "INSERT INTO project02test.T_T_D_PATH (STATE,YEAR,QUATER,DISTRICT,TRANSACTION,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                t_t_d_val = (t_t_dist_states,t_t_dist_year,t_t_dist_qtr,t_t_dist_dist,t_t_dist_type,t_t_dist_count,t_t_dist_amount)
                Cursor.execute(t_t_d_sql,t_t_d_val)
t_t_dist_clm_df=pd.DataFrame(t_t_dist_clm)
st.write(t_t_dist_clm_df)

'''Top Transaction Pincode'''
##Pincode
t_t_pin_path=r"D:/Data Science/Project 2/pulse-master/data/top/transaction/country/india/state/"
Agg_state_list=os.listdir(t_t_pin_path)


t_t_pin_clm={'t_t_pin_State':[], 't_t_pin_Year':[],'t_t_pin_Quater':[],'t_t_pin_Pincode':[],'t_t_pin_Transaction_type':[], 't_t_pin_Transaction_count':[], 't_t_pin_Transaction_amount':[]}

for state in Agg_state_list:
    state_path=t_t_pin_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)-
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            #print(Aggr_file['data'])
            for o in Aggr_file['data']['pincodes']:
                t_t_pin_state=state
                t_t_pin_clm['t_t_pin_State'].append(t_t_pin_state)
                t_t_pin_year=years
                t_t_pin_clm['t_t_pin_Year'].append(t_t_pin_year)
                t_t_pin_qtr=filelink.strip('.json')
                t_t_pin_clm['t_t_pin_Quater'].append(t_t_pin_qtr)
                t_t_pin_pincode=str(o['entityName'])
                t_t_pin_clm['t_t_pin_Pincode'].append(t_t_pin_pincode)
                t_t_pin_type=o['metric']['type']
                t_t_pin_clm['t_t_pin_Transaction_type'].append(t_t_pin_type)
                t_t_pin_count=str(o['metric']['count'])
                t_t_pin_clm['t_t_pin_Transaction_count'].append(t_t_pin_count)
                t_t_pin_amt=str(o['metric']['amount'])
                t_t_pin_clm['t_t_pin_Transaction_amount'].append(t_t_pin_amt)
                t_t_p_sql = "INSERT INTO project02test.T_T_P_PATH (STATE,YEAR,QUATER,PINCODE,TRANSACTION,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                t_t_p_val = (t_t_pin_state,t_t_pin_year,t_t_pin_qtr,t_t_pin_pincode,t_t_pin_type,t_t_pin_count,t_t_pin_amt)
                Cursor.execute(t_t_p_sql,t_t_p_val)
t_t_pin_clm_df=pd.DataFrame(t_t_pin_clm)
st.write(t_t_pin_clm_df)

'''Top User District'''
t_t_dist_path=r"D:/Data Science/Project 2/pulse-master/data/top/user/country/india/state/"
Agg_state_list=os.listdir(t_t_dist_path)


t_u_dist_clm={'t_u_dist_State':[], 't_u_dist_Year':[],'t_u_dist_Quater':[],'t_u_dist_District':[],'t_u_dist_RegisteredUsers':[]}

for state in Agg_state_list:
    state_path=t_t_dist_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            T=Aggr_file['data']['districts']
            for b in T:
                t_u_dist_state=state
                t_u_dist_clm['t_u_dist_State'].append(t_u_dist_state)
                t_u_dist_year=years
                t_u_dist_clm['t_u_dist_Year'].append(t_u_dist_year)
                t_u_dist_qtr=filelink.strip('.json')
                t_u_dist_clm['t_u_dist_Quater'].append(t_u_dist_qtr)
                t_u_dict_district=b['name']
                t_u_dist_clm['t_u_dist_District'].append(t_u_dict_district)
                t_u_dist_registeredusers=b['registeredUsers']
                t_u_dist_clm['t_u_dist_RegisteredUsers'].append(t_u_dist_registeredusers)
                t_u_d_sql = "INSERT INTO project02test.T_U_D_PATH (STATE,YEAR,QUATER,DISTRICT,REGISTERED_USERS) VALUES (%s, %s, %s, %s, %s)"
                t_u_d_val = (t_u_dist_state,t_u_dist_year,t_u_dist_qtr,t_u_dict_district,t_u_dist_registeredusers)
                Cursor.execute(t_u_d_sql,t_u_d_val)
t_u_dist_clm_df=pd.DataFrame(t_u_dist_clm)
st.write(t_u_dist_clm_df)

'''Top User Pincode'''
t_t_pin_path=r"D:/Data Science/Project 2/pulse-master/data/top/user/country/india/state/"
Agg_state_list=os.listdir(t_t_pin_path)


t_u_pin_clm={'t_u_pin_State':[], 't_u_pin_Year':[],'t_u_pin_Quater':[],'t_u_pin_Pincode':[],'t_u_pin_RegisteredUsers':[]}

for state in Agg_state_list:
    state_path=t_t_pin_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            T=Aggr_file['data']['pincodes']
            for b in T:
                t_u_pin_state=state
                t_u_pin_clm['t_u_pin_State'].append(state)
                t_u_pin_years=years
                t_u_pin_clm['t_u_pin_Year'].append(t_u_pin_years)
                t_u_pin_qtr=filelink.strip('.json')
                t_u_pin_clm['t_u_pin_Quater'].append(t_u_pin_qtr)
                t_u_pin_pincode=b['name']
                t_u_pin_clm['t_u_pin_Pincode'].append(t_u_pin_pincode)
                t_u_pin_registereduser=b['registeredUsers']
                t_u_pin_clm['t_u_pin_RegisteredUsers'].append(t_u_pin_registereduser)
                t_u_pin_sql = "INSERT INTO project02test.T_U_P_PATH (STATE,YEAR,QUATER,PINCODE,REGISTERED_USERS) VALUES (%s, %s, %s, %s, %s)"
                t_u_pin_val = (t_u_pin_state,t_u_pin_years,t_u_pin_qtr,t_u_pin_pincode,t_u_pin_registereduser)
                Cursor.execute(t_u_pin_sql,t_u_pin_val)
t_u_pin_clm_df=pd.DataFrame(t_u_pin_clm)
st.write(t_u_pin_clm_df)


'''Top Insurance District'''
t_i_dist_path=r"D:/Data Science/Project 2/pulse-master/data/top/insurance/country/india/state/"
Agg_state_list=os.listdir(t_i_dist_path)


t_i_dist_clm={'t_i_dist_State':[], 't_i_dist_Year':[],'t_i_dist_Quater':[],'t_i_dist_District':[],'t_i_dist_Transaction_type':[], 't_i_dist_Transaction_count':[], 't_i_dist_Transaction_amount':[]}

for state in Agg_state_list:
    state_path=t_i_dist_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)-
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            for p in Aggr_file['data']['districts']:
                t_i_dist_state=state
                t_i_dist_clm['t_i_dist_State'].append(t_i_dist_state)
                t_i_dist_year=years
                t_i_dist_clm['t_i_dist_Year'].append(t_i_dist_year)
                t_i_dist_qtr=filelink.strip('.json')
                t_i_dist_clm['t_i_dist_Quater'].append(t_i_dist_qtr)
                t_i_dist_district=p['entityName']
                t_i_dist_clm['t_i_dist_District'].append(t_i_dist_district)
                t_i_dist_type=p['metric']['type']
                t_i_dist_clm['t_i_dist_Transaction_type'].append(t_i_dist_type)
                t_i_dist_count=str(p['metric']['count'])
                t_i_dist_clm['t_i_dist_Transaction_count'].append(t_i_dist_count)
                t_i_dist_amount=str(p['metric']['amount'])
                t_i_dist_clm['t_i_dist_Transaction_amount'].append(t_i_dist_amount)
                t_i_dist_sql = "INSERT INTO project02test.T_I_D_PATH (STATE,YEAR,QUATER,DISTRICT,TRANSACTION,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                t_i_dist_val = (t_i_dist_state,t_i_dist_year,t_i_dist_qtr,t_i_dist_district,t_i_dist_type,t_i_dist_count,t_i_dist_amount)
                Cursor.execute(t_i_dist_sql,t_i_dist_val)
t_i_dist_clm_df=pd.DataFrame(t_i_dist_clm)
st.write(t_i_dist_clm_df)

'''Top Insurance Pincode'''

t_i_pin_path=r"D:/Data Science/Project 2/pulse-master/data/top/insurance/country/india/state/"
Agg_state_list=os.listdir(t_i_pin_path)


t_i_pin_clm={'t_i_pin_State':[], 't_i_pin_Year':[],'t_i_pin_Quater':[],'t_i_pin_Pincode':[],'t_i_pin_Transaction_type':[], 't_i_pin_Transaction_count':[], 't_i_pin_Transaction_amount':[]}

for state in Agg_state_list:
    state_path=t_i_pin_path+state+"/"
    year_list=os.listdir(state_path)
    #print(year_list)
    for years in year_list:
        quaters_path=state_path+years+"/"
        quaters_list=os.listdir(quaters_path)
        #print(quaters_list)
        for filelink in quaters_list:
            finallink=quaters_path+filelink
            #print(finallink)-
            open_file=open(finallink,'r')
            Aggr_file=json.load(open_file)
            for q in Aggr_file['data']['pincodes']:
                t_i_pin_state=state
                t_i_pin_clm['t_i_pin_State'].append(t_i_pin_state)
                t_i_pin_year=years
                t_i_pin_clm['t_i_pin_Year'].append(years)
                t_i_pin_qtr=filelink.strip('.json')
                t_i_pin_clm['t_i_pin_Quater'].append(t_i_pin_qtr)
                t_i_pin_pincode=str(q['entityName'])
                t_i_pin_clm['t_i_pin_Pincode'].append(t_i_pin_pincode)
                t_i_pin_type=q['metric']['type']
                t_i_pin_clm['t_i_pin_Transaction_type'].append(t_i_pin_type)
                t_i_pin_count=str(q['metric']['count'])
                t_i_pin_clm['t_i_pin_Transaction_count'].append(t_i_pin_count)
                t_i_pin_amount=str(q['metric']['amount'])
                t_i_pin_clm['t_i_pin_Transaction_amount'].append(t_i_pin_amount)
                t_i_pin_sql = "INSERT INTO project02test.T_I_P_PATH (STATE,YEAR,QUATER,PINCODE,TRANSACTION,COUNT,AMOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                t_i_pin_val = (t_i_pin_state,t_i_pin_year,t_i_pin_qtr,t_i_pin_pincode,t_i_pin_type,t_i_pin_count,t_i_pin_amount)
                Cursor.execute(t_i_pin_sql,t_i_pin_val)
t_i_pin_clm_df=pd.DataFrame(t_i_pin_clm)
st.write(t_i_pin_clm_df)
mydb.commit()