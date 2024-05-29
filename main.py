import pandas as pd
import streamlit as st
import mysql.connector as SQLC
import plotly.express as px
import requests 
import json
import plotly.graph_objects as go

# Connect to MySQL
mydb = SQLC.connect(
    host="localhost",
    user="root",
    password="",
    database="project02test"
)
Cursor = mydb.cursor()

Cursor.execute("SELECT * FROM project02trial.a_t_path;")
a_t_sql = Cursor.fetchall()

Cursor.execute("SELECT * FROM project02trial.a_u_path;")
a_u_sql = Cursor.fetchall()

Cursor.execute("SELECT * FROM project02test.a_i_path;")
a_i_sql = Cursor.fetchall()

Cursor.execute("SELECT * FROM project02test.m_t_path;")
m_t_sql = Cursor.fetchall()

Cursor.execute("SELECT * FROM project02test.m_u_path;")
m_u_sql = Cursor.fetchall()

Cursor.execute("SELECT * FROM project02test.m_i_path;")
m_i_sql = Cursor.fetchall()

Cursor.execute("SELECT * FROM project02.t_t_d_path;")
t_t_d_sql =Cursor.fetchall()

a_t_path_sql_df = pd.DataFrame(a_t_sql, columns=("a_t_state", "a_t_year", "a_t_qtr", "a_t_transaction", "a_t_category", "a_t_count", "a_t_amount"))
a_u_path_sql_df = pd.DataFrame(a_u_sql,columns=("a_u_state","a_u_year","a_u_qtr","a_u_registeredusers","a_u_appopens","a_u_userbrand","a_u_usercount","a_u_userpercentage"))
a_i_path_sql_df = pd.DataFrame(a_i_sql,columns=("a_i_state","a_i_year","a_i_qtr","a_i_transaction","a_i_category","a_i_count","a_i_amount"))

m_t_path_sql_df = pd.DataFrame(m_t_sql,columns=("m_t_state","m_t_year","m_t_qtr","m_t_district","m_t_transaction","m_t_count","m_t_amount")) 
m_u_path_sql_df = pd.DataFrame(m_u_sql,columns=("m_u_state","m_u_year","m_u_qtr","m_u_district","m_u_registeredusers","m_u_appopens"))
m_i_path_sql_df = pd.DataFrame(m_i_sql,columns=("m_i_state","m_i_year","m_i_qtr","m_i_district","m_i_transaction","m_i_count","m_i_amount"))

t_t_d_sql_df = pd.DataFrame(t_t_d_sql,columns=("t_t_d_state","t_t_d_year","t_t_d_qtr","t_t_d_district","t_t_d_transaction","t_t_d_count","t_t_d_amount"))

Cursor.close()
mydb.close()

years = a_t_path_sql_df["a_t_year"].unique()
quarters = a_t_path_sql_df["a_t_qtr"].unique()
states = m_t_path_sql_df["m_t_state"].unique()

def aggregated_transaction(year, quarter):
    a_t_yr = a_t_path_sql_df[a_t_path_sql_df["a_t_year"] == year]
    a_t_yr_qtr = a_t_yr[a_t_yr["a_t_qtr"] == quarter]
    a_t_yr_g = a_t_yr_qtr.groupby("a_t_state")[["a_t_count", "a_t_amount"]].sum().reset_index()

    a_t_yr_qtr_cat = a_t_yr_qtr.groupby("a_t_category")[["a_t_count", "a_t_amount"]].sum()
    #Total
    a_t_yr_qtr_cat_count_total = a_t_yr_qtr_cat["a_t_count"].sum()
    a_t_yr_qtr_cat_amount_total = a_t_yr_qtr_cat["a_t_amount"].sum()
    # Amount Individual
    a_t_yr_qtr_cat_str = a_t_yr_qtr_cat.to_dict()
    a_t_yr_qtr_cat_amt_financial  = int(a_t_yr_qtr_cat_str["a_t_amount"]["Financial Services"])
    a_t_yr_qtr_cat_amt_merchant = int(a_t_yr_qtr_cat_str["a_t_amount"]["Merchant payments"])
    a_t_yr_qtr_cat_amt_others = int(a_t_yr_qtr_cat_str["a_t_amount"]["Others"])
    a_t_yr_qtr_cat_amt_p2p = int(a_t_yr_qtr_cat_str["a_t_amount"]["Peer-to-peer payments"])
    a_t_yr_qtr_cat_amt_rc = int(a_t_yr_qtr_cat_str["a_t_amount"]["Recharge & bill payments"])

    return (a_t_yr_qtr_cat_count_total, a_t_yr_qtr_cat_amount_total,
            a_t_yr_qtr_cat_amt_financial, a_t_yr_qtr_cat_amt_merchant,
            a_t_yr_qtr_cat_amt_others, a_t_yr_qtr_cat_amt_p2p,
            a_t_yr_qtr_cat_amt_rc)

def aggregated_transaction_map(year, quarter):
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)

    a_t_yr = a_t_path_sql_df[a_t_path_sql_df["a_t_year"] == year]
    a_t_yr_qtr = a_t_yr[a_t_yr["a_t_qtr"] == quarter]
    a_t_yr_g = a_t_yr_qtr.groupby("a_t_state")[["a_t_count", "a_t_amount"]].sum().reset_index()
    
    data1 = json.loads(response.content)
    fig_india_1 = px.choropleth(a_t_yr_g, geojson=data1, locations="a_t_state", featureidkey="properties.ST_NM",
                                color="a_t_amount", color_continuous_scale="Rainbow",
                                range_color=(a_t_yr_g["a_t_amount"].min(), a_t_yr_g["a_t_amount"].max()),
                                hover_name="a_t_state", title=f"{year} Q{quarter} Transaction Amount",
                                fitbounds="locations", height=700, width=700)
    
    return fig_india_1

def aggregated_user(year, quarter):
    a_u_path_sql_df_yr = a_u_path_sql_df[a_u_path_sql_df["a_u_year"] == year]
    a_u_path_sql_df_qtr = a_u_path_sql_df_yr[a_u_path_sql_df_yr["a_u_qtr"] == quarter]
    a_u_path_sql_df_qtr.reset_index(drop=True, inplace=True)

    a_u_registeredusers_count = a_u_path_sql_df_qtr["a_u_registeredusers"].unique().sum()
    a_u_appopens_count = a_u_path_sql_df_qtr["a_u_appopens"].unique().sum()

    a_u_count_with_brand = a_u_path_sql_df_qtr.groupby("a_u_userbrand")[["a_u_usercount"]].sum()
    a_u_userbrand_index = a_u_count_with_brand.sort_values(by="a_u_usercount", ascending=False)

    a_u_path_sql_df_qtr_state = a_u_path_sql_df_qtr.groupby("a_u_state")[["a_u_registeredusers"]].sum()
    a_u_path_sql_df_qtr_state_topusers = a_u_path_sql_df_qtr_state.sort_values(by="a_u_registeredusers", ascending=False).head(10).reset_index()

    return (a_u_registeredusers_count, a_u_appopens_count,
            a_u_path_sql_df_qtr_state_topusers, a_u_userbrand_index)

def aggregated_user_map(year,quarter):
    a_u_path_sql_df_yr = a_u_path_sql_df[a_u_path_sql_df["a_u_year"]==year]
    a_u_path_sql_df_qtr = a_u_path_sql_df_yr[a_u_path_sql_df_yr["a_u_qtr"]==quarter]
    a_u_yr_g = a_u_path_sql_df_qtr.groupby("a_u_state")[["a_u_usercount"]].sum().reset_index()

    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)

    data1= json.loads(response.content)
    states_name =[]
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])
    states_name.sort()
    fig_india_2= px.choropleth(a_u_yr_g,geojson=data1,locations= "a_u_state",featureidkey= "properties.ST_NM",
                            color="a_u_usercount",color_continuous_scale="Rainbow",
                            range_color=(a_u_yr_g["a_u_usercount"].min(),a_u_yr_g["a_u_usercount"].max()),
                            hover_name="a_u_state",title=f"{year} Q{quarter} User Count", fitbounds="locations",height=700, width=700)
    return fig_india_2
def map_line_transaction_amount(states,years,quarter):
    m_t_path_sql_df_state = m_t_path_sql_df[m_t_path_sql_df["m_t_state"]==states]
    m_t_path_sql_df_year = m_t_path_sql_df_state[m_t_path_sql_df_state["m_t_year"]==years]
    m_t_path_sql_df_qtr = m_t_path_sql_df_year[m_t_path_sql_df_year["m_t_qtr"]==quarter]
    m_t_path_sql_df_qtr.reset_index(drop=True,inplace=True)
    
    m_t_path_sql_df_dist_amount = m_t_path_sql_df_qtr[["m_t_district","m_t_amount"]]

    fig_m_t_amount_line = px.line(m_t_path_sql_df_dist_amount,x="m_t_district",y="m_t_amount",title="MAP AMOUNT",color_discrete_sequence=px.colors.sequential.Bluered)

    return fig_m_t_amount_line

def map_line_transaction_count(states,years,quarter):
    m_t_path_sql_df_state = m_t_path_sql_df[m_t_path_sql_df["m_t_state"]==states]
    m_t_path_sql_df_year = m_t_path_sql_df_state[m_t_path_sql_df_state["m_t_year"]==years]
    m_t_path_sql_df_qtr = m_t_path_sql_df_year[m_t_path_sql_df_year["m_t_qtr"]==quarter]
    m_t_path_sql_df_qtr.reset_index(drop=True,inplace=True)
    m_t_path_sql_df_dist_count = m_t_path_sql_df_qtr[["m_t_district","m_t_count"]]
    
    fig_m_t_count_line = px.line(m_t_path_sql_df_dist_count,x="m_t_district",y="m_t_count",title="MAP COUNT",color_discrete_sequence=px.colors.sequential.Bluered)

    return fig_m_t_count_line

def map_bar_transaction_amount(states,years,quarter):
    m_t_path_sql_df_state = m_t_path_sql_df[m_t_path_sql_df["m_t_state"]==states]
    m_t_path_sql_df_year = m_t_path_sql_df_state[m_t_path_sql_df_state["m_t_year"]==years]
    m_t_path_sql_df_qtr = m_t_path_sql_df_year[m_t_path_sql_df_year["m_t_qtr"]==quarter]
    m_t_path_sql_df_qtr.reset_index(drop=True,inplace=True)
    
    m_t_path_sql_df_dist_amount = m_t_path_sql_df_qtr[["m_t_district","m_t_amount"]]


    fig_m_t_amount_bar = px.bar(m_t_path_sql_df_dist_amount,x="m_t_district",y="m_t_amount",title="MAP AMOUNT",color_discrete_sequence=px.colors.cyclical.HSV)
    
    return fig_m_t_amount_bar

def map_bar_transaction_count(states,years,quarter):
    m_t_path_sql_df_state = m_t_path_sql_df[m_t_path_sql_df["m_t_state"]==states]
    m_t_path_sql_df_year = m_t_path_sql_df_state[m_t_path_sql_df_state["m_t_year"]==years]
    m_t_path_sql_df_qtr = m_t_path_sql_df_year[m_t_path_sql_df_year["m_t_qtr"]==quarter]
    m_t_path_sql_df_qtr.reset_index(drop=True,inplace=True)
    m_t_path_sql_df_dist_count = m_t_path_sql_df_qtr[["m_t_district","m_t_count"]]
    
    fig_m_t_count_bar = px.bar(m_t_path_sql_df_dist_count,x="m_t_district",y="m_t_count",title="MAP COUNT",color_discrete_sequence=px.colors.cyclical.HSV) #
    
    return fig_m_t_count_bar
def map_line_user_reg(states,years,quarters):
    m_u_path_sql_df_state = m_u_path_sql_df[m_u_path_sql_df["m_u_state"]==states]
    m_u_path_sql_df_year = m_u_path_sql_df_state[m_u_path_sql_df_state["m_u_year"]==years]
    m_u_path_sql_df_qtr = m_u_path_sql_df_year[m_u_path_sql_df_year["m_u_qtr"]==quarters]
    m_u_path_sql_df_registereduser = m_u_path_sql_df_qtr[["m_u_district","m_u_registeredusers","m_u_appopens"]]
    #m_u_path_sql_df_qtr
    fig_m_u_registereduser_line = px.line(m_u_path_sql_df_registereduser,x="m_u_district",y="m_u_registeredusers",title="MAP REGISTERED USERS",color_discrete_sequence=px.colors.sequential.Bluered)
    return fig_m_u_registereduser_line
        

def map_line_user_appopens(states,years,quarters):
    m_u_path_sql_df_state = m_u_path_sql_df[m_u_path_sql_df["m_u_state"]==states]
    m_u_path_sql_df_year = m_u_path_sql_df_state[m_u_path_sql_df_state["m_u_year"]==years]
    m_u_path_sql_df_qtr = m_u_path_sql_df_year[m_u_path_sql_df_year["m_u_qtr"]==quarters]
    m_u_path_sql_df_registereduser = m_u_path_sql_df_qtr[["m_u_district","m_u_registeredusers","m_u_appopens"]]
    #m_u_path_sql_df_qtr
    fig_m_u_appopens_line = px.line(m_u_path_sql_df_registereduser,x="m_u_district",y="m_u_appopens",title="MAP APP OPENS",color_discrete_sequence=px.colors.sequential.Bluered)
    return fig_m_u_appopens_line

def map_bar_user_reg(states,years,quarters):
    m_u_path_sql_df_state = m_u_path_sql_df[m_u_path_sql_df["m_u_state"]==states]
    m_u_path_sql_df_year = m_u_path_sql_df_state[m_u_path_sql_df_state["m_u_year"]==years]
    m_u_path_sql_df_qtr = m_u_path_sql_df_year[m_u_path_sql_df_year["m_u_qtr"]==quarters]
    m_u_path_sql_df_registereduser = m_u_path_sql_df_qtr[["m_u_district","m_u_registeredusers","m_u_appopens"]]
    #m_u_path_sql_df_qtr
    fig_m_u_registereduser_bar = px.bar(m_u_path_sql_df_registereduser,x="m_u_district",y="m_u_registeredusers",title="MAP REGISTERED USERS",color_discrete_sequence=px.colors.cyclical.HSV)
    return fig_m_u_registereduser_bar

def map_bar_user_count(states,years,quarters):
    m_u_path_sql_df_state = m_u_path_sql_df[m_u_path_sql_df["m_u_state"]==states]
    m_u_path_sql_df_year = m_u_path_sql_df_state[m_u_path_sql_df_state["m_u_year"]==years]
    m_u_path_sql_df_qtr = m_u_path_sql_df_year[m_u_path_sql_df_year["m_u_qtr"]==quarters]
    m_u_path_sql_df_registereduser = m_u_path_sql_df_qtr[["m_u_district","m_u_registeredusers","m_u_appopens"]]
    #m_u_path_sql_df_qtr
    fig_m_u_appopens_bar = px.bar(m_u_path_sql_df_registereduser,x="m_u_district",y="m_u_appopens",title="MAP APP OPENS",color_discrete_sequence=px.colors.cyclical.HSV)
    return fig_m_u_appopens_bar

def map_line_insurance_amount(states,years,quarters):
    m_i_path_sql_df_states = m_i_path_sql_df[m_i_path_sql_df["m_i_state"]==states]
    m_i_path_sql_df_year =m_i_path_sql_df_states[m_i_path_sql_df_states["m_i_year"]==years]
    m_i_path_sql_df_qtr = m_i_path_sql_df_year[m_i_path_sql_df_year["m_i_qtr"]==quarters]
    m_i_path_sql_df_count = m_i_path_sql_df_qtr[["m_i_district","m_i_count"]]
    #m_i_path_sql_df_count
    m_i_path_sql_df_amount = m_i_path_sql_df_qtr[["m_i_district","m_i_amount"]]
    #m_i_path_sql_df_amount

    fig_m_i_amount_line = px.line(m_i_path_sql_df_amount,x="m_i_district",y="m_i_amount",title="MAP USER AMOUNT",color_discrete_sequence=px.colors.sequential.Bluered)
    return fig_m_i_amount_line

def map_line_insurance_count(states,years,quarters):
    m_i_path_sql_df_states = m_i_path_sql_df[m_i_path_sql_df["m_i_state"]==states]
    m_i_path_sql_df_year =m_i_path_sql_df_states[m_i_path_sql_df_states["m_i_year"]==years]
    m_i_path_sql_df_qtr = m_i_path_sql_df_year[m_i_path_sql_df_year["m_i_qtr"]==quarters]
    m_i_path_sql_df_count = m_i_path_sql_df_qtr[["m_i_district","m_i_count"]]
    #m_i_path_sql_df_count


    fig_m_i_count_line = px.line(m_i_path_sql_df_count,x="m_i_district",y="m_i_count",title="MAP USER COUNT",color_discrete_sequence=px.colors.sequential.Bluered)
    return fig_m_i_count_line  

    
def map_bar_insurance_amount(states,years,quarters):
    m_i_path_sql_df_states = m_i_path_sql_df[m_i_path_sql_df["m_i_state"]==states]
    m_i_path_sql_df_year =m_i_path_sql_df_states[m_i_path_sql_df_states["m_i_year"]==years]
    m_i_path_sql_df_qtr = m_i_path_sql_df_year[m_i_path_sql_df_year["m_i_qtr"]==quarters]
    m_i_path_sql_df_amount = m_i_path_sql_df_qtr[["m_i_district","m_i_amount"]]
    #m_i_path_sql_df_amount

    fig_m_i_amount_bar = px.bar(m_i_path_sql_df_amount,x="m_i_district",y="m_i_amount",title="MAP USER AMOUNT",color_discrete_sequence=px.colors.cyclical.HSV)
    return fig_m_i_amount_bar  

    
def map_bar_insurance_count(states,years,quarters):
    m_i_path_sql_df_states = m_i_path_sql_df[m_i_path_sql_df["m_i_state"]==states]
    m_i_path_sql_df_year =m_i_path_sql_df_states[m_i_path_sql_df_states["m_i_year"]==years]
    m_i_path_sql_df_qtr = m_i_path_sql_df_year[m_i_path_sql_df_year["m_i_qtr"]==quarters]
    m_i_path_sql_df_count = m_i_path_sql_df_qtr[["m_i_district","m_i_count"]]
    #m_i_path_sql_df_count
    
    fig_m_i_count_bar = px.bar(m_i_path_sql_df_count,x="m_i_district",y="m_i_count",title="MAP USER AMOUNT",color_discrete_sequence=px.colors.cyclical.HSV)
    return fig_m_i_count_bar

def map_transaction_amount_dataframe(states,years,quarter):
    m_t_path_sql_df_state = m_t_path_sql_df[m_t_path_sql_df["m_t_state"]==states]
    m_t_path_sql_df_year = m_t_path_sql_df_state[m_t_path_sql_df_state["m_t_year"]==years]
    m_t_path_sql_df_qtr = m_t_path_sql_df_year[m_t_path_sql_df_year["m_t_qtr"]==quarter]
    m_t_d_sql_df_district_desc = m_t_path_sql_df_qtr.sort_values(by="m_t_amount",ascending=True).head(10)
    m_t_d_sql_df_district_amount = m_t_d_sql_df_district_desc[['m_t_district', 'm_t_amount']]
    m_t_d_sql_df_district_amount.reset_index(inplace=True,drop=True)
    return m_t_d_sql_df_district_amount

def map_transaction_count_dataframe(states,years,quarter):
    m_t_path_sql_df_state = m_t_path_sql_df[m_t_path_sql_df["m_t_state"]==states]
    m_t_path_sql_df_year = m_t_path_sql_df_state[m_t_path_sql_df_state["m_t_year"]==years]
    m_t_path_sql_df_qtr = m_t_path_sql_df_year[m_t_path_sql_df_year["m_t_qtr"]==quarter]
    m_t_path_sql_df_count = m_t_path_sql_df_qtr.sort_values(by="m_t_count",ascending=True).head(10)
    m_t_path_sql_df_count_ten = m_t_path_sql_df_count[["m_t_district","m_t_count"]]
    m_t_path_sql_df_count_ten.reset_index(drop=True,inplace=True)
    return m_t_path_sql_df_count_ten

def map_user_registereduser_dataframe(states,years,quarter):
    m_u_path_sql_df_state = m_u_path_sql_df[m_u_path_sql_df["m_u_state"]==states]
    m_u_path_sql_df_year = m_u_path_sql_df_state[m_u_path_sql_df_state["m_u_year"]==years]
    m_u_path_sql_df_qtr = m_u_path_sql_df_year[m_u_path_sql_df_year["m_u_qtr"]==quarter]
    #m_u_path_sql_df_qtr
    m_u_path_sql_df_reg_user = m_u_path_sql_df_qtr.sort_values(by="m_u_registeredusers",ascending=True).head(10)
    m_u_path_sql_df_reg_user_ten = m_u_path_sql_df_reg_user[["m_u_district","m_u_registeredusers"]]
    m_u_path_sql_df_reg_user_ten.reset_index(inplace=True,drop=True)
    return m_u_path_sql_df_reg_user_ten

def map_user_appopens_dataframe(states,years,quarter):
    m_u_path_sql_df_state = m_u_path_sql_df[m_u_path_sql_df["m_u_state"]==states]
    m_u_path_sql_df_year = m_u_path_sql_df_state[m_u_path_sql_df_state["m_u_year"]==years]
    m_u_path_sql_df_qtr = m_u_path_sql_df_year[m_u_path_sql_df_year["m_u_qtr"]==quarter]
    #m_u_path_sql_df_qtr
    m_u_path_sql_df_reg_app_opens = m_u_path_sql_df_qtr.sort_values(by="m_u_appopens",ascending=True).head(10)
    m_u_path_sql_df_reg_app_opens_ten = m_u_path_sql_df_reg_app_opens[["m_u_district","m_u_appopens"]]
    m_u_path_sql_df_reg_app_opens_ten.reset_index(inplace=True,drop=True)
    return m_u_path_sql_df_reg_app_opens_ten

def map_insurance_amount_dataframe(states,years,quarter):
    m_i_path_sql_df_states = m_i_path_sql_df[m_i_path_sql_df["m_i_state"]==states]
    m_i_path_sql_df_year =m_i_path_sql_df_states[m_i_path_sql_df_states["m_i_year"]==years]
    m_i_path_sql_df_qtr = m_i_path_sql_df_year[m_i_path_sql_df_year["m_i_qtr"]==quarter]
    m_i_path_sql_df_amount = m_i_path_sql_df_qtr.sort_values(by="m_i_amount",ascending=True).head(10)
    m_i_path_sql_df_amount_ten = m_i_path_sql_df_amount[["m_i_district","m_i_amount"]]
    m_i_path_sql_df_amount_ten.reset_index(inplace=True,drop=True)
    return m_i_path_sql_df_amount_ten

def map_insurance_count_dataframe_ten(states,years,quarter):
    m_i_path_sql_df_states = m_i_path_sql_df[m_i_path_sql_df["m_i_state"]==states]
    m_i_path_sql_df_year =m_i_path_sql_df_states[m_i_path_sql_df_states["m_i_year"]==years]
    m_i_path_sql_df_qtr = m_i_path_sql_df_year[m_i_path_sql_df_year["m_i_qtr"]==quarter]
    m_i_path_sql_df_count = m_i_path_sql_df_qtr.sort_values(by="m_i_count",ascending=True).head(10)
    m_i_path_sql_df_count_ten = m_i_path_sql_df_count[["m_i_district","m_i_count"]]
    m_i_path_sql_df_count_ten.reset_index(inplace=True,drop=True)
    return m_i_path_sql_df_count_ten

def donut_top_states(years,quarters):
    # Filter data for the year 2018 and first quarter
    t_t_d_sql_df_year = t_t_d_sql_df[t_t_d_sql_df["t_t_d_year"] == years]
    t_t_d_sql_df_qtr = t_t_d_sql_df_year[t_t_d_sql_df_year["t_t_d_qtr"] == quarters]

    # Group by state and sum the counts
    t_t_d_sql_df_states = t_t_d_sql_df_qtr.groupby("t_t_d_state")[["t_t_d_count"]].sum()
    t_t_d_sql_df_states.reset_index(inplace=True)
    t_t_d_sql_df_states_desc = t_t_d_sql_df_states.sort_values(by="t_t_d_count", ascending=False).head(5)
    t_t_d_sql_df_states_desc.reset_index(inplace=True)

    # Extract labels and values
    labels = t_t_d_sql_df_states_desc["t_t_d_state"].tolist()
    values = t_t_d_sql_df_states_desc["t_t_d_count"].tolist()

    colors = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']

    # Create a donut-like pie chart
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.4,marker=dict(colors=colors))])
    return fig

def donut_top_districts(years,quarters):
    # Filter data for the year 2018 and first quarter
    t_t_d_sql_df_year = t_t_d_sql_df[t_t_d_sql_df["t_t_d_year"] == years]
    t_t_d_sql_df_qtr = t_t_d_sql_df_year[t_t_d_sql_df_year["t_t_d_qtr"] == quarters]

    # Group by district and sum the counts
    t_t_d_sql_df_district = t_t_d_sql_df_qtr.groupby("t_t_d_district")[["t_t_d_count"]].sum()
    t_t_d_sql_df_district.reset_index(inplace=True)

    t_t_d_sql_df_dist_desc = t_t_d_sql_df_district.sort_values(by="t_t_d_count", ascending=False).head(10)
    t_t_d_sql_df_dist_desc.reset_index(inplace=True)
    # Extract labels and values
    labels2 = t_t_d_sql_df_dist_desc["t_t_d_district"].tolist()
    values2 = t_t_d_sql_df_dist_desc["t_t_d_count"].tolist()

    colors2 = ['#FF6347', '#F1C40F', '#8A2BE2', '#2ECC71', '#D2691E', '#DC143C', '#00FFFF', '#FF1493', '#6495ED', '#CCCCFF']

    # Create a donut-like pie chart
    fig2 = go.Figure(data=[go.Pie(labels=labels2, values=values2, hole=.4,marker=dict(colors=colors2))])
    return fig2



#m_i_path_sql_df_count_ten = map_insurance_count_dataframe
st.set_page_config(layout="wide")
st.markdown('<h1 style="color: lime;">PhonePe Amounts Transactions 2018-2023 </h1>', unsafe_allow_html=True)

# Align selectboxes horizontally
col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.title("    ")
with col4:
    selected_year = st.selectbox("Select Year", years)
with col5:
    selected_quarter = st.selectbox("Select Quarter", quarters)
with col6:
    data_type = st.selectbox("Select Data Type", ["Transactions", "Users"])
with col2:
    st.write("   ")

column1, column2 = st.columns(2)

with column1:
    if data_type == "Transactions":
        fig_india_1 = aggregated_transaction_map(selected_year, selected_quarter)
        st.plotly_chart(fig_india_1)
    elif data_type == "Users":
        fig_india_2 = aggregated_user_map(selected_year, selected_quarter)
        st.plotly_chart(fig_india_2)


with column2:
    total_transaction_count, total_transaction_amount, \
    a_t_yr_qtr_cat_amt_financial, a_t_yr_qtr_cat_amt_merchant, \
    a_t_yr_qtr_cat_amt_others, a_t_yr_qtr_cat_amt_p2p, \
    a_t_yr_qtr_cat_amt_rc = aggregated_transaction(selected_year, selected_quarter)
    
    a_u_registeredusers_count, a_u_appopens_count, \
    a_u_path_sql_df_qtr_state_topusers, a_u_userbrand_index = aggregated_user(selected_year, selected_quarter)

    if data_type == "Transactions":
        st.header("Transactions")
        st.markdown('<h4 style="color: lime;">All Transactions (UPI+Cards+Wallets)</h4>', unsafe_allow_html=True)
        sub_col4, sub_col5 = st.columns(2)
        with sub_col4:
            st.markdown(f'<h3 style="color: lime;">Total Transaction Amount in {selected_year} Q{selected_quarter}</h3>', unsafe_allow_html=True)
            st.header(f"₹ {total_transaction_amount}")
        with sub_col5:
            st.markdown(f'<h3 style="color: lime;">Total Transaction Count in {selected_year} Q{selected_quarter}</h3>', unsafe_allow_html=True)
            st.header(f"{total_transaction_count}")
        st.divider()
        sub_col6, sub_col7 = st.columns(2)
        with sub_col6:
            st.markdown(f'<h3 style="color: lime;">Financial Services: </h3>', unsafe_allow_html=True)
            st.markdown(f'<h3 style="color: lime;">Merchant Payments: </h3>', unsafe_allow_html=True)
            st.markdown(f'<h3 style="color: lime;">Peer-to-peer Payments: </h3>', unsafe_allow_html=True)
            st.markdown(f'<h3 style="color: lime;">Recharge & Bills: </h3>', unsafe_allow_html=True)
            st.markdown(f'<h3 style="color: lime;">Others: </h3>', unsafe_allow_html=True)
        with sub_col7:
            st.subheader(f"₹ {a_t_yr_qtr_cat_amt_financial}")
            st.subheader(f"₹ {a_t_yr_qtr_cat_amt_merchant}")
            st.subheader(f"₹ {a_t_yr_qtr_cat_amt_p2p}")
            st.subheader(f"₹ {a_t_yr_qtr_cat_amt_rc}")
            st.subheader(f"₹ {a_t_yr_qtr_cat_amt_others}")

    elif data_type == "Users":
        st.header("Users")
        st.markdown('<h4 style="color: lime;">Registered Users and Count</h4>', unsafe_allow_html=True)
        st.markdown('<h6 style="color: red;">(Note: Data available between 2018-2022 Q1 only)</h6>', unsafe_allow_html=True)
        
        sub_col8, sub_col9 = st.columns(2)
        with sub_col8:
            st.markdown(f'<h3 style="color: lime;">Total Registered Users till {selected_year} Q{selected_quarter}</h3>', unsafe_allow_html=True)
            st.header(f"{a_u_registeredusers_count}")
        with sub_col9:
            st.markdown(f'<h3 style="color: lime;">Total App Opens in {selected_year} Q{selected_quarter}</h3>', unsafe_allow_html=True)
            st.header(f"{a_u_appopens_count}")
        st.divider()
        sub_col10, sub_col11 = st.columns(2)
        with sub_col10:
            st.markdown(f'<h5 style="color: lime;">Top PhonePe Users Brand</h5>', unsafe_allow_html=True)
            st.dataframe(a_u_userbrand_index)
        with sub_col11:
            st.markdown(f'<h5 style="color: lime;">Top Registered Users State</h5>', unsafe_allow_html=True)
            st.dataframe(a_u_path_sql_df_qtr_state_topusers)

st.divider()  #fig_m_t_count_bar,fig_m_t_amount_bar
st.markdown('<h2 style="color: lime;">Map based data comparison</h2>', unsafe_allow_html=True)
col7, col8, col9, col10, col11, col12 = st.columns(6)
col13,col14 = st.columns(2)
# Selections for state, year, and quarter
with col7:
    map_selected_state = st.selectbox("State", states)

with col8:
    map_selected_year = st.selectbox("Year", years)

with col9:
    map_selected_qtr = st.selectbox("Quarter", quarters)

# Selection for chart type
with col10:
    map_selected_chat = st.selectbox("Chart Type", ["Line Chart", "Bar Chart"])

    if map_selected_chat=="Line Chart":
        with col11:
            map_selected_category = st.selectbox(" ",["Transaction","User","Insurance"])
            if map_selected_category=="Transaction":
                with col12:
                    map_selected_condition=st.selectbox(" ",["Amount","Count"])
                    if map_selected_condition == "Amount":
                        with col13:
                            fig_m_t_amount_line = map_line_transaction_amount(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_t_amount_line)
                        with col14:
                            col15,col16 = st.columns(2)
                            with col16:
                                st.write("Bussiness Insights:")
                                st.write("Top 10 Districts needs Improvement")
                                m_t_d_sql_df_district_amount = map_transaction_amount_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                st.dataframe(m_t_d_sql_df_district_amount)
                    elif map_selected_condition == "Count":
                        with col13:
                            fig_m_t_count_line = map_line_transaction_count(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_t_count_line)
                        with col14:
                            col15,col16 = st.columns(2)
                            with col16:
                                st.write("Bussiness Insights:")
                                st.write("Top 10 Districts needs Improvement")
                                m_t_path_sql_df_count_ten = map_transaction_count_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                st.dataframe(m_t_path_sql_df_count_ten)
            elif map_selected_category=="User":
                with col12:
                    map_selected_condition=st.selectbox(" ",["Registered Users","App Opens"])
                    if map_selected_condition == "Registered Users":
                        with col13:
                            fig_m_u_registereduser_line = map_line_user_reg(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_u_registereduser_line)
                        with col14:
                                col15,col16 = st.columns(2)
                                with col16:
                                    st.write("Bussiness Insights:")
                                    st.write("Top 10 Districts needs Improvement")
                                    m_u_path_sql_df_reg_user_ten = map_user_registereduser_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                    st.dataframe(m_u_path_sql_df_reg_user_ten)

                    elif map_selected_condition == "App Opens":
                        with col13:
                            fig_m_u_appopens_line = map_line_user_appopens(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_u_appopens_line)
                        
                        with col14:
                            col15,col16 = st.columns(2)
                            with col16:
                                st.write("Bussiness Insights:")
                                st.write("Top 10 Districts needs Improvement")
                                m_u_path_sql_df_reg_app_opens_ten = map_user_appopens_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                st.dataframe(m_u_path_sql_df_reg_app_opens_ten)
            
            elif map_selected_category=="Insurance":
                with col12:
                    map_selected_condition=st.selectbox(" ",["Amount","Count"])
                    if map_selected_condition == "Amount":
                        with col13:
                            fig_m_i_amount_line = map_line_insurance_amount(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_i_amount_line)
                        with col14:
                            col15,col16 = st.columns(2)
                            with col16:
                                st.write("Bussiness Insights:")
                                st.write("Top 10 Districts needs Improvement")
                                m_i_path_sql_df_amount_ten = map_insurance_amount_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                st.dataframe(m_i_path_sql_df_amount_ten)
                            
                    elif map_selected_condition== "Count":
                        with col13:
                            fig_m_i_count_line = map_line_insurance_count(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_i_count_line)
                        
                        with col14:
                            col15,col16 = st.columns(2)
                            with col16:
                                st.write("Bussiness Insights:")
                                st.write("Top 10 Districts needs Improvement")
                                m_i_path_sql_df_count_ten = map_insurance_count_dataframe_ten(map_selected_state, map_selected_year, map_selected_qtr)
                                st.dataframe(m_i_path_sql_df_count_ten)
            
    elif map_selected_chat=="Bar Chart":
        with col11:
            map_selected_category = st.selectbox(" ",["Transaction","User","Insurance"])
            if map_selected_category=="Transaction":
                with col12:
                    map_selected_condition=st.selectbox(" ",["Amount","Count"])
                    if map_selected_condition == "Amount":
                        with col13:
                            fig_m_t_amount_bar = map_bar_transaction_amount(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_t_amount_bar)
                        with col14:
                                col15,col16 = st.columns(2)
                                with col16:
                                    st.write("Bussiness Insights:")
                                    st.write("Top 10 Districts needs Improvement")
                                    m_t_d_sql_df_district_amount = map_transaction_amount_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                    st.dataframe(m_t_d_sql_df_district_amount)

                    elif map_selected_condition == "Count":
                        with col13:
                            fig_m_t_count_bar = map_bar_transaction_count(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_t_count_bar)
                        with col14:
                                col15,col16 = st.columns(2)
                                with col16:
                                    st.write("Bussiness Insights:")
                                    st.write("Top 10 Districts needs Improvement")
                                    m_t_path_sql_df_count_ten = map_transaction_count_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                    st.dataframe(m_t_path_sql_df_count_ten)
            elif map_selected_category=="User":
                with col12:
                    map_selected_condition=st.selectbox(" ",["Registered Users","App Opens"])
                    if map_selected_condition == "Registered Users":
                        with col13:
                            fig_m_u_registereduser_bar = map_bar_user_reg(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_u_registereduser_bar)
                        
                        with col14:
                                col15,col16 = st.columns(2)
                                with col16:
                                    st.write("Bussiness Insights:")
                                    st.write("Top 10 Districts needs Improvement")
                                    m_u_path_sql_df_reg_user_ten = map_user_registereduser_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                    st.dataframe(m_u_path_sql_df_reg_user_ten)

                    elif map_selected_condition== "App Opens":
                        with col13:
                            fig_m_u_appopens_bar = map_bar_user_count(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_u_appopens_bar)
                        
                        with col14:
                                col15,col16 = st.columns(2)
                                with col16:
                                    st.write("Bussiness Insights:")
                                    st.write("Top 10 Districts needs Improvement")
                                    m_u_path_sql_df_reg_app_opens_ten = map_user_appopens_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                    st.dataframe(m_u_path_sql_df_reg_app_opens_ten)
 
            elif map_selected_category=="Insurance":
                with col12:
                    map_selected_condition=st.selectbox(" ",["Amount","Count"])
                    if map_selected_condition == "Amount":
                        with col13:
                            fig_m_i_amount_bar = map_bar_insurance_amount(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_i_amount_bar)
                        
                        with col14:
                                col15,col16 = st.columns(2)
                                with col16:
                                    st.write("Bussiness Insights:")
                                    st.write("Top 10 Districts needs Improvement")
                                    m_t_d_sql_df_district_amount = map_transaction_amount_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                    st.dataframe(m_t_d_sql_df_district_amount)
                            
                    elif map_selected_condition== "Count":
                        with col13:
                            fig_m_i_count_bar = map_bar_insurance_count(map_selected_state, map_selected_year, map_selected_qtr)
                            st.plotly_chart(fig_m_i_count_bar)
                        
                        with col14:
                                col15,col16 = st.columns(2)
                                with col16:
                                    st.write("Bussiness Insights:")
                                    st.write("Top 10 Districts needs Improvement")
                                    m_t_path_sql_df_count_ten = map_transaction_count_dataframe(map_selected_state, map_selected_year, map_selected_qtr)
                                    st.dataframe(m_t_path_sql_df_count_ten)

st.divider()
st.markdown('<h2 style="color: lime;">Top Transactions data comparison</h2>', unsafe_allow_html=True)
col17,col18=st.columns(2)


with col17:
    top_selected_year = st.selectbox(" ", years)  
    
with col18:
    top_selected_qtr = st.selectbox(" ", quarters)
    
col19,col20=st.columns(2)

with col19:
    st.write(f"Top 5 states in {top_selected_year} Q{top_selected_qtr}:")
    fig = donut_top_states(top_selected_year,top_selected_qtr)
    st.plotly_chart(fig)
with col20:
    st.write(f"Top 10 cities in {top_selected_year} Q{top_selected_qtr}:")
    fig2 = donut_top_districts(top_selected_year,top_selected_qtr)
    st.plotly_chart(fig2)
st.divider()