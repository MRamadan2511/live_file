import json
import datetime
from datetime import timedelta
import pandas as pd
from metabase_api import Metabase_API
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from time import sleep
import requests

from google.colab import drive
drive.mount('/content/drive')

import sys
sys.path.insert(0,'/content/drive/MyDrive/Colab_Notebooks/')
import  date_run
print(date_run.ofd_start)
ofd_start  = date_run.ofd_start[0]
ofd_start_month  = date_run.ofd_start[1]
ofd_end = date_run.ofd_end[0]
ofd_start_month  = date_run.ofd_end[1]


def salse_orders(card_id, start_day,end_day,query_name, start_month, end_month,q_start, q_end):
    start_day = start_day
    end_day = end_day
################ START Date to pass in Query ############
    year = 2023
    # start_day = 8 #int(input('Enter Start Day:   '))
    # start_month = 7 #int(input('Enter Start Month  '))
    start_date = datetime(year, start_month, start_day).date()
    print("----------------------------")
    print('Start Date : ',start_date)
    # end_day = int(input('Enter End day:  '))
    # end_month = int(input('Enter End Month:  '))
    end_date = datetime(year, end_month, end_day).date()
    print('End Date : ',end_date)
    print("----------------------------\n")
    # print('End Date : ',end_date)

    delta = start_date - start_date  
    date_list=[]
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        # print(day)
        date_list.append(day.strftime("%Y-%m-%d"))
    # print(date_list)
    try:
        ######### Start Login to metabase and clone data OFD ORDERS #####################################
        mb = Metabase_API('https://bi.maxab.info', 'mahmoud.ramadan@maxab.io', '012franco')  # if password is not given, it will prompt for password
        param= [{"type":"date/single","value":'{0}'.format(start_date),
                "target":["variable",["template-tag",'{0}'.format(q_start)]],
                "id":"b8f3f44d-35b9-0763-70c8-e09e4bff5d1e"},
                {"type":"date/single","value":'{0}'.format(end_date),
                "target":["variable",["template-tag",'{0}'.format(q_end)]],
                "id":"51dbbe01-c5d8-c8c0-e258-7a5c85e4bcef"}]

        print(f"\n Exporting Data,- { query_name } - Please wait\n")
        results = mb.get_card_data(card_id=card_id, parameters=param ,data_format='json')
        print("Data Exported\n")
        print("Convert Data to Json\n")
        df = pd.read_json(json.dumps(results))
        return df
        
    except requests.exceptions.HTTPError as errh:
        return(errh)
    except requests.exceptions.ConnectionError as errc:
        return(errc)
    except requests.exceptions.Timeout as errt:
        return(errt)
    except requests.exceptions.RequestException as err:
        return(err)

def gsheet():
    #salse_orders(card_id, start_day,end_day)
    # OFD Query
    ########################################################################
                            #G Spread
                            #########
    scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',]

    creds = ServiceAccountCredentials.from_json_keyfile_name(r'/content/drive/MyDrive/Colab_Notebooks/secret_key.json', scopes=scopes)
    file = gspread.authorize(creds)

                            ############
                              #G Spread
    ########################################################################
    dry_wh = ['El-Marg', 'Basatin',  'El-Wahat', 'Mostorod','Barageel']

    wrokbook=file.open("OFD Live Tracking")
    print_sheet_update = wrokbook.worksheet('Live Tracking')
    print_sheet_update.update_cell(1, 11, 'Updating Sheet ...')
    
#######################################################
# def salse_orders(card_id, start_day,end_day,query_name, start_month, end_month):
    df  = salse_orders(12177, ofd_start , ofd_end,"OFD Orders",ofd_start_month ,9, "FROM","TO")
#######################################################
    sheet =wrokbook.worksheet('OFD')
    print("----------------------------")
    print("        OFD Query           ")
    print("       ----------------     ")

    print("Clear Data from OFD G-Sheet")
    sheet.batch_clear(["A:L",])

    # Reformate date and time col
    df['ACTION_TIME']=df['ACTION_TIME'].astype(str)
    
    # max_v = df['ACTION_TIME'].max()
    print_sheet_update2 = wrokbook.worksheet('OFD')
    # print_sheet_update2.update_cell(1, 11, max_v)
    # print (max_v)
    df = df.fillna('')
    df = df.query("WAREHOUSE==@dry_wh")
    # df["ACTION_DATE"] = df["ACTION_DATE"].str[:-1]
    df['ACTION_DATE'] = df['ACTION_DATE'].str.strip()
    df = df.sort_values('ACTION_DATE', ascending=True)
    #Set needed colm
    clistnew =["SALES_ORDER_ID", "WAREHOUSE","SO_STATUS", "NMV", "RUN_SHEET_ID","ACTION_DATE", "ACTION_TIME","OFD", "COURIER", "ACTION_HOUR"]
    # df['ACTION_TIME'] = df['ACTION_TIME'].dt.strftime("%H:%M").str.split(" ", expand=True)
    # df['ACTION_TIME'] = df['ACTION_TIME'].dt.strftime("%H:%M:%S").str.replace("'",'')
    df =  df[clistnew]
    
    #send data to google sheet
    print("\n Adding  OFD Data on Sheet, Please Wait...")
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    sleep(3)
    ##############################################################################
    # Sales Orders Query
############################################################### 
# def salse_orders(card_id, start_day,end_day,query_name, start_month, end_month):   
    df2 = salse_orders(12178,24,26,"Sales Orders", 9, 9, "FROM","TO")
################################################################
    print("----------------------------")
    print("----------------------------")
    print("    Sales orders Query     ")
    print("       ----------------     ")

    sheet2 =wrokbook.worksheet('SO')
    #clear All sheet
    print("Clear Data from SO Sheet")
    sheet2.batch_clear(["A:Z",])

    # Reformate date and time col
    df2['DATE']=df2['DATE'].astype(str)

    status = ['In Route',"In Progress"]
    df2= df2.fillna('')
    df2 = df2.query(" STATUS==@status & WAREHOUSE==@dry_wh")

    #Set needed colm
    clistnew2 =["SALES_ORDER_ID", "WAREHOUSE","STATUS", "RUN_SHEET_ID", "DATE", "DRIVER"]
    df2 =  df2[clistnew2]

    print("Adding Data on Sheet SO, Please Wait...\n")
    sheet2.update([df2.columns.values.tolist()] + df2.values.tolist())

    #Adding Time to SO sheet
    print(f"Done....")
    print(datetime.now())
    print("----------------------------")
    sheet2 =wrokbook.worksheet('SO')
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    sheet2.update_cell(1, 11, now_str)

# ##############################################################################
# ##############################################################################


    # Fleet Operation for start and end time

# def salse_orders(card_id, start_day,end_day,query_name, start_month, end_month):   
    df4 = salse_orders(16314, 24, 26,"Fleet OPS (Start & End)" ,9, 9,"START","END")
################################################################
    print("----------------------------")
    print("----------------------------")
    print("    Fleet OPS > Start End     ")
    print("       ----------------     ")

    sheet4 =wrokbook.worksheet('start_end')
    #clear All sheet
    print("Clear Data from runsheet Sheet")
    sheet4.batch_clear(["A:G",])

    # Reformate date and time col
    df4['DELIVERY_DATE']=df4['DELIVERY_DATE'].astype(str)
    df4['END_LOADING_AT']=df4['END_LOADING_AT'].astype(str)
    df4['TRIP_STARTED_AT']=df4['TRIP_STARTED_AT'].astype(str)
    df4['TRIP_ENDED_AT']=df4['TRIP_ENDED_AT'].astype(str)
    # status = ['In Route',"In Progress"]
    dry_wh = ['El-Marg', 'Basatin',  'El-Wahat', 'Mostorod','Barageel']
    df4= df4.fillna('')
    df4 = df4.query("WAREHOUSE==@dry_wh")

    #Set needed colm
    clistnew4 =["RUN_SHEET_ID", "WAREHOUSE","COURIER","DELIVERY_DATE", "END_LOADING_AT", "TRIP_STARTED_AT", "TRIP_ENDED_AT"]
    df4 =  df4[clistnew4]

    print("Adding Data on Sheet Runsheet, Please Wait...\n")
    sheet4.update([df4.columns.values.tolist()] + df4.values.tolist())

    #Adding Time to SO sheet
    print(f"Done....")
    print(datetime.now())
    print("----------------------------")
    # sheet =wrokbook.worksheet('SO')
    # now = datetime.now()
    # now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    # sheet.update_cell(1, 11, now_str)

# ##############################################################################



    # Sales Orders Query
    # def salse_orders(card_id, start_day,end_day,query_name, start_month, end_month):
    df3 = salse_orders(12178, 27, 27,"Next Day Value", 9, 9, "FROM","TO")
#####################################################################33
    print("-------------------------------")
    print("-------------------------------")
    print("Sales orders Query For Next Day")
    print("         --------------        ")

    sheet5 =wrokbook.worksheet('Next_Day_Data')
    sheet5.batch_clear(["A:H",])

    status = ['New',"Delayed","In Progress"]
    supply_chain = ['Dry', 'CSDs - Dry', 'CSDs']
    df3= df3.fillna('')
    df3 = df3.query(" STATUS==@status & WAREHOUSE==@dry_wh & SUPPLY_CHAINS==@supply_chain" )
    df3['CREATED_AT']=df3['CREATED_AT'].astype(str)
    max_v = df3['CREATED_AT'].max()
    sheet5.update_cell(1, 11, max_v)
    #Set needed colm
    clistnew3 = ["SALES_ORDER_ID", "WAREHOUSE", "ORDER_PRICE",  'WEIGHT', 'SUPPLY_CHAINS']
    df3 =  df3[clistnew3]

    print("Adding Data on Sheet Next Day data, Please Wait...\n")
    sheet5.update([df3.columns.values.tolist()] + df3.values.tolist())
    sleep(5)
    print(f"Done....")
    print_sheet_update.update_cell(1, 11, '')
    print(datetime.now())
    print("----------------------------")
#############################################################################################
#################################################################
################ Option for code runing #########################

func_map = {'3': gsheet}

def run(condition):
    print("Running 1st Time...")
    while datetime.now().minute % 1 != 0:  # Wait until we are synced up with the 'every 1 minutes' clock
        sleep(1)

    def task():
        # Call the function based on the provided key in func_map
        func_map['3']()

    task()

    while condition:
        print("Running Again...")
        sleep(60 * 30)  # Wait for 30 minutes
        task()
run(True)
