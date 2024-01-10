
import pandas as pd
import schedule
from datetime import date
import time
from sqlalchemy import create_engine


def get_wallet():
    current_time = date.today()
    engine = create_engine('oracle://')
    connection = engine.connect()    
    sql_query = pd.read_sql_query("SELECT sum((BUY_QTY_DUE - QTY_INVOICED)*ifsapp.Customer_Order_Line_API.Get_Sale_Price_Total(ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO)/BUY_QTY_DUE) \
    FROM IFSAPP.customer_order_join \
    WHERE contract = 'Z01' \
    AND ORDER_NO like 'S%' \
    AND customer_no like upper('E%') \
    AND objstate <> (SELECT ifsapp.CUSTOMER_ORDER_LINE_API.FINITE_STATE_ENCODE__('Anulowane') from dual) \
    AND objstate <> (SELECT ifsapp.CUSTOMER_ORDER_LINE_API.FINITE_STATE_ENCODE__('Zafakturowane/Zamknięte') from dual) \
    AND objstate <> (SELECT ifsapp.CUSTOMER_ORDER_LINE_API.FINITE_STATE_ENCODE__('Dostarczone') from dual) \
    AND (upper(ifsapp.INVENTORY_PART_API.Get_Part_Product_Code(CONTRACT, CATALOG_NO)) like upper( 'A%' ) \
    OR upper(ifsapp.INVENTORY_PART_API.Get_Part_Product_Code(CONTRACT, CATALOG_NO)) like upper( 'B%' ) \
    OR upper(ifsapp.INVENTORY_PART_API.Get_Part_Product_Code(CONTRACT, CATALOG_NO)) like upper( 'C%' ) \
    OR upper(ifsapp.INVENTORY_PART_API.Get_Part_Product_Code(CONTRACT, CATALOG_NO)) like upper( 'D%' ) \
    OR upper(ifsapp.INVENTORY_PART_API.Get_Part_Product_Code(CONTRACT, CATALOG_NO)) like upper( 'E%' ))", engine)
    df = pd.DataFrame(sql_query)
    connection.close()
    df = df.rename(columns={'SUM((BUY_QTY_DUE-QTY_INVOICED)*IFSAPP.CUSTOMER_ORDER_LINE_API.GET_SALE_PRICE_TOTAL(ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO)/BUY_QTY_DUE)':'net_value'})
    df['current_time'] = current_time
    df = df[['current_time', 'net_value']]
    df['current_time'] = pd.to_datetime(df['current_time'])
    return df

def job():
    df = get_wallet()
    print('wallet imported')
    df2 = pd.read_feather('wallet_value_by_day.feather')
    print('open file')
    df3 = df2.append(df, ignore_index=True)
    print('append historical data done')
    df3.to_feather('wallet_value_by_day.feather')
    print('file saved')
    print('job done')


schedule.every().day.at('07:50:00').do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

