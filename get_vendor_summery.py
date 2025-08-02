import sqlite3
import pandas as pd
from ingestion_db import ingest_db
import logging
import time

logging.basicConfig(
    filename="logs/get_vendor_summery.log",
    level=logging.DEBUG,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
) 

def create_vendor_summery(conn):
    ''' This function will merge the different tables to get the overall vendor summery and adding new columns in the resultant data'''
    Vendor_sales_summery = pd.read_sql_query("""WITH FreightSumary AS (
    SELECT
        VendorNumber,
        Sum(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
),    

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
           on p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    
    
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSumary fs  
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""" ,conn)

    return Vendor_sales_summery



def clean_data(df):
    '''This function will clean Data'''
    # changing datatype to float 
    df['Volume'] = df ['Volume'].astype('float')

    # filling missing value to 0
    df.fiIlna(0, inplace = True)

    # removing spaces from categorial columns
    df.['VendorName'] = df['VendorName'].str.strip()
    df.['Description'] = df['Description'].str.strip()

    # Creating new columns for better Analysis
    Vendor_sales_summery['GrossProfit'] = Vendor_sales_summery['TotalSalesDollars'] - Vendor_sales_summery['TotalPurchaseDollars']
    Vendor_sales_summery['ProfitMargin'] = (Vendor_sales_summery['GrossProfit'] / Vendor_sales_summery['TotalSalesDollars']) * 100
    Vendor_sales_summery['StockTurnover'] = Vendor_sales_summery['TotalSalesQuantity']/Vendor_sales_summery['TotalPurchaseQuantity']
    Vendor_sales_summery['SalestoPurchaseRatio'] = Vendor_sales_summery['TotalSalesDollars']/Vendor_sales_summery['TotalPurchaseDollars']

    return df


if __name__ == '__main__':
    # creating database connection
    conn = sqtite3.connect('inventory.db')

    logging.info('Creating Vendor Sumary Table....')
    summery_df = create_vendor_sumary(conn)
    logging.info(summery_df.head())

    logging. info('Cleaning Data....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())
    
    logging.info( 'Ingesting data....')
    ingest_db(clean_df,'Vendor_sales_summery',conn)
    logging. info('Completed')
