import pandas as pd
import threading
import numpy as np
from pandas_datareader import data
from datetime import date, timedelta
from cstock.request import Requester
from cstock.hexun_engine import HexunEngine
from retry import retry
import time

# Define datetime parameter
YAHOO_END_DAY=date.today()
YAHOO_BEGIN_DAY=YAHOO_END_DAY-timedelta(10)
# HEXUN_ENGINE always get the most recent data

def CleanStock(f):
    """input stock data and clean them"""
    dt=pd.read_csv(f)
    c=dt[dt.columns[0]]  
    cd=[]
    for i in c:
        cd.append(i[-8:-2])
    return cd

def cstock_to_yahoo(stock_id):
    """Transform stock ID to yahoo stock ticker"""
    begin=str(stock_id)[0:3]
    s=''
    if begin in ['600', '601', '603']:
       s=str(stock_id)+'.SS'
    else:
       s=str(stock_id)+'.SZ'
    return s

@retry(tries=3,delay=2)
def SelectStockRule(stock):
    """Stock Selection Rule"""
    print stock
    # convert stock code and retrieve historical data
    ticker=cstock_to_yahoo(stock)    
    try:
       
       # From HexunEngine, get today's stock information
       engine = HexunEngine()
       requester=Requester(engine)
       stock_obj=requester.request(stock)
       SD=stock_obj[0].as_dict()
       print "Hexun's date=", SD['date']
       todayVolume=SD['volume']
       #todayPrice=SD['price']
       
       # History data from yahoo      
       df=data.DataReader(ticker, 'yahoo', YAHOO_BEGIN_DAY, YAHOO_END_DAY)
       #print df
       # clean the dataframe
       v=df['Volume']
       cdf=df[v>0]
       volume=cdf['Volume']
       L=len(cdf.index)-1
       #close=cdf['Close']
       print "Yahoo lastest volume date=", cdf.index[-1]
       print "Yahoo beginning volume date=", cdf.index[0]
    except:
        print "Can't get data"
        return False   
    # Analysis the stock data
    try:
       # Intraday data
       condition=(todayVolume>0 and volume[L]> 0) \
                  and (todayVolume / (volume[L]+1.0)<0.4
                  and todayVolume / (volume[L-1]+1.0)<0.99
                  and todayVolume / (volume[L-2]+1.0)<0.99
                  and todayVolume / (volume[L-3]+1.0)<0.99
                  and todayVolume / (volume[L-4]+1.0)<0.99
                  and todayVolume / (volume[L-5]+1.0)<0.99)
                  #and volume[L] /(volume[L-1])<0.1)
         
       # Lowest volume condition                    
       condition1=volume[L]>0 and (volume[L] / (volume[L-1]+1.0))<0.5 \
                   and (volume[L] / (volume[L-2]+1.0))<0.99 \
                   and (volume[L] / (volume[L-3]+1.0))<0.99 \
                   and (volume[L] / (volume[L-4]+1.0))<0.99 \
                   and (volume[L] / (volume[L-5]+1.0))<0.99 \
                   #and (volume[L-5] / (volume[L-6]+1.0))<0.99 
       if condition == True: 
           return stock 
       else:
           return False
    except IndexError:
        return False

####################################################################
class Counter:
    def __init__(self):
        self.lock=threading.Lock() #define a lock attribute
        self.value=0               #define a value attribute
        
    def increment(self):
        self.lock.acquire() #acquire the global lock
        self.value=self.value+1 #the value variable can only be accessed by one thread
        self.lock.release() #release the lock
        return self.value
        
    def v(self):
        return self.value
c=Counter() #define a global counter variable

class SelectedStockList:
    def __init__(self):
        self.lock=threading.Lock()
        self.value=[]
        
    def add_item(self,item):
        self.lock.acquire()
        self.value.append(item)
        self.lock.release()
        return self.value
        
    def get_value(self):
        return self.value
selectedStockList=SelectedStockList() #define a list to store the stock

class Worker(threading.Thread):
    """ Thread worker for selecting stocks in input list"""
    def __init__(self,StockList):
        threading.Thread.__init__(self)
        self.StockList=StockList
    
    def run(self):
        #global c
        global selectedStockList
        for i in self.StockList:
            print "Threadname=", self.getName()
            print "Active Threads=", threading.active_count()
            result=SelectStockRule(i)
            #c.increment()
            #print "Counter=", c.v()
            print "Result=", result
            if result != False:
                selectedStockList.add_item(str(result))
            print "selectedStockList=", selectedStockList.get_value()
            print "............................................................"
            time.sleep(0.1)

def workDistributor(stockList,threadCounts):
    """Distribute work to workers"""
    #Total 
    TOTAL=len(stockList)
    reshapedList=np.array(stockList).reshape(threadCounts,(TOTAL/threadCounts))
    for i in reshapedList:
        Worker(i).start()

# Read mainboard stock into list
FILE='~/Desktop/PandasFinance/MainBoardList.txt'                        
MainBoardStockList=CleanStock(FILE)
MainBoardStockList=MainBoardStockList[0:2000]
workDistributor(MainBoardStockList,10)