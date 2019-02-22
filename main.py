import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas_datareader.data as web
import datetime
import numpy as np

start = datetime.datetime(2006, 1, 1)
end = datetime.date.today()

# TD Monthly
td = web.DataReader("TD", "yahoo", start, end)
tdW = 6

ry = web.DataReader("RY", "yahoo", start, end)
ryW = 5.8

bmo = web.DataReader("BMO", "yahoo", start, end)
bmoW = 5

cm = web.DataReader("CM", "yahoo", start, end)
cmW = 4.5

bns = web.DataReader("BNS", "yahoo", start, end)
bnsW = 4.3

enb = web.DataReader("ENB", "yahoo", start, end)
enbW = 3.5

su = web.DataReader("SU", "yahoo", start, end)
suW = 2.8

bam = web.DataReader("BAM", "yahoo", start, end)
bamW = 2.7

mfc = web.DataReader("MFC", "yahoo", start, end)
mfcW = 2.1

trp = web.DataReader("TRP", "yahoo", start, end)
trpW = 1.9

# US Blue Chip
amzn = web.DataReader("AMZN", "yahoo", start, end)
amznW = 10.2

goog = web.DataReader("GOOG", "yahoo", start, end)
googW = 5.6

fb = web.DataReader("FB", "yahoo", start, end)
fbW = 5.3

msft = web.DataReader("MSFT", "yahoo", start, end)
msftW = 4.7

bkng = web.DataReader("BKNG", "yahoo", start, end)
bkngW = 4.1

baba = web.DataReader("BABA", "yahoo", start, end)
babaW = 3.8

ba = web.DataReader("BA", "yahoo", start, end)
baW = 3.4

v = web.DataReader("V", "yahoo", start, end)
vW = 3.3

unh = web.DataReader("UNH", "yahoo", start, end)
unhW = 2.8

ma = web.DataReader("MA", "yahoo", start, end)
maW = 2.8

stock = pd.DataFrame({'TD': td['Close'], 'RBC': ry['Close'], 'BMO': bmo['Close'],
                      'ImperialBank': cm['Close'], 'NovaScotia': bns['Close'],
                      'Enbridge': enb['Close'], 'Suncor': su['Close'],
                      'Brookfield': bam['Close'], 'Manulife': mfc['Close'],
                      'TransCanada': trp['Close'], 'Amazon': amzn['Close'],
                      'Google': goog['Close'], 'Facebook': fb['Close'],
                      'MicroSoft': msft['Close'], 'Booking': bkng['Close'],
                      'Alibaba': baba['Close'], 'Boeing': ba['Close'],
                      'Visa': v['Close'], 'UnitedHealth': unh['Close'],
                      'MasterCard': ma['Close']})

rate = stock.apply(lambda x: np.log(x) - np.log(x.shift(1))).fillna(0)
stock = stock.fillna(0)

stock.to_csv("stock.csv")
rate.to_csv("rate.csv")
