#! environment
import pandas as pd
import pandas_datareader.data as web
import datetime
import numpy as np
from sqlalchemy import create_engine


end = datetime.date.today()
start = end - datetime.timedelta(days = 1)
eng = create_engine('')#db connection


# TD Monthly
td = web.DataReader("TD", "yahoo", start, end)

ry = web.DataReader("RY", "yahoo", start, end)

bmo = web.DataReader("BMO", "yahoo", start, end)

cm = web.DataReader("CM", "yahoo", start, end)

bns = web.DataReader("BNS", "yahoo", start, end)

enb = web.DataReader("ENB", "yahoo", start, end)

su = web.DataReader("SU", "yahoo", start, end)

bam = web.DataReader("BAM", "yahoo", start, end)

mfc = web.DataReader("MFC", "yahoo", start, end)

trp = web.DataReader("TRP", "yahoo", start, end)

# US Blue Chip
amzn = web.DataReader("AMZN", "yahoo", start, end)

goog = web.DataReader("GOOG", "yahoo", start, end)

fb = web.DataReader("FB", "yahoo", start, end)

msft = web.DataReader("MSFT", "yahoo", start, end)

bkng = web.DataReader("BKNG", "yahoo", start, end)

baba = web.DataReader("BABA", "yahoo", start, end)

ba = web.DataReader("BA", "yahoo", start, end)

v = web.DataReader("V", "yahoo", start, end)

unh = web.DataReader("UNH", "yahoo", start, end)

ma = web.DataReader("MA", "yahoo", start, end)

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

rate = stock.apply(lambda x: np.log(x) - np.log(x.shift(1))).reset_index()
stock = stock.reset_index()
stock[stock['Date'] == pd.Timestamp(end)].to_sql('stock', eng, if_exists='append', index=False)
rate[rate['Date'] == pd.Timestamp(end)].to_sql('rate', eng, if_exists='append', index=False)
