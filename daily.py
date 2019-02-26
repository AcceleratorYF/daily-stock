#! environment
import pandas as pd
import pandas_datareader.data as web
import datetime
import numpy as np
from sqlalchemy import create_engine


end = datetime.date.today()
eng = create_engine('')#db connection
start = end - datetime.timedelta(days = 3) if end.weekday()==0 else end - datetime.timedelta(days = 1)


# TD Monthly
td1 = web.DataReader("TD", "yahoo", end, end)
td2 = web.DataReader("TD", "yahoo", start, start)
td = td2.append(td1)

ry1 = web.DataReader("RY", "yahoo", end, end)
ry2 = web.DataReader("RY", "yahoo", start, start)
ry = ry2.append(ry1)

bmo1 = web.DataReader("BMO", "yahoo", end, end)
bmo2 = web.DataReader("BMO", "yahoo", start, start)
bmo = bmo2.append(bmo1)

cm1 = web.DataReader("CM", "yahoo", end, end)
cm2 = web.DataReader("CM", "yahoo", start, start)
cm = cm2.append(cm1)

bns1 = web.DataReader("BNS", "yahoo", end, end)
bns2 = web.DataReader("BNS", "yahoo", start, start)
bns = bns2.append(bns1)

enb1 = web.DataReader("ENB", "yahoo", end, end)
enb2 = web.DataReader("ENB", "yahoo", start, start)
enb = enb2.append(enb1)

su1 = web.DataReader("SU", "yahoo", end, end)
su2 = web.DataReader("SU", "yahoo", start, start)
su = su2.append(su1)

bam1 = web.DataReader("BAM", "yahoo", end, end)
bam2 = web.DataReader("BAM", "yahoo", start, start)
bam = bam2.append(bam1)

mfc1 = web.DataReader("MFC", "yahoo", end, end)
mfc2 = web.DataReader("MFC", "yahoo", start, start)
mfc = mfc2.append(mfc1)

trp1 = web.DataReader("TRP", "yahoo", end, end)
trp2 = web.DataReader("TRP", "yahoo", start, start)
trp = trp2.append(trp1)
# US Blue Chip
amzn1 = web.DataReader("AMZN", "yahoo", end, end)
amzn2 = web.DataReader("AMZN", "yahoo", start, start)
amzn = amzn2.append(amzn1)

goog1 = web.DataReader("GOOG", "yahoo", end, end)
goog2 = web.DataReader("GOOG", "yahoo", start, start)
goog = goog2.append(goog1)

fb1 = web.DataReader("FB", "yahoo", end, end)
fb2 = web.DataReader("FB", "yahoo", start, start)
fb = fb2.append(fb1)

msft1 = web.DataReader("MSFT", "yahoo", end, end)
msft2 = web.DataReader("MSFT", "yahoo", start, start)
msft = msft2.append(msft1)

bkng1 = web.DataReader("BKNG", "yahoo", end, end)
bkng2 = web.DataReader("BKNG", "yahoo", start, start)
bkng = bkng2.append(bkng1)

baba1 = web.DataReader("BABA", "yahoo", end, end)
baba2 = web.DataReader("BABA", "yahoo", start, start)
baba = baba2.append(baba1)

ba1 = web.DataReader("BA", "yahoo", end, end)
ba2 = web.DataReader("BA", "yahoo", start, start)
ba = ba2.append(ba1)

v1 = web.DataReader("V", "yahoo", end, end)
v2 = web.DataReader("V", "yahoo", start, start)
v = v2.append(v1)

unh1 = web.DataReader("UNH", "yahoo", end, end)
unh2 = web.DataReader("UNH", "yahoo", start, start)
unh = unh2.append(unh1)

ma1 = web.DataReader("MA", "yahoo", end, end)
ma2 = web.DataReader("MA", "yahoo", start, start)
ma = ma2.append(ma1)


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