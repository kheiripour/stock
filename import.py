import csv
import mysql.connector
from time import time
from os import system
from statistics import mean
from collections import defaultdict

importFile='C:\\Users\\98912\\Desktop\\Stock\\csv\\Import14001009raw.csv'

def cleanRecord(value):  
    value = str(value.replace(',',''))
    try:
        r= int(value)
    except :
            try:
                if float(value)== float('inf'): 
                    r= 999999
                else: 
                    r= float(value)

            except :
                
                    r= None          
    return r 


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="123",
  database='stock'
)

mycursor = mydb.cursor()

t0 = time()
outputtext1= "Adding new Items:\n"
sql = 'SELECT id, name FROM symbols'
symbols = mycursor.execute(sql)
symbolsdict = dict((y,x) for x , y in mycursor.fetchall())

sql = 'SELECT symbolid,date,coefficient FROM adjustments'
adjustments=mycursor.execute(sql)
adjustments=mycursor.fetchall()

dailydict = defaultdict(dict)

with open (importFile,'r',encoding='UTF-8',newline='') as impfile:
    
    reader = csv.reader(impfile)
    reader = list(reader)
    fields={k: v for k,v in enumerate(reader[0])}
    del reader[0]
    rowsnubmer = len(list(reader))
    i=0
       
    for row in reader:
        i +=1
        sym = row[1]
        row[1]=symbolsdict.get(row[1],0)
        if row[1] == 0 : 
            
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],sym)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            continue
        
        row[0]=cleanRecord(row[0])
        
        for r in range(2,len(row)):
            row[r] =cleanRecord(row[r])
        
        # Adding raw data to dicts.
        for field in range(2,len(fields)):
            dailydict[row[1]][fields[field]]=dailydict[row[1]].get(fields[field],[])+[(row[0],row[field])]

        #calc realBuyerPow: (realBuyersVol /  realBuyersCount) * fPrice
        try:realBuyerPow=(row[16]/row[12])*row[5] 
        except Exception as er:
            e = 'realBuyerPow: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            realBuyerPow=None
        dailydict[row[1]]['realBuyerPow']=dailydict[row[1]].get('realBuyerPow',[])+[(row[0],realBuyerPow)]

        #calc realSellerPow: (realSellersVol /  realSellersCount) * fPrice
        try:realSellerPow= row[17]/row[13] *row[5]
        except Exception as er:
            e = 'realSellerPow: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            realSellerPow= None
        dailydict[row[1]]['realSellerPow']=dailydict[row[1]].get('realSellerPow',[])+[(row[0],realSellerPow)]

        #calc realBuyerSellerPowRate: realBuyerPow / realSellerPow; if realSellerPow=0 will be 999999 and realBuyerPow==0 then realBuyerPow==0
        if realBuyerPow==0:
            realBuyerPow==0
        else:
            try:realBuyerSellerPowRate= 999999 if realSellerPow == 0 or realSellerPow==None else (realBuyerPow / realSellerPow)  # if realSellerPow = None then realBuyerSellerPowRate=9999999 or None?  
            except Exception as er:
                e = 'realBuyerSellerPowRate: ' + str (er)
                try:
                    sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                    
                    val = (row[0],row[1],e)  
                                    
                    mycursor.execute(sql, val)
                    mydb.commit()
                except:
                    pass
                realBuyerSellerPowRate=999999  if str(er)== 'division by zero' else None
        dailydict[row[1]]['realBuyerSellerPowRate']=dailydict[row[1]].get('realBuyerSellerPowRate',[])+[(row[0],realBuyerSellerPowRate)]

        #calc legalBuyerSellerPowRate: ((legalBuyersVol /  legalBuyersCount) * fPrice) / ((legalSellersVol /  legalSellersCount) * fPrice) if ((legalSellersVol /  legalSellersCount) * fPrice)> 0 else 999999
        
        if row[18]==0:
            legalBuyerSellerPowRate=0
        else:
            try:legalBuyerSellerPowRate= ((row[18] / row[14]) * row[5]) / ((row[19] /  row[15]) * row[5]) if ((row[19] /  row[15]) * row[5])> 0 else 999999   # if makhraj = None then legalBuyerSellerPowRate=9999999 or None?  
            except Exception as er:
                e = 'legalBuyerSellerPowRate: ' + str (er)
                try:
                    sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                    
                    val = (row[0],row[1],e)  
                                    
                    mycursor.execute(sql, val)
                    mydb.commit()
                except:
                    pass
                legalBuyerSellerPowRate=999999  if str(er)== 'division by zero' else None
        dailydict[row[1]]['legalBuyerSellerPowRate']=dailydict[row[1]].get('legalBuyerSellerPowRate',[])+[(row[0],legalBuyerSellerPowRate)]
        
        #calc realStakeholdersMoneyFlow: (realBuyersVol * fPrice) - (realSellersVol * fPrice)
        try:realStakeholdersMoneyFlow= (row[16] * row[5]) - (row[17] * row[5])
        except Exception as er:
            e = 'realStakeholdersMoneyFlow: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            realStakeholdersMoneyFlow= None
        dailydict[row[1]]['realStakeholdersMoneyFlow']=dailydict[row[1]].get('realStakeholdersMoneyFlow',[])+[(row[0],realStakeholdersMoneyFlow)]

        #calc volOnBasicVol: vol/basicVol
        try:volOnBasicVol=  row[3]/row[4]
        except Exception as er:
            e = 'volOnBasicVol: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            volOnBasicVol= None
        dailydict[row[1]]['volOnBasicVol']=dailydict[row[1]].get('volOnBasicVol',[])+[(row[0],volOnBasicVol)]
        
        #calc candleColor: (close - open) >0 : 1; (close - open) <0 : -1; (close - open) =0 : 0
        try:
            c = row[10] - row[7]
            if c>0:candleColor = 1 
            elif c<0: candleColor = -1 
            else: candleColor = 0
        except Exception as er:
            e = 'candleColor: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            candleColor= None
        dailydict[row[1]]['candleColor']=dailydict[row[1]].get('candleColor',[])+[(row[0],candleColor)]


        #calc candleType: 5 candle types:  0 for Doji, 1 for Hammer, 2 for inverted hammer,3 for marabuzo and 4 for others called Normal.
        
        if row[8] == row[9]!=None:
            candleType=0
        else:
            try:
                M = abs(row[7]-row[10])/(row[8]-row[9])    # abs(open - close)/(max-min)
                
                if 0.6<= M <=1:
                    candleType=3
                elif M<=0.1:
                    candleType=0
                else:
                    X= abs (max(row[7],row[10])-row[8])/(row[8]-row[9])   #abs (max(open,close) - max)/(max-min)
                    Y = abs (min(row[7],row[10])-row[9])/(row[8]-row[9])  #abs (min(open,close) - min)/(max-min)
                    if X<=0.2 and Y >=0.5:
                        candleType = 1
                    elif X>=0.5 and Y<=0.2:
                        candleType = 2
                    else:
                        candleType=4


            except Exception as er:
                e = 'candleType: ' + str (er)
                try:
                    sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                    
                    val = (row[0],row[1],e)  
                                    
                    mycursor.execute(sql, val)
                    mydb.commit()
                except:
                    pass
                candleType= None
        dailydict[row[1]]['candleType']=dailydict[row[1]].get('candleType',[])+[(row[0],candleType)]

        #calc closeFinalDev: (close - fprice)/fprice
        try:closeFinalDev=  (row[10]-row[5])/row[5]
        except Exception as er:
            e = 'closeFinalDev: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            closeFinalDev= None
        dailydict[row[1]]['closeFinalDev']=dailydict[row[1]].get('closeFinalDev',[])+[(row[0],closeFinalDev)]

        #calc closeMinDev: (close - min)/min
        try:closeMinDev=  (row[10]-row[9])/row[9]
        except Exception as er:
            e = 'closeMinDev: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            closeMinDev= None
        dailydict[row[1]]['closeMinDev']=dailydict[row[1]].get('closeMinDev',[])+[(row[0],closeMinDev)]


        #calc realBuyersVolOnVol: realBuyersvol/vol
        try:realBuyersVolOnVol=  row[16]/row[3]
        except Exception as er:
            e = 'realBuyersVolOnVol: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            realBuyersVolOnVol= None
        dailydict[row[1]]['realBuyersVolOnVol']=dailydict[row[1]].get('realBuyersVolOnVol',[])+[(row[0],realBuyersVolOnVol)]

        #calc legalSellersVolOnVol: legalSellersvol/vol
        try:legalSellersVolOnVol=  row[19]/row[3]
        except Exception as er:
            e = 'legalSellersVolOnVol: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            realBuyersVolOnVol= None
        dailydict[row[1]]['legalSellersVolOnVol']=dailydict[row[1]].get('legalSellersVolOnVol',[])+[(row[0],legalSellersVolOnVol)]

        #calc adPrice , adVol:

        coes = list(filter(lambda x: x[0]==row[1] and x[1]>row[0],adjustments ))
        coef = 1
        for x in coes: coef = x[2]*coef
        try: adPrice = row[5] * coef
        except Exception as er:
            e = 'adPrice: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            adPrice= None
        dailydict[row[1]]['adPrice']=dailydict[row[1]].get('adPrice',[])+[(row[0],adPrice)]

        try: adVol = row[3] / coef
        except Exception as er:
            e = 'adVol: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            adVol= None
        dailydict[row[1]]['adVol']=dailydict[row[1]].get('adVol',[])+[(row[0],adVol)]

        #Inserting new items into databse with independent feilds.
        sql1='INSERT INTO daily (date,symbolid'
        sql2='VALUES (%s,%s'%(row[0],row[1])
        for column in dailydict[row[1]].keys():
            sql1=sql1+','+column
            val=dailydict[row[1]][column][-1][1]
            if val!=None and val!="":
                sql2=sql2 +',' + str(val)
            else:
                sql2=sql2 +',' + 'NULL'

        sql=sql1 + ')' +sql2 + ')'
        
        try:
            mycursor.execute(sql)
            mydb.commit()
        except Exception as er:
            
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],str(er))  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
    
        pr = str(int((i/rowsnubmer) *100)) + '%'
        system('cls')
        outputtext2 = outputtext1 + pr
        print (outputtext2)        

t1=time()
t=t1-t0
if t//60 >0:
    outputtext3=outputtext2+  ('\nTime elapsed: %i Minutes and %f Seconds\n'%(int(t)//60,t%60))
    
else:
    outputtext3=outputtext2 +  ('\nTime elapsed: %f Seconds\n'%(t%60))

system('cls')
print(outputtext3)

t0=time()

#Calculating dependent fields:
updailydict = defaultdict(dict)
try:del dailydict[0]

except :pass
outputtext3 = outputtext3 + '\nCalculatting dependent fields:\n' 

q=0
for symb in dailydict:

    q+=1      

    # Upcalc dailyRealBuyerPowerJump and tomorrowRealBuyerSellerPowRate:
    #getting before and after data in realBuyerSellerPowRate:

    sqlbefore= '(SELECT date,realBuyerSellerPowRate FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 1) ORDER BY date' %(dailydict[symb]['realBuyerSellerPowRate'][0][0],symb)
    sqlafter= '(SELECT date,realBuyerSellerPowRate FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 1) ORDER BY date' %(dailydict[symb]['realBuyerSellerPowRate'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()
    dailydict[symb]['realBuyerSellerPowRate']=bdate+dailydict[symb]['realBuyerSellerPowRate']+adate

    for d in range(0,len(dailydict[symb]['realBuyerSellerPowRate'])-1):
        
        date= dailydict[symb]['realBuyerSellerPowRate'][d][0]
        val=None        
        val = dailydict[symb]['realBuyerSellerPowRate'][d+1][1]
            
        if val!=None: updailydict[(symb,date)]['tomorrowRealBuyerSellerPowRate']= val

    # Upcalc dailyRealBuyerPowerJump
    #getting before and after data in realBuyerPow:
    sqlbefore= '(SELECT date,realBuyerPow FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 1) ORDER BY date' %(dailydict[symb]['realBuyerPow'][0][0],symb)
    sqlafter= '(SELECT date,realBuyerPow FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 1) ORDER BY date' %(dailydict[symb]['realBuyerPow'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()
    dailydict[symb]['realBuyerPow']=bdate+dailydict[symb]['realBuyerPow']+adate

    for d in range(1,len(dailydict[symb]['realBuyerPow'])):
        date= dailydict[symb]['realBuyerPow'][d][0]
        try: updailydict[(symb,date)]['dailyRealBuyerPowerJump']=dailydict[symb]['realBuyerPow'][d][1]/dailydict[symb]['realBuyerPow'][d-1][1]
        except: pass

    # Adjustments:

    #getting before and after data in fPrice:
    sqlbefore= '(SELECT date,fPrice FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 1) ORDER BY date' %(dailydict[symb]['fPrice'][0][0],symb)
    sqlafter= '(SELECT date,fPrice FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 1) ORDER BY date' %(dailydict[symb]['fPrice'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall() 
    dailydict[symb]['fPrice']=bdate+dailydict[symb]['fPrice']+adate


    #getting before and after data in fPriceDev:
    sqlbefore= '(SELECT date,fPriceDev FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 1) ORDER BY date' %(dailydict[symb]['fPriceDev'][0][0],symb)
    sqlafter= '(SELECT date,fPriceDev FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 1) ORDER BY date' %(dailydict[symb]['fPriceDev'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()
    dailydict[symb]['fPriceDev']=bdate+dailydict[symb]['fPriceDev']+adate

    leng=len(dailydict[symb]['fPrice'])
    for p in range(1,leng):
        fpricedev = dailydict[symb]['fPriceDev'][p][1]
        if fpricedev<-99:fpricedev=-100
        try:
            bprice = dailydict[symb]['fPrice'][p][1]/(1+(fpricedev/100))
        except Exception as er:
            e = 'adjustment before price: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (dailydict[symb]['fPrice'][p][0],symb,e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            continue
        dev = abs((bprice - dailydict[symb]['fPrice'][p-1][1]))/ bprice
        
        if dev > 0.1:
            coef = (dailydict[symb]['fPrice'][p][1]/(100+(dailydict[symb]['fPriceDev'][p][1]))/dailydict[symb]['fPrice'][p-1][1])*100
            try:
                sql='INSERT adjustments (symbolid,date,coefficient) VALUES(%i,%i,%s)'%(symb,dailydict[symb]['fPrice'][p][0],coef)
                mycursor.execute(sql)
                mydb.commit()
            except Exception as er:
                e = 'insert adjustment : ' + str (er)
                try:
                    sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                    
                    val = (dailydict[symb]['fPrice'][p][0],symb,e)  
                                    
                    mycursor.execute(sql, val)
                    mydb.commit()
                except:
                    pass

            sql= '(SELECT date,adPrice,adVol FROM daily  WHERE date < %i AND symbolid=%i) ORDER BY date DESC' %(dailydict[symb]['fPrice'][p][0],symb)
            ads=mycursor.execute(sql)
            ads = mycursor.fetchall()
            
            for date , adP,adV in ads:
                updailydict[(symb,date)]['adPrice']= updailydict[(symb,date)].get('adPrice',adP) * coef
                updailydict[(symb,date)]['adVol']= updailydict[(symb,date)].get('adVol',adV) / coef
                
                for i in range (len(dailydict[symb]['adPrice'])):
                    if dailydict[symb]['adPrice'][i][0]==date:
                        del dailydict[symb]['adPrice'][i]
                        break
                
                dailydict[symb]['adPrice'].insert(0,(date ,updailydict[(symb,date)]['adPrice']))  

                for i in range (len(dailydict[symb]['adVol'])):
                    if dailydict[symb]['adVol'][i][0]==date:
                        del dailydict[symb]['adVol'][i]
                        break
                
                dailydict[symb]['adVol'].insert(0,(date ,updailydict[(symb,date)]['adVol']))        




    #Upcalc laters adPrices:
    #getting before and after data in adPrice:
    sqlbefore= '(SELECT date,adPrice FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 60) ORDER BY date' %(dailydict[symb]['adPrice'][0][0],symb)
    sqlafter= '(SELECT date,adPrice FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 60) ORDER BY date' %(dailydict[symb]['adPrice'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()
    dailydict[symb]['adPrice']=bdate+dailydict[symb]['adPrice']+adate
    tlen =len(dailydict[symb]['adPrice'])-len(adate)
    alen = len(adate)
    blen = len(bdate)
    # ranges for tommorow:
    r1_1 =(blen-1) if blen>1 else 0
    r2_1 = tlen - ((1 - alen) if alen<1 else 0)

    # ranges for fivedays:
    r1_5 =(blen-5) if blen>5 else 0
    r2_5 = tlen - ((5 - alen) if alen<5 else 0)

    # ranges for tendays:
    r1_10 =(blen-10) if blen>10 else 0
    r2_10 = tlen - ((10 - alen) if alen<10 else 0)

    # ranges for twentydays:
    r1_20 =(blen-20) if blen>20 else 0
    r2_20 = tlen - ((20 - alen) if alen<20 else 0)

    # ranges for thirtydays:
    r1_30 =(blen-30) if blen>30 else 0
    r2_30 = tlen - ((30 - alen) if alen<30 else 0)

    # ranges for sixtydays:
    r1_60 =(blen-60) if blen>60 else 0
    r2_60 = tlen - ((60 - alen) if alen<60 else 0)

    end = tlen + (blen if blen<4 else 4)
    #Upcalc DaysLaterAdPrice
    for p in range(0,end):
        val=None
        date = dailydict[symb]['adPrice'][p][0]
        adp=dailydict[symb]['adPrice'][p][1]
        #Upcalc tomorrowAdPriceDev:
        if p >=r1_1 and p<r2_1:
            val=dailydict[symb]['fPriceDev'][p+1][1]
            if val!=None: updailydict[(symb,date)]['tomorrowAdPriceDev']= val

        #Upcalc fiveDaysLaterAdPrice
        if p >=r1_5 and p<r2_5:
            
            val=None
            try: val = dailydict[symb]['adPrice'][p+5][1] 
            except: pass
            if val!=None: 
                updailydict[(symb,date)]['fiveDaysLaterAdPrice']= val
                try:updailydict[(symb,date)]['fiveDaysLaterAdPriceDev']= (val-adp)/adp
                except:pass
                
                

        #Upcalc tenDaysLaterAdPrice
        if p >=r1_10 and p<r2_10:

            val=None
            try: val = dailydict[symb]['adPrice'][p+10][1] 
            except: pass       
            if val!=None: 
                updailydict[(symb,date)]['tenDaysLaterAdPrice']= val
                try:updailydict[(symb,date)]['tenDaysLaterAdPriceDev']= (val-adp)/adp
                except:pass


        #Upcalc twentyDaysLaterAdPrice
        if p >=r1_20 and p<r2_20:

            val=None
            try: val = dailydict[symb]['adPrice'][p+20][1] 
            except: pass       
            if val!=None: 
                updailydict[(symb,date)]['twentyDaysLaterAdPrice']= val
                try:updailydict[(symb,date)]['twentyDaysLaterAdPriceDev']= (val-adp)/adp
                except:pass

        #Upcalc thirtyDaysLaterAdPrice
        if p >=r1_30 and p<r2_30:

            val=None
            try: val = dailydict[symb]['adPrice'][p+30][1] 
            except: pass       
            if val!=None: 
                updailydict[(symb,date)]['thirtyDaysLaterAdPrice']= val
                try:updailydict[(symb,date)]['thirtyDaysLaterAdPriceDev']= (val-adp)/adp
                except:pass

        #Upcalc sixtyDaysLaterAdPrice
        if p >=r1_60 and p<r2_60:

            val=None
            try: val = dailydict[symb]['adPrice'][p+60][1] 
            except: pass   
            if val!=None: 
                updailydict[(symb,date)]['sixtyDaysLaterAdPrice']= val
                try:updailydict[(symb,date)]['sixtyDaysLaterAdPriceDev']= (val-adp)/adp
                except:pass

        #Upcalc yesterdayOnFourDaysBeforeAdPrice:
        val=None
        if p>3:
            try: updailydict[(symb,date)]['yesterdayOnFourDaysBeforeAdPrice'] = dailydict[symb]['adPrice'][p-1][1] /dailydict[symb]['adPrice'][p-4][1]
            except:pass

                


    #Upcalc fifteenAdVolAve and adVolOnFifteenAdVolAve:
    #getting before and after data in adVols:
    
    sqlbefore= '(SELECT date,adVol FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 15) ORDER BY date' %(dailydict[symb]['adVol'][0][0],symb)
    sqlafter= '(SELECT date,adVol FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 15) ORDER BY date' %(dailydict[symb]['adVol'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()

    dailydict[symb]['adVol']=bdate+dailydict[symb]['adVol']+adate

    lb = len(bdate)
    if lb>15 :
        s=15
    elif lb>9:
        s= lb
    else:
        s=10

    for d in range(s,len(dailydict[symb]['adVol'])):
        date=dailydict[symb]['adVol'][d][0]
        vol=dailydict[symb]['adVol'][d][1]
        ave=None
        beg = 0 if d<16 else d-15
        vols = list(filter(lambda x: x!=None ,list(map(lambda x: x[1],dailydict[symb]['adVol'][beg:d]))))
        if len(vols) > 10: ave= mean(vols)
        if ave != None: 
            updailydict[(symb,date)]['fifteenAdVolAve']=ave    
            if  ave !=0 and vol!=None: updailydict[(symb,date)]['adVolOnFifteenAdVolAve']=vol/ave


    # Upcalc threeDaysLegalBuyPowerRateAve: 3 days before ave of legalBuyerSellerPowRate. 1 days tolerance( 2 days mean in 3 last days)
    # getting before and after data in legalBuyerSellerPowRate:


    sqlbefore= '(SELECT date,legalBuyerSellerPowRate FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 3) ORDER BY date' %(dailydict[symb]['legalBuyerSellerPowRate'][0][0],symb)
    sqlafter= '(SELECT date,legalBuyerSellerPowRate FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 3) ORDER BY date' %(dailydict[symb]['legalBuyerSellerPowRate'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall() 
    dailydict[symb]['legalBuyerSellerPowRate']=bdate+dailydict[symb]['legalBuyerSellerPowRate']+adate

    
    s=3 if len(bdate)>2 else 2
    
    for d in range(1,len(dailydict[symb]['legalBuyerSellerPowRate'])):
        date=dailydict[symb]['legalBuyerSellerPowRate'][d][0]
        rate=dailydict[symb]['legalBuyerSellerPowRate'][d-1][1]

        if rate!=None: updailydict[(symb,date)]['yesterdayLegalBuyPowerRate']= rate
        if d>=s:
            ave=None
            beg = 0 if d<4 else d-3
            rates = list(filter(lambda x: x!=None ,list(map(lambda x: x[1],dailydict[symb]['legalBuyerSellerPowRate'][beg:d]))))
            if len(rates) > 1: ave= mean(rates)
            if ave != None: updailydict[(symb,date)]['threeDaysLegalBuyPowerRateAve']=ave    
            

    # Upcalc threeDaysRealBuyPowerRateAve:3 days before ave of realBuyerSellerPowRate. 1 days tolerance( 2 days mean in 3 last days)
    # getting before and after data in realBuyerSellerPowRate: 
    
    sqlbefore= '(SELECT date,realBuyerSellerPowRate FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 3) ORDER BY date' %(dailydict[symb]['realBuyerSellerPowRate'][0][0],symb)
    sqlafter= '(SELECT date,realBuyerSellerPowRate FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 3) ORDER BY date' %(dailydict[symb]['realBuyerSellerPowRate'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall() 
    dailydict[symb]['realBuyerSellerPowRate']=bdate+dailydict[symb]['realBuyerSellerPowRate']+adate

    s=3 if len(bdate)>2 else 2

    for d in range(s,len(dailydict[symb]['realBuyerSellerPowRate'])):
        date=dailydict[symb]['realBuyerSellerPowRate'][d][0]
        ave=None
        beg = 0 if d<4 else d-3
        rates = list(filter(lambda x: x!=None ,list(map(lambda x: x[1],dailydict[symb]['realBuyerSellerPowRate'][beg:d]))))
        if len(rates) > 1: ave= mean(rates)
        if ave != None: updailydict[(symb,date)]['threeDaysRealBuyPowerRateAve']=ave   


    # Upcalc threeDaysCandleTypeAve:3 days before ave of candleTypes. 1 days tolerance( 2 days mean in 3 last days)
    # getting before and after data in candleTypes: 
    
    sqlbefore= '(SELECT date,candleType FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 3) ORDER BY date' %(dailydict[symb]['candleType'][0][0],symb)
    sqlafter= '(SELECT date,candleType FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 3) ORDER BY date' %(dailydict[symb]['candleType'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall() 
    dailydict[symb]['candleType']=bdate+dailydict[symb]['candleType']+adate

    s=3 if len(bdate)>2 else 2

    for d in range(s,len(dailydict[symb]['candleType'])):
        date=dailydict[symb]['candleType'][d][0]
        ave=None
        beg = 0 if d<4 else d-3
        candles = list(filter(lambda x: x!=None ,list(map(lambda x: x[1],dailydict[symb]['candleType'][beg:d]))))
        if len(candles) > 1: ave= mean(candles)
        if ave != None: updailydict[(symb,date)]['threeDaysCandleTypeAve']=ave
   

    pr= str(int(q/len(dailydict)*100)) +'%'

    outputtext4 = outputtext3 + pr
    system('cls')
    print(outputtext4)


t1=time()
t=t1-t0
if t//60 >0:
    outputtext5=outputtext4 + '\nTime elapsed: %i Minutes and %f Seconds\n'%(int(t)//60,t%60)
    
else:
    outputtext5=outputtext4 + '\nTime elapsed: %f Seconds\n'%(t%60)
    
system('cls')
print(outputtext5)

t0=time()
i=0
outputtext5 = outputtext5 + '\nUpdating database by dependent fields:\n'
#Updating records to db:
for symb , date in updailydict:
    i+=1
    sql= 'UPDATE daily SET '  
     
    for col in updailydict[(symb,date)]:
        sql = sql + col + ' = ' + str(updailydict[(symb,date)][col])+' , '

    sql = sql[0:-2] + ' WHERE symbolid=%i AND date = %i'%(symb,date)

    mycursor.execute(sql)
    mydb.commit()

    pr = str(int((i/len(updailydict))*100)) + '%'
    outputtext6 = outputtext5 + pr
    system('cls')
    print(outputtext6)


mydb.close()



t1=time()
t=t1-t0
if t//60 >0:
    outputtext6=outputtext6+  '\nTime elapsed: %i Minutes and %f Seconds\n'%(int(t)//60,t%60)
    
else:
    outputtext6=outputtext6+  '\nTime elapsed: %f Seconds\n'%(t%60)
    
system('cls')
print(outputtext6)


