import csv
import mysql.connector
from time import daylight, time
from os import system
from statistics import mean
from collections import defaultdict



importFile='C:\\Users\\98912\\Desktop\\Stock\\imp4noDup.csv'

def cleanRecord(value):  
    
    try:
        r= int(value.replace(',',''))
    except :
            try:
                if float(value.replace(',',''))== float('inf'): 
                    r= 999999
                else: 
                    r= float(value.replace(',',''))

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
        row[1]=symbolsdict.get(row[1],0)
        if row[1] == 0 : continue
        ed= row[2]
        row[0]=cleanRecord(row[0])
        
        for r in range(2,len(row)):
            row[r] =cleanRecord(row[r])
        
        # Adding raw data to dicts.
        for field in range(2,len(fields)):
            dailydict[row[1]][fields[field]]=dailydict[row[1]].get(fields[field],[])+[(row[0],row[field])]

        #calc realBuyerPow: (realBuyersVolume /  realBuyersCount) * fPrice
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

        #calc realSellerPow: (realSellersVolume /  realSellersCount) * fPrice
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

        #calc realBuyerSellerPowRate: realBuyerPow / realSellerPow; if realSellerPow=0 will be 999999
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

        #calc legalBuyerSellerPowRate: ((legalBuyersVolume /  legalBuyersCount) * fPrice) / ((legalSellersVolume /  legalSellersCount) * fPrice) if ((legalSellersVolume /  legalSellersCount) * fPrice)> 0 else 999999
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
        
        #calc realStakeholdersMoneyFlow: (realBuyersVolume * fPrice) - (realSellersVolume * fPrice)
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

        #calc volumeOnBasicVolume: volume/basicVolume
        try:volumeOnBasicVolume=  row[3]/row[4]
        except Exception as er:
            e = 'volumeOnBasicVolume: ' + str (er)
            try:
                sql = "INSERT INTO errors (date,symbolid,error) VALUES (%s,%s,%s)"
                
                val = (row[0],row[1],e)  
                                
                mycursor.execute(sql, val)
                mydb.commit()
            except:
                pass
            volumeOnBasicVolume= None
        dailydict[row[1]]['volumeOnBasicVolume']=dailydict[row[1]].get('volumeOnBasicVolume',[])+[(row[0],volumeOnBasicVolume)]
        
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
    # Upcalc fifteendVolAve and volumeOnFifteenDaysBasicVolumeAve:
    #getting before and after data in vols:
    
    sqlbefore= '(SELECT date,volume FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 15) ORDER BY date' %(dailydict[symb]['volume'][0][0],symb)
    sqlafter= '(SELECT date,volume FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 15) ORDER BY date' %(dailydict[symb]['volume'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()

    dailydict[symb]['volume']=bdate+dailydict[symb]['volume']+adate


    for d in range(15,len(dailydict[symb]['volume'])):
        date=dailydict[symb]['volume'][d][0]
        vol=dailydict[symb]['volume'][d][1]
        ave=None
        vols = list(filter(lambda x: x!=None ,list(map(lambda x: x[1],dailydict[symb]['volume'][d-15:d]))))
        if len(vols) > 10: ave= mean(vols)
        if ave != None: 
            updailydict[(symb,date)]['fifteendVolAve']=ave    
            if  ave !=0 and vol!=None: updailydict[(symb,date)]['volumeOnFifteenDaysBasicVolumeAve']=vol/ave



        

    # Upcalc dailyRealBuyerPowerJump and tomorrowRealBuyerSellerPowRate:
    #getting before and after data in realBuyerSellerPowRate:

    sqlbefore= '(SELECT date,realBuyerSellerPowRate FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 1) ORDER BY date' %(dailydict[symb]['realBuyerSellerPowRate'][0][0],symb)
    sqlafter= '(SELECT date,realBuyerSellerPowRate FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 1) ORDER BY date' %(dailydict[symb]['realBuyerSellerPowRate'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()
    dailydict[symb]['realBuyerSellerPowRate']=bdate+dailydict[symb]['realBuyerSellerPowRate']+adate

    for d in range(0,len(dailydict[symb]['realBuyerSellerPowRate'])):
        
        date= dailydict[symb]['realBuyerSellerPowRate'][d][0]

        if d>0:
            val = dailydict[symb]['realBuyerSellerPowRate'][d-1][1]
            
            if val!=None:
                updailydict[(symb,date)]['dailyRealBuyerPowerJump']= val

        if d<len(dailydict[symb]['realBuyerSellerPowRate'])-1:

            val = dailydict[symb]['realBuyerSellerPowRate'][d+1][1]
            
            if val!=None:
                updailydict[(symb,date)]['tomorrowRealBuyerSellerPowRate']= val


    # Upcalc laters prices:
    #getting before and after data in fPrice:
    sqlbefore= '(SELECT date,fPrice FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 60) ORDER BY date' %(dailydict[symb]['fPrice'][0][0],symb)
    sqlafter= '(SELECT date,fPrice FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 60) ORDER BY date' %(dailydict[symb]['fPrice'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()
    dailydict[symb]['fPrice']=bdate+dailydict[symb]['fPrice']+adate
    tlen =len(dailydict[symb]['fPrice'])-len(adate)

    # ranges for tommorow:
    r1_1 =(len(bdate)-1) if len(bdate)>1 else 0
    r2_1 = tlen - ((1 - len(adate)) if len(adate)<1 else 0)

    # ranges for fivedays:
    r1_5 =(len(bdate)-5) if len(bdate)>5 else 0
    r2_5 = tlen - ((5 - len(adate)) if len(adate)<5 else 0)

    # ranges for tendays:
    r1_10 =(len(bdate)-10) if len(bdate)>10 else 0
    r2_10 = tlen - ((10 - len(adate)) if len(adate)<10 else 0)

    # ranges for twentydays:
    r1_20 =(len(bdate)-20) if len(bdate)>20 else 0
    r2_20 = tlen - ((20 - len(adate)) if len(adate)<20 else 0)

    # ranges for thirtydays:
    r1_30 =(len(bdate)-30) if len(bdate)>30 else 0
    r2_30 = tlen - ((30 - len(adate)) if len(adate)<30 else 0)

    # ranges for sixtydays:
    r1_60 =(len(bdate)-60) if len(bdate)>60 else 0
    r2_60 = tlen - ((60 - len(adate)) if len(adate)<60 else 0)

    #Upcalc DaysLaterPrice
    for p in range(0,tlen):
        val=None
        #Upcalc lastDayPrice:
        if (p>=len(bdate)-1 and p<tlen-1) or (p==tlen-1 and len(adate)>0):
            date = dailydict[symb]['fPrice'][p+1][0]
            val = dailydict[symb]['fPrice'][p][1]              
        if val!=None: updailydict[(symb,date)]['lastDayPrice']= val

        #date for all next fields:
        date = dailydict[symb]['fPrice'][p][0]

        #Upcalc tomorrowPrice:
        if p >=r1_1 and p<r2_1:
            val=dailydict[symb]['fPrice'][p+1][1]
            if val!=None: updailydict[(symb,date)]['tomorrowPrice']= val

        #Upcalc fiveDaysLaterPrice
        if p >=r1_5 and p<r2_5:

            val=None
            # 1 day tolerance: 
            for i in [0,1]:
                try:
                    val = dailydict[symb]['fPrice'][p+5+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPrice'][p+5-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['fiveDaysLaterPrice']= val
                

        #Upcalc tenDaysLaterPrice
        if p >=r1_10 and p<r2_10:

            val=None
            #2 days tolerance: 
            for i in range(0,3):
                try:
                    val = dailydict[symb]['fPrice'][p+10+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPrice'][p+10-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['tenDaysLaterPrice']= val


        #Upcalc twentyDaysLaterPrice
        if p >=r1_20 and p<r2_20:

            val=None
            #3 days tolerance: 
            for i in range(0,4):
                try:
                    val = dailydict[symb]['fPrice'][p+20+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPrice'][p+20-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['twentyDaysLaterPrice']= val

        #Upcalc thirtyDaysLaterPrice
        if p >=r1_30 and p<r2_30:

            val=None
            #4 days tolerance: 
            for i in range(0,5):
                try:
                    val = dailydict[symb]['fPrice'][p+30+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPrice'][p+30-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['thirtyDaysLaterPrice']= val

        #Upcalc sixtyDaysLaterPrice
        if p >=r1_60 and p<r2_60:

            val=None
            #5 days tolerance: 
            for i in range(0,6):
                try:
                    val = dailydict[symb]['fPrice'][p+60+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPrice'][p+60-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['sixtyDaysLaterPrice']= val


    # Upcalc laters PriceDevs:
    #getting before and after data in fPriceDev:
    sqlbefore= '(SELECT date,fPriceDev FROM daily  WHERE date < %i AND symbolid=%i ORDER BY date DESC LIMIT 60) ORDER BY date' %(dailydict[symb]['fPriceDev'][0][0],symb)
    sqlafter= '(SELECT date,fPriceDev FROM daily  WHERE date > %i AND symbolid=%i ORDER BY date LIMIT 60) ORDER BY date' %(dailydict[symb]['fPriceDev'][-1][0],symb)
    bdate=mycursor.execute(sqlbefore)
    bdate = mycursor.fetchall()
    adate=mycursor.execute(sqlafter)
    adate = mycursor.fetchall()
    dailydict[symb]['fPriceDev']=bdate+dailydict[symb]['fPriceDev']+adate
    tlen =len(dailydict[symb]['fPriceDev'])-len(adate)

    # ranges for fivedays:
    r1_5 =(len(bdate)-5) if len(bdate)>5 else 0
    r2_5 = tlen - ((5 - len(adate)) if len(adate)<5 else 0)

    # ranges for tendays:
    r1_10 =(len(bdate)-10) if len(bdate)>10 else 0
    r2_10 = tlen - ((10 - len(adate)) if len(adate)<10 else 0)

    # ranges for twentydays:
    r1_20 =(len(bdate)-20) if len(bdate)>20 else 0
    r2_20 = tlen - ((20 - len(adate)) if len(adate)<20 else 0)

    # ranges for thirtydays:
    r1_30 =(len(bdate)-30) if len(bdate)>30 else 0
    r2_30 = tlen - ((30 - len(adate)) if len(adate)<30 else 0)

    # ranges for sixtydays:
    r1_60 =(len(bdate)-60) if len(bdate)>60 else 0
    r2_60 = tlen - ((60 - len(adate)) if len(adate)<60 else 0)

    #Upcalc DaysLaterPriceDev:
    for p in range(0,tlen):
        
        #Upcalc threeDaysPriceDevSum:
        val=None
        if (p>=len(bdate)-3 and p<tlen-3) or(p==tlen-1 and len(adate)>2) or (p==tlen-2 and len(adate)>1 or (p==tlen-3 and len(adate)>0)):
            date = dailydict[symb]['fPriceDev'][p+3][0]
            val = 0
            c = 0
            # 1 day tolerance
            for s in range(4):
                if dailydict[symb]['fPriceDev'][p+2-s][1] != None:
                    try:
                        val = val + dailydict[symb]['fPriceDev'][p+2-s][1]
                        c+=1
                    except:
                        pass    
                if c==3: break

        if val!=None and c==3 : updailydict[(symb,date)]['threeDaysPriceDevSum']= val
    
    
    
        date = dailydict[symb]['fPriceDev'][p][0]


        #Upcalc fiveDaysLaterPriceDev
        if p >=r1_5 and p<r2_5:

            val=None
            # 1 day tolerance: 
            for i in [0,1]:
                try:
                    val = dailydict[symb]['fPriceDev'][p+5+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPriceDev'][p+5-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['fiveDaysLaterPriceDev']= val
                

        #Upcalc tenDaysLaterPriceDev
        if p >=r1_10 and p<r2_10:

            val=None
            #2 days tolerance: 
            for i in range(0,3):
                try:
                    val = dailydict[symb]['fPriceDev'][p+10+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPriceDev'][p+10-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['tenDaysLaterPriceDev']= val


        #Upcalc twentyDaysLaterPriceDev
        if p >=r1_20 and p<r2_20:

            val=None
            #3 days tolerance: 
            for i in range(0,4):
                try:
                    val = dailydict[symb]['fPriceDev'][p+20+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPriceDev'][p+20-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['twentyDaysLaterPriceDev']= val

        #Upcalc thirtyDaysLaterPriceDev
        if p >=r1_30 and p<r2_30:

            val=None
            #4 days tolerance: 
            for i in range(0,5):
                try:
                    val = dailydict[symb]['fPriceDev'][p+30+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPriceDev'][p+30-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['thirtyDaysLaterPriceDev']= val

        #Upcalc sixtyDaysLaterPriceDev
        if p >=r1_60 and p<r2_60:

            val=None
            #5 days tolerance: 
            for i in range(0,6):
                try:
                    val = dailydict[symb]['fPriceDev'][p+60+i][1]
                    if val!= None: 
                        break
                    else:
                        val = dailydict[symb]['fPriceDev'][p+60-i][1]
                    if val!= None: break   
                except:
                    pass
                    
            if val!=None: updailydict[(symb,date)]['sixtyDaysLaterPriceDev']= val

    


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


