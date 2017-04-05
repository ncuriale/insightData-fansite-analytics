import numpy as np
import re
import os
from datetime import *
import calendar
import pybloom_live

def processFile(filename):
    
    #------------------------------------------------------------------------------
    #
    # Purpose: This function opens the input file and processes the data line by line
    #
    #------------------------------------------------------------------------------
    
    #open file
    os.chdir('..')
    f = open(filename, 'rb')
    
    #initialize 
    host_data=[]
    timestamp_data=[]
    resource_data=[]
    http_data=[]
    bytes_data=[]
    full_data=[]
    
    i = 0 
         
    #run through each line  
    for line in f:
        if(i<100000):#ENQSOH-1608697 
            try: 
                line=line.decode('ascii')
                #print(i,line) 
                                          
                #call sort function and add to array
                line,host,timestamp,resource,http,bytes=sortLine(line)
                full_data.append(line) 
                host_data.append(host)                   
                timestamp_data.append(timestamp) 
                resource_data.append(resource) 
                http_data.append(http) 
                bytes_data.append(bytes) 
                
                #if(i%10000==0):
                #    print (i)
                    
            except UnicodeDecodeError:
                print (i,"was not a ascii-encoded unicode string")
                #try another encoding or try binary to sort  
                                  
        #increase count
        i+=1
                 
    f.close()            
    
    return full_data,host_data,timestamp_data,resource_data,http_data,bytes_data

def sortLine(line):
    
    #------------------------------------------------------------------------------
    #
    # Purpose: Split data into respective values using regex
    #
    #------------------------------------------------------------------------------
    
    line = re.sub(r'\s+', ' ',line.strip())#strip line and remove white space
    host = line.split(' - -')[0]
    timestamp = re.findall("\[(.*?)\]", line)[0]
    request = re.findall("\"(.*?)\"", line)[0]
    resource = re.findall("/[!-~]*",str(request))[0]
    http=line.split('" ')[-1].split()[0]
    bytes=line.split('" ')[-1].split()[-1] 
    if (bytes=='-'):#check bytes
        bytes=0    
    #lineData=[host,timestamp,resource,http,bytes]    
    
    return line,host,timestamp,resource,http,bytes

class UTCm4(tzinfo):
        def utcoffset(self,dt): 
            return timedelta(hours=-4,minutes=0) 
        def tzname(self,dt): 
            return "-0400"
        def dst(self,dt): 
            return timedelta(0) 
        
def calcTime(timestamp):   
    
    #------------------------------------------------------------------------------
    #
    # Purpose: This functions calculates time from the input timeetamp structure 
    # Uses datetime as a dependency
    #
    #------------------------------------------------------------------------------
      
    date=timestamp.split(':')[0]
    year=int(date.split('/')[2])
    month=list(calendar.month_abbr).index(date.split('/')[1])
    day=int(date.split('/')[0])
    hour=int(timestamp.split(':')[1])
    minute=int(timestamp.split(':')[2])
    second=int(timestamp.split(':')[3].split(' ')[0])
    zone=UTCm4()
    time=datetime(year,month,day,hour,minute,second,tzinfo=zone)

    return time
    
def countActivity(data):
    
    #------------------------------------------------------------------------------
    #
    # Purpose: Count frequency of specific users/IPs provided in data file
    #            Uses a bloom filter with try/except to search
    #
    #------------------------------------------------------------------------------
   
    #initialize    
    type=[]
    freq=[]
    icnt=1
        
    start = datetime.now()
    #count
    for i in range(len(data)):
        
        if(i==0):#add first entry to type 
            type.append(data[i])
            freq.append(1) 
            
            #initialize bloom filter
            bf = pybloom_live.pybloom.ScalableBloomFilter()
            bf.add(data[i])
            
        else:                            
            if(data[i] in bf):                
                try:#test to see if BF gives result
                    freq[type.index(data[i])]+=1

                except:#if false-positive,add entry to type         
                    type.append(data[i])
                    freq.append(1) 
                    bf.add(data[i])
                
            else:#add entry to type
                type.append(data[i])
                freq.append(1) 
                bf.add(data[i])     
         
        #if(i==len(data)*icnt/10):
        #    icnt+=1
        #    print (". ")   
            
    finish = datetime.now()
    print ('Time:',finish-start)
    
    return type,freq

def countResource(data,bytes_data):
    
    #------------------------------------------------------------------------------
    #
    # Purpose: Counts frequency a specific resource is accessed and then multiplies by
    #            byte size to get bandwidth intensity
    #             Uses a bloom filter with try/except to search
    #
    #------------------------------------------------------------------------------
    
    #initialize    
    type=[]
    freq=[]
    byte=[]
    icnt=1
        
    start = datetime.now()
    
    #count
    for i in range(len(data)):
        
        if(i==0):#add first entry to type 
            type.append(data[i])
            freq.append(1) 
            byte.append(bytes_data[i]) 
            
            #initialize bloom filter
            bf = pybloom_live.pybloom.BloomFilter(len(data))
            bf.add(data[i]) 
            
        else:            
            if(data[i]in bf):
                try:#test to see if BF gives result
                    freq[type.index(data[i])]+=1

                except:#if false-positive,add entry to type         
                    type.append(data[i])
                    freq.append(1)   
                    byte.append(bytes_data[i]) 
                    bf.add(data[i])

            else:#add entry to type 
                type.append(data[i])
                freq.append(1)   
                byte.append(bytes_data[i])  
                bf.add(data[i])  
        
        #if(i==len(data)*icnt/10):
        #    icnt+=1
        #    print (". ")  
                
    #calculation of bandwidth
    freq=np.asarray(freq,dtype=np.int32)
    byte=np.asarray(byte,dtype=np.int32)
    bandwidth=np.empty_like(freq)
    for i in range(len(freq)):
        bandwidth[i]=freq[i]*byte[i] 
        
    finish = datetime.now()
    print ('Time:',finish-start)
    
    return type,bandwidth

def countBusy(data):
    
    #------------------------------------------------------------------------------
    #
    # Purpose: Computes the 60 min period of time which the site is accessed most 
    #            frequent
    #            Uses while loop and counters to keep track of data/times
    #
    #------------------------------------------------------------------------------
    
    #initialize  
    freq=[]
    type=[]
           
    start = datetime.now()
    
    #initialize time indexes
    initial,final=calcTime(data[0]),calcTime(data[-1])#start and final time of data
    total=(final-initial).seconds#total number of steps
    aCnt=0#index for amount of events on site during 60 min 
    k=0
    while(1): 
        
        #set up time curr and end and indexes        
        curr=calcTime(data[k])
        if(aCnt==0):
            end=curr+timedelta(minutes=60)
            if(end>final):
                end=final
            time1=curr#index for start of 60 min
            time2=end#index for end of 60 min
            
        #check if time is before/equal to end time    
        if( k==len(data)-1 ):
            aCnt+=1   
            break 
        elif(curr<=end):
            aCnt+=1
        else:
            break
            
        k+=1
    
    #initial values
    index1=0
    index2=k
    type.append(time1.strftime("%d/%b/%Y:%T %Z"))
    freq.append(aCnt)        
    
    #print(aCnt,time1,index1,index2)
    #print(time1.strftime("%d/%b/%Y:%T %Z"))
    
    #go through each of the total steps and 
    #find entries then are added to range
    #and that are removed from range
    buff=50#buff set to 50 -- data change(change if higher traffic noticed)
    for i in range(total):
        
        #increase time indexes
        time1+=timedelta(seconds=1)
        time2+=timedelta(seconds=1)
    
        #check remove from index1
        remove=0
        remFlag=True 
        k=index1
        while(remFlag):
            curr=calcTime(data[k])
            if(curr<time1 and k<len(data)-1):
                remove+=1   
                k+=1
            elif(curr>=time1 or k==len(data)-1):
                remFlag=False 
        
        #if(index1+buff>len(data)):
        #    max=len(data)-1
        #else:    
        #    max=index1+buff
        #for k in range(index1,max):
        #    curr=calcTime(data[k])
        #    if(curr<time1):
        #        remove+=1  
                 
             
        #check add from index2
        add=0
        addFlag=True 
        k=index2
        while(addFlag):
            curr=calcTime(data[k])
            if(curr<time2 and k<len(data)-1):
                add+=1   
                k+=1
            elif(curr>=time2 or k==len(data)-1):
                addFlag=False  
                               
        #if(index2+buff>len(data)):
        #    max=len(data)-1
        #else:    
        #    max=index2+buff
        #for k in range(index2,max):
        #    curr=calcTime(data[k])
        #    if(curr<time2):
        #        add+=1    
        
        #change indexes
        index1+=remove
        index2+=add
        aCnt=aCnt+add-remove
        type.append(time1.strftime("%d/%b/%Y:%T %Z"))
        freq.append(aCnt)
        
    finish = datetime.now()
    print ('Time:',finish-start)
    
    return type,freq

def detectFails(full_data,host_data,timestamp_data,http_data,host,f1):
    
    #------------------------------------------------------------------------------
    #
    # Purpose: Detects frequent failed attempts to access the website by a user
    #            Based on http data = 401 as failed login attempt
    #
    #------------------------------------------------------------------------------
    
    #use unique hosts data from f1 unless not running
    if(f1!=1):
        host,_ = countActivity(host_data)
        
    #initialize  
    full=[]
    icnt=1
           
    start = datetime.now()
    
    final=calcTime(timestamp_data[-1])
    #go through unique hosts and reset counters
    for i in range(len(host)):  
        blockFlag=False
        blockCnt=0
        loginCnt=0
        
        #go through all host data
        for k in range(len(host_data)):
        
            #check for unique hosts in hosts data and check if failed attempt with http data
            if(host[i]==host_data[k] and http_data[k]=="401"):
                #print(host_data[k],'----------------------------------------->>')
                
                #find current time
                curr=calcTime(timestamp_data[k])
                
                if(blockFlag==False):
                    loginCnt+=1
                    
                    if (loginCnt==1):
                        end=curr+timedelta(seconds=20)
                        if(end>final):
                            end=final
                    else:
                        if(curr>end):
                            loginCnt=0#over 20sec timeframe, restart cnt
                        elif(loginCnt==3):#implicitly checks -> curr<=end
                            blockFlag=True
                            blockTime=curr
                            loginCnt=0
                            
                elif(blockFlag==True):
                    blockCnt+=1
                    
                    if (blockCnt==1):
                        end=blockTime+timedelta(minutes=5)
                        if(end>final):
                            end=final    
                     
                    if(curr<=end):#during 5 min block period,save blocked login attempts
                        full.append(full_data[k])

                    else:#end of 5 min -> (curr>end)
                        blockFlag=False
                        blockCnt=0
                        
                        #this is actually a proper login attempt 
                        #so new end time is needed
                        loginCnt=1
                        end=curr+timedelta(seconds=20)
                        if(end>final):
                            end=final
                        
            
        #if(i==len(host_data)*icnt/10):
        #    icnt+=1
        #    print (". ")  
             
    finish = datetime.now()
    print ('Time:',finish-start)
        
    return full

def printTop(type,param,feature,filename,num=10): 
    
    #------------------------------------------------------------------------------
    #
    # Purpose: Sort and print results to file
    #
    #------------------------------------------------------------------------------
    
    #Sort values
    top=sorted(zip(type,param), key=lambda x: x, reverse=False)

    #open file to write
    f = open(filename, 'w')
    
    #check for num and length
    if(len(type)<num):
        num=len(type)
        
    #print results 
    if(feature==1):#user and site visits
        for i in range(num):
            f.write(str(top[i][0]) + ',' + str(top[i][1]) +'\n')
            #print(str(top[i][0]) + ',' + str(top[i][1]))
                  
    elif(feature==2):#resource and bandwidth intensity
        for i in range(num):
            f.write(str(top[i][0]) +'\n')
            #print(str(top[i][0]))
    
    elif(feature==3):#time period start and site visits
        for i in range(num):
            f.write(str(top[i][0]) + ',' + str(top[i][1]) +'\n')
            #print(str(top[i][0]) + ',' + str(top[i][1]))
    
    f.close          

def printFails(full,filename): 
    
    #------------------------------------------------------------------------------
    #
    # Purpose: Print results to file
    #
    #------------------------------------------------------------------------------
    
    #open file
    f = open(filename, 'w')
    
    #print results 
    for i in range(len(full)):
        f.write(str(full[i]) +'\n')
        
    f.close()
    
def main():
    
    ############Unit Test Flags######
    f1=1
    f2=1
    f3=1
    f4=1
    ##################################   
        
    #Read data file
    print("Initialize: Reading and processing data file ...")
    filename='log_input/log.txt'
    full_data,host_data,timestamp_data,resource_data,http_data,bytes_data=processFile(filename)    
    print("Complete\n")
        
    #Feature 1
    if(f1==1):
        print("Feature 1: Searching for 10 most active hosts ...")
        host,hostfreq = countActivity(host_data) 
        printTop(host,hostfreq,1,'log_output/hosts.txt',10) 
        print("Complete\n")
    
    #Feature 2
    if(f2==1):
        print("Feature 2: Searching for 10 most bandwidth-intensive ...")        
        resource,bandwidth = countResource(resource_data,bytes_data)     
        printTop(resource,bandwidth,2,'log_output/resources.txt',10)  
        print("Complete\n")
    
    #Feature 3
    if(f3==1):
        print("Feature 3: Searching for 10 busiest sites during 60 min ...")        
        time,timefreq = countBusy(timestamp_data)     
        printTop(time,timefreq,3,'log_output/hours.txt',10)  
        print("Complete\n")
    
    #Feature 3
    if(f4==1):
        print("Feature 4: Detecting failed login attempts ...")  
        if(f1!=1):
            host=[]       
        full = detectFails(full_data,host_data,timestamp_data,http_data,host,f1)     
        printFails(full,'log_output/blocked.txt') 
        print("Complete\n")          

if __name__ == "__main__":
    main()
    
