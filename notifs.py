import urllib.request
import time
from pyfcm import *
import pymysql
import datetime
import time

#set the authentication key
push_service = FCMNotification(api_key="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
iphone_key = FCMNotification(api_key="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

execution_number = 0


#remove all notifs when app starts to prevent mass notification if script crashes

reset = pymysql.connect(host='XXXX', port=XXXX, user='XXXX', passwd='XXXX', db='XXXX')
reset_cur = reset.cursor()
reset_cur.execute("SELECT * FROM mobilenotifications where status='unsent'")

for row in reset_cur:
    row_id = row[0]
    #print(row_id)
    reset_cur2 = reset.cursor()
    reset_cur2.execute("UPDATE mobilenotifications SET status= " + "'reset'" + " where id = " + str(row_id))
    reset_cur2.close()
reset.commit()
reset_cur.close()
reset.close()
while True:

    success = 0
    failure = 0
    
    date_str = time.strftime("%d-%m-%y")
    
    #logfile
    output = open(date_str + ".csv", "a")


    #open a connection
    conn = pymysql.connect(host='XXXX', port=XXXX, user='XXXX', passwd='XXXX', db='XXXX')
    cur = conn.cursor()
    cur.execute("SELECT * FROM mobilenotifications where status='unsent'")

    for row in cur:

        memberid = row[1]
        
        #set to 0 before counting
        total_notifs = 0
        #connect to db to count number of notics
        cur2 = conn.cursor()
        cur2.execute("SELECT * FROM notifications where memberid = " + str(memberid) +" and action = 'notify'")
        #add up the number of notifications
        for j in cur2:
            total_notifs = total_notifs + 1
        
        #print(row)
        #get the notification ID for the current row
        row_id = row[0]
        registration_id = row[2]
        #print(registration_id)
        message_title = row[3]
        link = row[4]
        
        data_message = {"badge": total_notifs, "body": message_title, "link": link}
        result = push_service.notify_single_device(registration_id=registration_id, data_message=data_message)



        #changes only where notification ID matches
        #if success after android try, update to success
        if result['success'] == 1:
            cur2 = conn.cursor()
            cur2.execute("UPDATE mobilenotifications SET status= " + "'success'" + " where id = " + str(row_id))
            cur2.close()
            success = success + 1
        #if failure, try iPhone API key
        if result['success'] == 0:

            data_message = {"link": link}
            
            result = iphone_key.notify_single_device(registration_id=registration_id, data_message=data_message, message_body=row[3], badge=total_notifs)
            #update for success
            if result['success'] == 1:
                cur2 = conn.cursor()
                cur2.execute("UPDATE mobilenotifications SET status= " + "'success'" + " where id = " + str(row_id))
                cur2.close()
                success = success + 1           
            #update for failure
            if result['success'] == 0:
                cur2 = conn.cursor()
                cur2.execute("UPDATE mobilenotifications SET status= " + "'failure'" + " where id = " + str(row_id))
                cur2.close()
                failure = failure + 1
            
        #write the log
        output.write(str(datetime.datetime.now())+ "," + str(execution_number)+ "," + str(row_id) +"," + str(row_id)+","+ str(registration_id)+","+ str(memberid)+","+ str(result) + "\n")
        #print(result)

    print("Execution number: " + str(execution_number)+ "\t" + date_str+ "\t" + "Success: " +str(success) + "\t" +"Failure: " + str(failure))
    execution_number = execution_number + 1
    conn.commit()    
    output.close()
    cur.close()
    conn.close()
    time.sleep(10)
                
            
            


    

