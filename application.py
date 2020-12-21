import os
from flask import Flask, redirect, render_template, request
import pypyodbc
import time
import random
import redis
import pickle
import hashlib

app = Flask(__name__)

server = 'ahaanrajesh.database.windows.net'
database = 'earthquakes'
username = 'raj'
password = 'Azure@123'
driver = '{ODBC Driver 13 for SQL Server}'
myHostname = "ahaan.redis.cache.windows.net"
myPassword = "89LwGGRPONsY+JJwv6C0W7wCQHVhB55F7O31rIRrVZw="

r = redis.Redis(host='ahaan.redis.cache.windows.net',
                port=6379, db=0, password='89LwGGRPONsY+JJwv6C0W7wCQHVhB55F7O31rIRrVZw=')


def disdata():
    cnxn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    print("Hi")
    cursor = cnxn.cursor()
    start = time.time()
    cursor.execute("SELECT TOP 10000 * FROM [quake]")
    row = cursor.fetchall()
    end = time.time()
    executiontime = end - start
    return render_template('searchearth.html', ci=row, t=executiontime)


def randrange(rangfro=None, rangto=None, num=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
    timeq = []
    mag1_li = []
    mag2_li = []
    time_li = []
    final = []
    for i in range(0, int(num)):
        mag1 = round(random.uniform(rangfro, rangto), 1)
        mag2 = round(random.uniform(rangfro, rangto), 1)
        if mag1 > mag2:
            success = "SELECT count(*) from [quake] where depthError>'" + str(mag2) + "' and depthError <" + str(mag1)
        else:
            success = "SELECT count(*) from [quake] where depthError>'" + str(mag1) + "' and depthError <" + str(mag2)

        print(mag1)
        print(mag2)
        hash = hashlib.sha224(success.encode('utf-8')).hexdigest()
        key = "redis_cache:" + hash

        if (r.get(key)):
            print("redis cached")
        else:
            print(key)
            print(r)
            # Do MySQL query
            print("Execution failed")
            cursor.execute(success)
            data = cursor.fetchall()
            rows = []
            for j in data:
                print(j)
                row_count = j
                rows.append(str(j))
                new_row = rows
            # Put data into cache for 1 hour
            r.set(key, pickle.dumps(list(rows)))
            r.expire(key, 36);

        # if (i < 1):
        first_time = time.time()
        first_execute = first_time - start
        timeq.append(first_execute)
        check123 = "The Depth 1 " + str(mag1) + " The Depth 2 " + str(mag2) + " The time is " + str(
            first_execute) + "The count is " + str(row_count)
        final.append(check123)
        print(first_execute)

        # print(new_row)
        # print ("Hello")

        cursor.execute(success)
    print("Step4")
    # print(rows)
    end = time.time()
    exectime = end - start
    return render_template('count.html', t=final, u=mag2_li, s=timeq, ci=rows, en=exectime)


def randrange_time(rangfro=None, rangto=None, num=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
    timeq = []
    mag1_li = []
    mag2_li = []
    time_li = []
    final = []
    for i in range(0, int(num)):
        mag1 = round(random.uniform(rangfro, rangto), 1)
        mag2 = round(random.uniform(rangfro, rangto), 1)
        if mag1 > mag2:
            success = "SELECT count(*) from [quake] where depthError>'" + str(mag2) + "' and depthError <" + str(mag1)
        else:
            success = "SELECT count(*) from [quake] where depthError>'" + str(mag1) + "' and depthError <" + str(mag2)
        # Do MySQL query
        print("Execution failed")
        cursor.execute(success)
        data = cursor.fetchall()
        rows = []
        row_count = 0
        for j in data:
            print(j)
            row_count = j
            rows.append(str(j))
            new_row = rows
        first_time = time.time()
        first_execute = first_time - start
        timeq.append(first_execute)
        check123 = "The Depth 1 " + str(mag1) + " The Depth 2 " + str(mag2) + " The time is " + str(
            first_execute) + "The count is " + str(row_count)
        final.append(check123)
        print(first_execute)

        # print(new_row)
        # print ("Hello")

        cursor.execute(success)
    print("Step4")
    # print(rows)
    end = time.time()
    exectime = end - start
    return render_template('count123.html', t=final, u=mag2_li, s=timeq, ci=rows, en=exectime)


def randrange_out(rangfro=None, rangto=None, num=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
    timeq = []
    mag1_li = []
    mag2_li = []
    time_li = []
    final = []
    for i in range(0, int(num)):
        mag1 = round(random.uniform(rangfro, rangto), 1)
        mag2 = round(random.uniform(rangfro, rangto), 1)
        if mag1 > mag2:
            success = "SELECT count(*) from [quake] where depthError>'" + str(mag2) + "' and depthError <" + str(mag1)
        else:
            success = "SELECT count(*) from [quake] where depthError>'" + str(mag1) + "' and depthError <" + str(mag2)

        print(mag1)
        print(mag2)
        hash = hashlib.sha224(success.encode('utf-8')).hexdigest()
        key = "redis_cache:" + hash

        if (r.get(key)):
            print("redis cached")
        else:
            print(key)
            print(r)
            # Do MySQL query
            print("Execution failed")
            cursor.execute(success)
            data = cursor.fetchall()
            rows = []
            row_count = 0
            for j in data:
                print(j)
                row_count = j
                rows.append(str(j))
                new_row = rows
            # Put data into cache for 1 hour
            r.set(key, pickle.dumps(list(rows)))
            r.expire(key, 36);

        # if (i < 1):
        first_time = time.time()
        first_execute = first_time - start
        timeq.append(first_execute)
        check123 = "The Depth 1 " + str(mag1) + " The Depth 2 " + str(mag2) + " The time is " + str(
            first_execute) + "The count is " + str(row_count)
        final.append(check123)
        print(first_execute)

        # print(new_row)
        # print ("Hello")

        cursor.execute(success)
    print("Step4")
    # print(rows)
    end = time.time()
    exectime = end - start
    return render_template('count1.html', t=final, u=mag2_li, s=timeq, ci=rows, en=exectime)


def randrange1(rangfro=None, rangto=None, lat1=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()

    success = "SELECT latitude,longitude,time,depthError from [quake] where depthError>'" + str(
        rangfro) + "' and depthError <'" + str(rangto) \
              + "' and latitude>" + str(lat1)

    print("Execution failed")
    cursor.execute(success)
    data = cursor.fetchall()
    print("Step4")
    print(data)
    return render_template('searchearth.html', ci=data)


def splitdata(magone=None, magtwo=None, split=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
    timeq = []
    mag1_li = []
    mag2_li = []
    time_li = []
    final = []
    print(split)
    mag1 = int(magone)
    mag2 = int(magtwo)
    diff = (mag2 - mag1) / int(split)
    print(diff)
    rows = []
    while (mag1 < int(magtwo)):
        print(mag1)
        mag2 = mag1 + diff
        sql = "SELECT count(*) from [quake] where mag BETWEEN'" + str(mag1) + "' and " + str(mag2)
        cursor.execute(sql)
        data = cursor.fetchall()
        print(data)
        for j in data:
            print('in for')
            rows.append(j)
            print(rows)
        mag1 = mag2
        print(mag2)
    #     hash = hashlib.sha224(sql.encode('utf-8')).hexdigest()
    #     key = "redis_cache:" + hash

    #     if (r.get(key)):
    #        print("redis cached")
    #     else:
    #         print(key)
    #         print(r)
    #        # Do MySQL query
    #         print("Execution failed")
    #         cursor.execute(sql)
    #         data = cursor.fetchall()
    #         rows = []
    #         row_count = 0
    #         for j in data:
    #             print(j)
    #             row_count = j
    #             rows.append(str(j))
    #             new_row = rows
    #         # Put data into cache for 1 hour
    #         r.set(key, pickle.dumps(list(rows)) )
    #         r.expire(key, 36);
    #
    #     #if (i < 1):
    #     first_time = time.time()
    #     first_execute = first_time - start
    #     timeq.append(first_execute)
    #     check123 = "The Depth 1 "+str(mag1)+" The Depth 2 "+str(mag2)+" The time is "+ str(first_execute)+"The count is "+str(row_count)
    #     final.append(check123)
    #     print(first_execute)
    #     cursor.execute(sql)
    # print("Step4")
    # end = time.time()
    # exectime = end - start
    return render_template('results.html', data=rows)



def ccode(code=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
    success = "SELECT * from [sp] where Code='" + str(code) + "'"
    #"SELECT * from sp where code = '" + rangfro + "'"
    print("Execution failed")
    cursor.execute(success)
    data = cursor.fetchall()
    print("Step4")
    print(data)
    end = time.time()
    exectime = end - start
    return render_template('codes.html', ci=data, en=exectime )

def cncode(code=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
   # success = "SELECT s.*,p.cost from [sp s],[pc p] where (s.Code = p.Code) Code='" + str(code) + "'"
    success = "select s.*, p.cost from sp s, pc p where (s.code = p.code) and s.code = '" + code + "'"
    print("Execution failed")
    cursor.execute(success)
    data = cursor.fetchall()
    print("Step4")
    print(data)
    end = time.time()
    exectime = end - start
    return render_template('codes1.html', ci=data, en=exectime )


def ques9code(yearfrom=None, yearto=None, costfrom=None, costto=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
    success="select s.*, p.cost from sp s, pc p where (s.code = p.code) and (s.year > " + str(yearfrom) + "and s.year < " + str(yearto) + ")  and (p.cost > " + str(costfrom) + " and p.cost < " + str(costto) + ")"

    print("Execution failed")
    cursor.execute(success)
    data = cursor.fetchall()
    print("Step4")
    print(data)
    end = time.time()
    exectime = end - start
    return render_template('count2.html',ci=data, en=exectime )

def ques10aa(code1=None, num=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
   # success = "SELECT s.*,p.cost from [sp s],[pc p] where (s.Code = p.Code) Code='" + str(code) + "'"
    for i in range(0, int(num)):
        success = "select s.*, p.cost from sp s, pc p where (s.code = p.code) and s.code = '" + code1 + "'"
        print("Execution failed")
        cursor.execute(success)
        data = cursor.fetchall()
        print("Step4")
        print(data)
    end = time.time()
    exectime = end - start
    return render_template('codes1.html', ci=data, en=exectime )


def ques10bcode(yearfrom=None, yearto=None, costfrom=None, costto=None, num=None):
    dbconn = pypyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = dbconn.cursor()
    start = time.time()
    for i in range(0, int(num)):
        success="select s.*, p.cost from sp s, pc p where (s.code = p.code) and (s.year > " + str(yearfrom) + "and s.year < " + str(yearto) + ")  and (p.cost > " + str(costfrom) + " and p.cost < " + str(costto) + ")"

        print("Execution failed")
        cursor.execute(success)
        data = cursor.fetchall()
        print("Step4")
        print(data)
    end = time.time()
    exectime = end - start
    return render_template('count2.html',ci=data, en=exectime )





@app.route('/')
def hello_world():
    return render_template('index.html')


@app.route('/displaydata', methods=['POST'])
def display():
    return disdata()


@app.route('/multiplerun', methods=['GET'])  # Redis cache extract
def randquery():
    rangfro = float(request.args.get('rangefrom'))
    rangto = float(request.args.get('rangeto'))
    num = request.args.get('nom')
    return randrange(rangfro, rangto, num)


@app.route('/multiplerun567', methods=['GET'])          #time
def randquery_out():
    rangfro = float(request.args.get('rangefrom'))
    rangto = float(request.args.get('rangeto'))
    num = request.args.get('nom')
    return randrange_out(rangfro, rangto, num)


@app.route('/multiplerun123', methods=['GET'])  # without redis/memcache extract
def randquery123():
    rangfro = float(request.args.get('rangefrom'))
    rangto = float(request.args.get('rangeto'))
    num = request.args.get('nom')
    return randrange_time(rangfro, rangto, num)


@app.route('/countrycode', methods=['GET'])  # countrycode
def countrycode():
    code = request.args.get('code')

    return ccode(code)

@app.route('/ques8', methods=['GET'])  # countrycode
def ques8():
    code1 = request.args.get('code1')

    return cncode(code1)


@app.route('/ques9', methods=['GET'])          #time
def ques9():
    yearfrom = int(request.args.get('yearfrom'))
    yearto = int(request.args.get('yearto'))
    costfrom = float(request.args.get('costfrom'))
    costto = float(request.args.get('costto'))

    return ques9code(yearfrom, yearto, costfrom, costto)

@app.route('/ques10a', methods=['GET'])  # without redis/memcache extract
def ques10a():
    code1 = request.args.get('code1')
    num = request.args.get('nom')
    return ques10aa(code1, num)

@app.route('/ques10b', methods=['GET'])          #time
def ques10b():
    yearfrom = int(request.args.get('yearfrom'))
    yearto = int(request.args.get('yearto'))
    costfrom = float(request.args.get('costfrom'))
    costto = float(request.args.get('costto'))
    num = request.args.get('nom')

    return ques10bcode(yearfrom, yearto, costfrom, costto,num)


@app.route('/split', methods=['GET'])  # split mag data
def splitquery():
    mag1 = float(request.args.get('mag1'))
    mag2 = float(request.args.get('mag2'))
    split = request.args.get('split')
    return splitdata(mag1, mag2, split)


@app.route('/routeone', methods=['GET'])
def routeone():
    rangfro = float(request.args.get('rangefrom'))
    rangto = float(request.args.get('rangeto'))
    num = request.args.get('nom')
    print("in button")
    val = request.args.get('radio')
    print(val)
    if val == '0':          #without cache
        print("do something")
        return randrange_time(rangfro, rangto, num)
    elif val == '1':
        print("Do else")
        return randrange(rangfro, rangto, num)


# 5.59
if __name__ == '__main__':
    app.run()
