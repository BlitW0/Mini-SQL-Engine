# import sqlparse
from collections import defaultdict
import os
import re
import csv
import sys

DIR_PATH = 'files/'
META = defaultdict()
TABLE_DATA = defaultdict()
AGG = ['sum(','SUM(','max(','MAX(','min(','MIN(','avg(','AVG(','distinct(','DISTICT(','count(','COUNT(']

def processQuery(query):
    query = query.split()
    if validate(query):
        tableName = query[3].split(',')
        if len(tableName)>0:
            for name in tableName:
                if name not in TABLE_DATA:
                    print "ERROR : Table name - " + name + " not exist"
            if not TABLE_DATA[tableName[0]]:
                print "ERROR: " + tableName[0] + " not found"
                return -1;
            finalTable = TABLE_DATA[tableName[0]]
            finalRow = META[tableName[0]]
            for i in range(1,len(tableName)):
                if not TABLE_DATA[tableName[i]]:
                    print "ERROR: " + tableName[i] + " not found"
                    return -1;
                finalTable,finalRow = joinTable(finalTable,TABLE_DATA[tableName[i]],finalRow,META[tableName[i]])
        #WHERE CODE
        if len(query)>=5:
            flag,condition1 = processCondition(query[5],finalTable,finalRow)
            if not flag:
                return -1;
            if len(query)>6:
                operator = query[6]
                flag,condition2 = processCondition(query[7],finalTable,finalRow)
                if not flag:
                    return -1
                finalAns = []
                if operator == 'and' or operator == 'AND':
                    for row in condition1:
                        if row in condition2:
                            finalAns.append(row)
                if operator == 'or' or operator == 'OR' :
                    finalAns = condition1 + condition2
            else:
                finalAns = condition1
        else :
            finalAns = finalTable
        fields = query[1].split(',')
        co = 0
        for field in fields:
            for agg in AGG:
                if agg in field:
                    co += 1
        if co < len(fields) and co!= 0:
            print "Input Format not supportable"
            return -1
        elif co == len(fields):

            flag = AGGREGATE_FUNCTIONS(fields,finalAns,finalRow)
            if not flag:
                return -1
        else :
            flag = printOutput(fields,finalAns,finalRow)
        if not flag:
            return -1

    else:
        return -1

def AGGREGATE_FUNCTIONS(query,finalTable,finalRow):
    for name in query:
        name = name.split('(')[1][:-1]
        if name not in finalRow:
            print "field : " + name + " not exist"
            return False;

    for name in query:
        print name,
    print
    co = 0
    for name in query :
        if "distinct(" in name or "DISTICT(" in name:
            co += 1

    if co<len(query) and co!=0:
        print "ERROR: Check Input Format"
        return False

    for name in query:
        if "distinct(" in name or "DISTICT(" in name:
            name = name.split('(')[1][:-1]
            temp = []
            if name not in finalRow:
                print "ERROR: Check column name"
                return False
            ind = finalRow.index(name)
            print name
            for row in finalTable:
                if row[ind] not in temp:
                    temp.append(row[ind])
                    print row[ind],
            print
        if "max(" in name or "MAX(" in name:
            name = name.split('(')[1][:-1]
            if name not in finalRow:
                print "ERROR: Check column name"
                return False
            ans = 0
            ans = int(finalTable[0][finalRow.index(name)])
            for row in finalTable:
                ans = max(int(row[finalRow.index(name)]),ans)
            print ans,
        if "min(" in name or "MIN(" in name:
            name = name.split('(')[1][:-1]
            if name not in finalRow:
                print "ERROR: Check column name"
                return False
            ans = 0
            ans = int(finalTable[0][finalRow.index(name)])
            for row in finalTable:
                ans = min(int(row[finalRow.index(name)]),ans)
            print ans,
        if "sum(" in name or "SUM(" in name:
            name = name.split('(')[1][:-1]
            if name not in finalRow:
                print "ERROR: Check column name"
                return False
            ans = 0
            for row in finalTable:
                ans += int(row[finalRow.index(name)])
            print ans,
        if "avg(" in name or "AVG(" in name:
            name = name.split('(')[1][:-1]
            if name not in finalRow:
                print "ERROR: Check column name"
                return False
            ans = 0
            for row in finalTable:
                # print(int(row[finalRow.index(name)]))
                ans += int(row[finalRow.index(name)])
            print ans*1.0/len(finalTable),
        if "count(" in name or "COUNT(" in name:
            name = name.split('(')[1][:-1]
            if name not in finalRow:
                print "ERROR: Check column name"
                return False
            ans = len(finalTable)
            print ans,
    print

def printOutput(query,finalTable,finalRow):
    # print(len(finalTable))
    # print(finalRow)
    finalAns = []
    temp = []
    final = []
    if len(query) == 1:
        if query[0] == '*':
            for word in finalRow:
                t = word.split('.')[-1]
                if t not in temp:
                    temp.append(t)
                    final.append(word)
            query = final
    for name in query:
        if name not in finalRow:
            print "field : " + name + " not exist"
            return False;
    for name in query:
        print name,
    print
    i = 0
    # j = 0
    for row in finalTable:
        i += 1
        temp = []
        for name in query:
            temp.append(row[finalRow.index(name)].split('\r')[0])
        finalAns.append(tuple(temp))
    #print(len(finalAns),i)
    for row in finalAns:
        print row
    return True

def processCondition(query,finalTable,finalRow):
    ans = []
    #print 'l1'
    if '>=' in query:
        query = query.split('>=')
        try:
            var = int(query[-1])
            if query[0] not in finalRow:
                print "Column : " + col + " doesn't exist"
                return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) >= var:
                    ans.append(finalTable[i])
            return True,ans
        except:
            for col in query:
                if col not in finalRow:
                    print "Column : " + col + " doesn't exist"
                    return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) >= int(finalTable[i][finalRow.index(query[1])]):
                    ans.append(finalTable[i])
            return True,ans

    if '<=' in query:
        query = query.split('<=')
        try:
            var = int(query[-1])
            if query[0] not in finalRow:
                print "Column : " + col + " doesn't exist"
                return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) <= var:
                    ans.append(finalTable[i])
            return True,ans
        except:
            for col in query:
                if col not in finalRow:
                    print "Column : " + col + " doesn't exist"
                    return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) <= int(finalTable[i][finalRow.index(query[1])]):
                    ans.append(finalTable[i])
            return True,ans

    if '!=' in query:
        query = query.split('!=')
        try:
            var = int(query[-1])
            if query[0] not in finalRow:
                print "Column : " + col + " doesn't exist"
                return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) != var:
                    ans.append(finalTable[i])
            return True,ans
        except:
            for col in query:
                if col not in finalRow:
                    print "Column : " + col + " doesn't exist"
                    return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) != int(finalTable[i][finalRow.index(query[1])]):
                    ans.append(finalTable[i])
            return True,ans

    if '=' in query:
        query = query.split('=')
        try:
            var = int(query[-1])
            if query[0] not in finalRow:
                print "Column : " + col + " doesn't exist"
                return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) == var:
                    ans.append(finalTable[i])
            return True,ans
        except:
            for col in query:
                if col not in finalRow:
                    print "Column : " + col + " doesn't exist"
                    return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) == int(finalTable[i][finalRow.index(query[1])]):
                    ans.append(finalTable[i])
            return True,ans

    if '<' in query:
        query = query.split('<')
        try:
            var = int(query[-1])
            if query[0] not in finalRow:
                print "Column : " + col + " doesn't exist"
                return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) < var:
                    ans.append(finalTable[i])
            return True,ans
        except:
            for col in query:
                if col not in finalRow:
                    print "Column : " + col + " doesn't exist"
                    return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) < int(finalTable[i][finalRow.index(query[1])]):
                    ans.append(finalTable[i])
            return True,ans

    if '>' in query:
        query = query.split('>')
        try:
            var = int(query[-1])
            if query[0] not in finalRow:
                print "Column : " + col + " doesn't exist"
                return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) > var:
                    ans.append(finalTable[i])
            return True,ans
        except:
            for col in query:
                if col not in finalRow:
                    print "Column : " + col + " doesn't exist"
                    return False,ans
            for i in range(len(finalTable)):
                if int(finalTable[i][finalRow.index(query[0])]) > int(finalTable[i][finalRow.index(query[1])]):
                    ans.append(finalTable[i])
            return True,ans

def joinTable(table1,table2,row2,row1):
    final = []
    for i in table1:
        for j in table2:
            final.append(i+j)
    finalRow = row2 + row1
    return final,finalRow

def validate(query):
    if query[0]!='select' and query[0]!='SELECT':
        return False
    if query[2]!='from' and query[2]!='FROM':
        return False
    if len(query)>4 and query[4]!='where' and query[4]!='WHERE':
        return False
    return True

def databaseInfo(fileName):
    tableMetaData = defaultdict(list)
    try:
        with open(fileName,'r') as metaFile:
            metaFile = metaFile.readlines()

        for i in xrange(len(metaFile)):
            TableName = ''
            columnName = []
            if '<begin_table>' in metaFile[i]:
                i += 1
                if '<end_table>' not in metaFile[i]:
                    tableName = metaFile[i].split('\r')[0]
                    i += 1
                while '<end_table>' not in metaFile[i] :
                    columnName.append(tableName + '.' + metaFile[i].split('\r')[0])
                    i+=1
                if tableName:
                    tableMetaData[tableName] = columnName
    except:
        print "Something is Wrong"
        pass
    return tableMetaData

def table(filename,metaData):
    tableDict = []
    try :
        with open(DIR_PATH + filename + '.csv') as table:
            tableData = table.readlines()
        if len(metaData) != len(tableData[0].split(',')):
            print "update Meta data dictionary"
        else :
            row = []
            # print(len(tableData))
            for i in xrange(len(tableData)):
                tableDict.append(tableData[i].split(','))
                temp = tableDict[i][-1]
                tableDict[i][-1] = temp[:len(temp)-1]

    except:
        print "Something Wrong"
        pass
    # print (tableDict)
    return tableDict

if __name__ == "__main__":
    print "Welcome To VidSQL _/\_"

    META = databaseInfo('files/metadata.txt')
    TABLE_DATA = defaultdict()
    for name in META:
        TABLE_DATA[name] = table(name,META[name])
    print TABLE_DATA
    while(True):
        try:
            command = raw_input(">> ").strip()
            command  = sys.argv[1]
            if command.lower() == 'exit' or command.lower() == 'exit;':
                sys.exit();
            while command[-1]!=';':
                command += ' ' + raw_input(" ...").strip()
            query = command[:-1].strip()

            processQuery(query)

        except KeyboardInterrupt:
            sys.exit()
        except:
            pass
