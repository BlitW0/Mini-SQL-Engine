import sqlparse
from collections import defaultdict
import os
import re
import csv

DIR_PATH = 'files/'
META = defaultdict()
TABLE_DATA = defaultdict()


def processQuery(query):
    query = query.split()
    if validate(query):
        tableName = query[3].split(',')
        if len(tableName)>0:
            finalTable = TABLE_DATA[tableName[0]]
            finalRow = META[tableName[0]]
            for i in range(1,len(tableName)):
                finalTable,finalRow = joinTable(finalTable,TABLE_DATA[tableName[i]],finalRow,META[tableName[i]])

        #WHERE CODE
        
    else:
        return -1

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
    if query[4]!='where' and query[4]!='WHERE':
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
                    columnName.append(metaFile[i].split('\r')[0])
                    i+=1
                if tableName:
                    tableMetaData[tableName] = columnName
    except:
        print "Something is Wrong"
        pass
    return tableMetaData

def createTableArray(filename,metaData):
    tableDict = defaultdict(list)
    try :
        with open(DIR_PATH + filename + '.csv') as table:
            tableData = table.readlines()
        if len(metaData) != len(tableData[0].split(',')):
            print "update Meta data dictionary"
        else :
            row = []
            for i in xrange(len(tableData)):
                row.append(tableData[i].split(','))
            for i in range(len(row[0])):
                temp = []
                for r in row:
                    temp.append(r[i].split('\r')[0])
                #print temp
                tableDict[metaData[i]] = temp
    except:
        print "Something Wrong"
        pass
    return tableDict

def table(filename,metaData):
    tableDict = []
    try :
        with open(DIR_PATH + filename + '.csv') as table:
            tableData = table.readlines()
        if len(metaData) != len(tableData[0].split(',')):
            print "update Meta data dictionary"
        else :
            row = []
            for i in xrange(len(tableData)):
                tableDict.append(tableData[i].split(','))
                temp = tableDict[i][-1]
                tableDict[i][-1] = temp[:len(temp)-2]

    except:
        print "Something Wrong"
        pass
    return tableDict

if __name__ == "__main__":
    print "Welcome To VidSQL _/\_"

    META = databaseInfo('files/metadata.txt')
    #print META
    TABLE_DATA = defaultdict()
    for name in META:
        TABLE_DATA[name] = table(name,META[name])
    #print TABLE_DATA
    while(True):
        try:
            command = raw_input(">> ").strip()
            if command.lower() == 'exit' or command.lower() == 'exit;':
                break
            while command[-1]!=';':
                command += ' ' + raw_input(" ...").strip()
            query = command[:-1].strip()
            #query = sqlparse.format(query, reindent=True, keyword_case='upper')

            processQuery(query)

        except KeyboardInterrupt:
            break
        except:
            pass

# Meta = databaseInfo('files/metadata.txt')
# for word in Meta:
#     temp = createTableArray(word,Meta[word])
#     print word
#     print temp
