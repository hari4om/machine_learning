'''
Created on Oct 14, 2016

@author: PromodG
'''

import configparser
import os
from bs4 import BeautifulSoup
import requests
import re
import time
from natsort import natsorted, ns
import pysolr
import random


class AllLearning:
    def solRIndexingPurchaseHistory(self):
        print("######solRIndexingPurchaseHistory()############")
        start_time = time.time()
        # Setup a basic Solr instance. The timeout is optional.
        solr = pysolr.Solr('http://172.25.1.156:8983/solr/purchaseHistoryCollection', timeout=10)

        for i in range(1, 4001):  # no. of customers + 1
            print("solRIndexingPurchaseHistory customer " + str(i))
            data = {}
            data['id'] = 'cust' + str(i)
            for j in range(1, 11):  # no. of products + 1
                data['product' + str(j)] = str(random.getrandbits(1))
            # print(data)
            solr.add([data], commit=True)
            solr.commit()

        solr.optimize()
        print("--- %s seconds ---" % (time.time() - start_time))
        print("######solRIndexingPurchaseHistory()-DONE!############")

    def solRIndexingRulePoolCollection(self):
        print("######solRIndexingRulePoolCollection()############")
        start_time = time.time()
        # Setup a basic Solr instance. The timeout is optional.
        solr = pysolr.Solr('http://172.25.1.156:8983/solr/rulePoolCollection', timeout=10)
        # solr = pysolr.Solr('http://localhost:8983/solr/rulePoolCollection', timeout=10)

        dataRuleCondition = {}
        dataRuleResult = {}

        for i in range(1, 21):  # no. of rules + 1
            print("solRIndexingRulePoolCollection rule " + str(i))
            #             dataRuleCondition = {}
            #             dataRuleResult = {}

            dataRuleCondition['id'] = 'rCondition' + str(i)
            dataRuleResult['id'] = 'rResult' + str(i)
            for j in range(1, 11):  # no. of products + 1
                randomVariable = random.getrandbits(1)
                dataRuleCondition['product' + str(j)] = str(randomVariable)
                if randomVariable == 0:  # and j < 7:
                    dataRuleResult['product' + str(j)] = str(1)
                elif randomVariable == 1:  # and j < 7:
                    dataRuleResult['product' + str(j)] = str(0)
                    # elif j >= 7:
                    # dataRuleResult['product'+str(j)] = str(0)
            # print(data)
            solr.add([dataRuleCondition])
            solr.add([dataRuleResult])

        # solr.commit()
        # solr.optimize()
        print("--- %s seconds ---" % (time.time() - start_time))
        print("######solRIndexingRulePoolCollection()-DONE!############")

    def readConfig(self, section, key):
        print("######Reading configuration file############")
        settings = configparser.ConfigParser()
        # Open the file with the correct encoding
        settingFileRead = settings.read('ApplicationSettings.ini')

        if (settingFileRead):
            settingsValue = settings.get(section, key)
            return settingsValue

        else:
            print("ApplicationSettings.ini file not found!")

    def iciciCorporate(self):
        # Open a file
        path = "E:\\MyShared\\downloads\\ICICI_Emails\\RETAIL\\"

        print("######iciciCorporate()############")
        dirs = os.listdir(path)

        for file in dirs:
            AllLearning.readInputFile(classInstance, path + file)

    def readInputFile(self, filename):
        with open(filename) as content_file:
            content = content_file.read()

            AllLearning.processHTML(self, content)

    def processHTML(self, content):
        soup = BeautifulSoup(content, "lxml")
        table = soup.find("table", attrs={"class": "MsoNormalTable"})
        if (table != None):
            for row in table.findAll("tr"):
                # if table.findParent("table") is None:
                # print(row.text)

                # cells = row.findAll("td")
                for l in row.findAll('td'):
                    if l.find('sup'):
                        l.find('sup').extract()

                    listSplitLine = l.getText().strip().split()
                    print(" ".join(listSplitLine))
                print("-------------------")
        else:
            print("No data for the class type MsoNormalTable")
            table = soup.find("table", attrs={"class": ""})
            for row in table.findAll("tr"):
                for l in row.findAll('td'):
                    if l.find('sup'):
                        l.find('sup').extract()

                    listSplitLine = l.getText().strip().split()
                    print(" ".join(listSplitLine))
                print("++++++++++++++++++++++")

    def restCallToSolR(self):
        utrList = ['SBIN116154982446', 'ICIC0006415', 'N154160157793320', 'SBIN116154982247', 'jkghkjghjkhgjk',
                   '7865875876587587']  # NEFTIn
        # utrList = ['CHASR52016090100310293', 'YESBR52016090100000026', '9879879070', 'BOFAR32016090100221987',
        #           'jkghkjghjkhgjk', 'BOFAR32016090100221974'] #RTGS

        strRtgsUTRList = "content9:"
        strNeftUTRList = "content3:"
        urlNeftInward = "http://localhost:8983/solr/neftinwardcollection/select?"
        urlNeftOutward = "http://localhost:8983/solr/neftoutwardcollection/select?"
        urlRtgsCommon = "http://localhost:8983/solr/rtgscommoncollection/select?"

        data1Rtgs = "indent=on&q="
        dataWtJson = "&wt=json&rows=100"

        data1Neft = "indent=on&q="

        for utr in range(len(utrList)):
            strRtgsUTRList = strRtgsUTRList + utrList[utr] + " OR content9:"

        for utr in range(len(utrList)):
            strNeftUTRList = strNeftUTRList + utrList[utr] + " OR content3:"

        dataRtgs = data1Rtgs + strRtgsUTRList[:-13] + dataWtJson
        dataNeft = data1Neft + strNeftUTRList[:-13] + dataWtJson

        rtgsFull_url = urlRtgsCommon + dataRtgs
        neftInwardFull_url = urlNeftInward + dataNeft
        neftOutwardFull_url = urlNeftOutward + dataNeft
        # print(rtgsFull_url)
        # print(neftInwardFull_url)
        # print(neftOutwardFull_url)

        response = requests.get(rtgsFull_url)
        if response.json()['response']['numFound'] != 0:
            AllLearning.writeJsonResponse(self, response.json(), "RTGS")
        else:
            response = requests.get(neftInwardFull_url)
            # print(response.json().response)
            if response.json()['response']['numFound'] != 0:
                AllLearning.writeJsonResponse(self, response.json(), "NEFTInward")
                # AllLearning.writeReportFile(self, response.json(), "NEFTInward")
            else:
                response = requests.get(neftOutwardFull_url)
                if response.json()['response']['numFound'] != 0:
                    AllLearning.writeJsonResponse(self, response.json(), "NEFTOutward")

    def writeReportFile(self, jsonObject, whichCollection):
        print("Calling writeReportFile()")
        # print(jsonObject)
        with open(r"" + "E:\\MyShared\\template\\" + "OUTPUT.txt", "w") as reportFile:
            for doc in jsonObject["response"]["docs"]:
                # print (natsorted(doc.keys(), alg=ns.IGNORECASE) )
                sortedKeys = natsorted(doc.keys(), alg=ns.IGNORECASE)
                strValues = ''
                for key in sortedKeys:
                    if key.find('content') != -1:
                        strValues = strValues + ''.join(doc[key]) + "|"
                # print("----------->"+strValues.replace('\n', ' ').replace('\r', ''))
                reportFile.writelines(whichCollection + "\n")
                reportFile.writelines(strValues.replace('\n', ' ').replace('\r', '') + "\n")

    def writeJsonResponse(self, jsonObject, whichCollection):
        templateFolder = 'E:\\MyShared\\template\\'
        template_file = ""
        signature = ""
        current_milli_time = lambda: int(round(time.time() * 1000))
        print("Data Found in " + whichCollection + " Collection!")

        if (whichCollection == 'NEFTInward'):
            signature = 'NEFT'
            template_file = r"" + templateFolder + "neftinboundTemplate.txt"
        elif (whichCollection == 'NEFTOutward'):
            signature = 'NEFT'
            template_file = r"" + templateFolder + "neftoutboundTemplete.txt"
        elif (whichCollection == 'RTGS'):
            signature = 'RTGS'
            template_file = r"" + templateFolder + "rtgsTemplate.txt"

        with open(template_file, 'r') as templateFile:
            with open(r"" + templateFolder + "OUTPUT.txt", "w") as reportFile:
                lines = templateFile.readlines()
                jsonData = jsonObject['response']['docs']
                for idx, item in enumerate(jsonData):
                    for line in lines:
                        if ('content' not in line and idx == 0):
                            reportFile.writelines(line)
                        elif ('content' in line):
                            strContent = re.compile('(content\\d+)').findall(line)
                            updatedLine = line.replace(strContent[0], (item.get(strContent[0]))[0])
                            reportFile.writelines(updatedLine)
                    reportFile.writelines("*********\n")
                reportFile.writelines("\n\nRegards,\n" + signature + " Team\nICICI")
                reportFile.writelines('\nssgblrzspk' + str(current_milli_time()))

        print("Done!")
        # print(jsonObject['response']['docs'])


classInstance = AllLearning()
classInstance.solRIndexingRulePoolCollection()

# returnedValue = classInstance.readConfig('Section-Common','trainingdatafile')
# print(returnedValue)
