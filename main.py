import requests
import time
import json
from datetime import datetime, timedelta
from datetime import date
from google.cloud import bigquery
import configparser
import time
from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()


class NrOrgData:
    # config = configparser.ConfigParser()
    # config.read_file(open(r'app.config'))
    # X_CAP_API_AUTH_KEY = config["Default"]["X-CAP-API-AUTH-KEY"]
    # Authorization = config["Default"]["Authorization"]
    newrelic_key = os.getenv("NEWRELIC_API_KEY")

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")


    url = "https://api.newrelic.com/graphql"
    masterquery = "{actor{account(id: 67421){nrql(query: \"SELECT COUNT(*) AS count, percentile(duration,95) * 1000 as P95, percentile(duration,90) * 1000 as P90, filter(count(*), WHERE httpResponseCode = 500) AS p500, filter(count(*), WHERE httpResponseCode = 520) AS p520," \
                  " filter(count(*), WHERE httpResponseCode = 521) AS p521, filter(count(*), WHERE httpResponseCode = 400) AS p400 from Transaction WHERE  host NOT LIKE '%internal%' <ORGFILTER> AND org_id is not null and appName = 'appname' since 'sincedate 00:00:00+0530'  UNTIL 'untildate 00:00:00+0530' " \
                  "FACET  org_id,name,appName LIMIT  MAX\") {results}}}}"
    appnames = [
        {"key": "China", "value": "ningxia-crm-intouch-api"},
        {"key": "India", "value": "incrm-intouch-api"},
        {"key": "Asia", "value": "asiacrm-intouch-api"},
        {"key": "Tata", "value": "tatacrm-intouch-api"},
        {"key": "EU", "value": "eucrm-intouch-api"},
        {"key": "US", "value": "uscrm-intouch-api"},
        {"key": "India", "value": "incrm-intouch-api-v3"},
        {"key": "Asia", "value": "asiacrm-intouch-api-v3"},
        {"key": "Tata", "value": "tatacrm-intouch-api-v3"},
        {"key": "EU", "value": "eucrm-intouch-api-v3"},
        {"key": "US", "value": "uscrm-intouch-api-v3"}
    ]
    # payload = "client_id=1000.HTWUERSF9IRJFH7YLWWRCOQM35KDBH&client_secret=0160fa62e5175bea62ecb8cbe6b5a4792586678b6d&refresh_token=1000.baaacba7c2e27ca3bf0ce68082160f80.1fc6866e4138a1b5ceb390921e81937b&grant_type=refresh_token"
    headers = {
        'Accept': "application/json",
        'API-Key': newrelic_key
    }
    # fromdate = "2025-03-21"
    # toDate = "2025-03-22"
    # metricdate = "2025-03-21"

    today = date.today()
    d1 = today.strftime("%Y-%m-%d")
    fromdate = (datetime.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
    metricdate = (datetime.today() + timedelta(days=-1)).strftime("%m/%d/%Y")

    # fromdate = '2024-07-10'
    toDate = d1
    # toDate = '2024-07-11'
    metricdate = fromdate
    client = bigquery.Client()
    table_id = "pmodatabase-398513.newreliccrondata.crmnrdata_copy"
    table_id1 = "pmodatabase-398513.newreliccrondata.nrdatafullv2_copy"
    table_id2 = "pmodatabase-398513.newreliccrondata.crmorgdata_copy"

    i = 0
    j = 1
    while i < len(appnames):
        totalData = []
        query = masterquery
        query = query.replace("appname", appnames[i]["value"])
        query = query.replace("sincedate", fromdate)
        query = query.replace("untildate", toDate)
        if appnames[i]["value"] == "incrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"] == "asiacrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            print(responedata)
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"].__contains__("v3"):
            tempquery = query
            query = query.replace("<ORGFILTER>", " ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            # query = tempquery
            # query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            # respone = requests.request("POST", url, data=query, headers=headers)
            # responedata = respone.json()
            # totalData.append(responedata)
        else:
            query = query.replace("<ORGFILTER>", "")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)

        rows_to_insert = []
        for data in totalData:
            responedata = data
            try:
                orgdata = responedata["data"]["actor"]["account"]["nrql"]["results"]
            except Exception as ex:
                orgdata = []
            print("Total Records for {} is :{}".format(appnames[i], len(orgdata)))
            for data in orgdata:
                facet = data["facet"]
                if facet[0] != "null":
                    orgId = int(facet[0])
                    apiname = facet[1]
                    appname = facet[2]
                    count = int(data["count"])
                    p95 = round(float(data["P95"]), 2)
                    p90 = round(float(data["P90"]), 2)
                    p400 = int(data["p400"])
                    p500 = int(data["p500"])
                    p520 = int(data["p520"])
                    p521 = int(data["p521"])
                    jsonobj = {
                        "orgid": orgId,
                        "apiname": apiname,
                        "appname": appname,
                        "count": count,
                        "p95": p95,
                        "p90": p90,
                        "500": p500,
                        "520": p520,
                        "521": p521,
                        "400": p400,
                        "metricdate": metricdate
                    }
                    rows_to_insert.append(jsonobj)
                    jsonobj = {}

            if (rows_to_insert != []):
                errors = client.insert_rows_json(table_id, rows_to_insert)
                if errors == []:
                    print("Record inserted successfully into crmnrdata table")
                    rows_to_insert = []
                else:
                    print("Encountered errors while inserting rows: {}".format(errors))

        i = i + 1

    masterquery = "{actor{account(id: 67421){nrql(query: \"SELECT COUNT(*) AS count, percentile(duration,95) * 1000 as P95, average(duration * 1000) as P90, filter(count(*), WHERE http.statusCode = 500) AS p500, filter(count(*), WHERE http.statusCode = 520) AS p520, filter(count(*), WHERE http.statusCode = 521) AS p521, filter(count(*), WHERE http.statusCode = 400) AS p400 from Transaction  where appName = 'appname' and name in ('WebTransaction/Custom/newBillEvent', 'WebTransaction/Custom/registrationEvent', 'WebTransaction/Custom/pointsRedemptionEvent', 'WebTransaction/Custom/customerUpdateEvent', 'WebTransaction/Custom/getCustomerPointsSummariesByFilter', 'WebTransaction/Custom/returnBillAmountEvent') AND orgId NOT IN (150603, 150969, 150616, 150606, 150602, 150713, 150151, 150595, 100458, 2000, 1800) and appName NOT LIKE '%staging%' and appName NOT LIKE '%nightly%' AND appName NOT LIKE '%devenv%' since yesterday FACET orgId,name,appName LIMIT  MAX\") {results}}}}"

    appnames = [

        {"key": "India", "value": "incrm-emf"},
        {"key": "Asia", "value": "asiacrm-emf"},
        {"key": "Tata", "value": "tatacrm-emf"},
        {"key": "EU", "value": "eucrm-emf"},
        {"key": "US", "value": "uscrm-emf"},
        {"key": "Tata", "value": "tatacrm-emf"}
    ]
    i = 0
    j = 1
    while i < len(appnames):
        totalData = []
        query = masterquery
        query = query.replace("appname", appnames[i]["value"])
        query = query.replace("sincedate", fromdate)
        query = query.replace("untildate", toDate)
        if appnames[i]["value"] == "incrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"] == "asiacrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"].__contains__("v3"):
            tempquery = query
            query = query.replace("<ORGFILTER>", " ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            # query = tempquery
            # query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            # respone = requests.request("POST", url, data=query, headers=headers)
            # responedata = respone.json()
            # totalData.append(responedata)
        else:
            query = query.replace("<ORGFILTER>", "")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)

        rows_to_insert = []
        for data in totalData:
            responedata = data
            try:
                orgdata = responedata["data"]["actor"]["account"]["nrql"]["results"]
            except Exception as ex:
                orgdata = []
            print("Total Records for {} is :{}".format(appnames[i], len(orgdata)))
            for data in orgdata:
                facet = data["facet"]
                if facet[0] != "null":
                    orgId = int(facet[0])
                    apiname = facet[1]
                    appname = facet[2]
                    count = int(data["count"])
                    p95 = round(float(data["P95"]), 2)
                    p90 = round(float(data["P90"]), 2)
                    p400 = int(data["p400"])
                    p500 = int(data["p500"])
                    p520 = int(data["p520"])
                    p521 = int(data["p521"])
                    jsonobj = {
                        "orgid": orgId,
                        "apiname": apiname,
                        "appname": appname,
                        "count": count,
                        "p95": p95,
                        "p90": p90,
                        "500": p500,
                        "520": p520,
                        "521": p521,
                        "400": p400,
                        "metricdate": metricdate
                    }
                    rows_to_insert.append(jsonobj)
                    jsonobj = {}

            if (rows_to_insert != []):
                errors = client.insert_rows_json(table_id, rows_to_insert)
                if errors == []:
                    print("Record inserted successfully into crmnrdata table")
                    rows_to_insert = []
                else:
                    print("Encountered errors while inserting rows: {}".format(errors))

        i = i + 1

    masterquery = "{actor{account(id: 67421){nrql(query: \"SELECT COUNT(*) AS count, percentile(duration,95) * 1000 as P95, percentile(totalTimeTaken,95) / 1000 as P90, filter(count(*), WHERE http.statusCode = 500) AS p500, filter(count(*), WHERE http.statusCode = 520) AS p520, filter(count(*), WHERE http.statusCode = 521) AS p521, filter(count(*), WHERE http.statusCode = 400) AS p400 from Transaction WHERE  host NOT LIKE '%internal%' AND replayCount >= 30  AND orgId is not null AND appName='appname' AND appName not in ('crm-nightly-new-intouch-api', 'crm-staging-new-intouch-api', 'devenv-crm-intouch-api') since yesterday  FACET orgId,name,appName LIMIT  MAX \") {results}}}}"

    appnames = [
        {"key": "India", "value": "incrm-intouch-api"},
        {"key": "Asia", "value": "asiacrm-intouch-api"},
        {"key": "Tata", "value": "tatacrm-intouch-api"},
        {"key": "EU", "value": "eucrm-intouch-api"},
        {"key": "US", "value": "uscrm-intouch-api"}
    ]

    i = 0
    j = 1
    while i < len(appnames):
        totalData = []
        query = masterquery
        query = query.replace("appname", appnames[i]["value"])
        query = query.replace("sincedate", fromdate)
        query = query.replace("untildate", toDate)
        if appnames[i]["value"] == "incrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"] == "asiacrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"].__contains__("v3"):
            tempquery = query
            query = query.replace("<ORGFILTER>", " ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            # query = tempquery
            # query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            # respone = requests.request("POST", url, data=query, headers=headers)
            # responedata = respone.json()
            # totalData.append(responedata)
        else:
            query = query.replace("<ORGFILTER>", "")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)

        rows_to_insert = []
        for data in totalData:
            responedata = data
            try:
                orgdata = responedata["data"]["actor"]["account"]["nrql"]["results"]
            except Exception as ex:
                orgdata = []
            print("Total Records for {} is :{}".format(appnames[i], len(orgdata)))
            for data in orgdata:
                facet = data["facet"]
                if facet[0] != "null":
                    orgId = int(facet[0])
                    apiname = facet[1]
                    appname = facet[2]
                    count = int(data["count"])
                    p95 = round(float(data["P95"]), 2)
                    p90 = round(float(data["P90"]), 2)
                    p400 = int(data["p400"])
                    p500 = int(data["p500"])
                    p520 = int(data["p520"])
                    p521 = int(data["p521"])
                    jsonobj = {
                        "orgid": orgId,
                        "apiname": apiname,
                        "appname": appname,
                        "count": count,
                        "p95": p95,
                        "p90": p90,
                        "500": p500,
                        "520": p520,
                        "521": p521,
                        "400": p400,
                        "metricdate": metricdate
                    }
                    rows_to_insert.append(jsonobj)
                    jsonobj = {}

            if (rows_to_insert != []):
                errors = client.insert_rows_json(table_id, rows_to_insert)
                if errors == []:
                    print("Record inserted successfully into crmnrdata table")
                    rows_to_insert = []
                else:
                    print("Encountered errors while inserting rows: {}".format(errors))

        i = i + 1

    masterquery = "{actor{account(id: 67421){nrql(query: \"SELECT COUNT(*) AS count, percentile(duration,95) * 1000 as P95, percentile(duration,90) * 1000 as P90, filter(count(*), WHERE http.statusCode = 500) AS p500, filter(count(*), WHERE http.statusCode = 520) AS p520, filter(count(*), WHERE http.statusCode = 521) AS p521, filter(count(*), WHERE http.statusCode = 400) AS p400  from Transaction where appName='appname' and org_id in (151181,151229,151197,151195)  since yesterday FACET  org_id,name,appName LIMIT  MAX\") {results}}}}"
    appnames = [
        {"key": "Asia", "value": "asiacrm-promotion-engine"}
    ]
    i = 0
    j = 1
    while i < len(appnames):
        totalData = []
        query = masterquery
        query = query.replace("appname", appnames[i]["value"])
        query = query.replace("sincedate", fromdate)
        query = query.replace("untildate", toDate)
        if appnames[i]["value"] == "incrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 1801 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"] == "asiacrm-intouch-api":
            tempquery = query
            query = query.replace("<ORGFILTER>", "AND org_id <=150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            print(responedata)
            totalData.append(responedata)
            query = tempquery
            query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
        elif appnames[i]["value"].__contains__("v3"):
            tempquery = query
            query = query.replace("<ORGFILTER>", " ")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)
            # query = tempquery
            # query = query.replace("<ORGFILTER>", "AND org_id > 150900 ")
            # respone = requests.request("POST", url, data=query, headers=headers)
            # responedata = respone.json()
            # totalData.append(responedata)
        else:
            query = query.replace("<ORGFILTER>", "")
            respone = requests.request("POST", url, data=query, headers=headers)
            responedata = respone.json()
            totalData.append(responedata)

        rows_to_insert = []
        for data in totalData:
            responedata = data
            try:
                orgdata = responedata["data"]["actor"]["account"]["nrql"]["results"]
            except Exception as ex:
                orgdata = []
            print("Total Records for {} is :{}".format(appnames[i], len(orgdata)))
            for data in orgdata:
                facet = data["facet"]
                if facet[0] != "null":
                    orgId = int(facet[0])
                    apiname = facet[1]
                    appname = facet[2]
                    count = int(data["count"])
                    p95 = round(float(data["P95"]), 2)
                    p90 = round(float(data["P90"]), 2)
                    p400 = int(data["p400"])
                    p500 = int(data["p500"])
                    p520 = int(data["p520"])
                    p521 = int(data["p521"])
                    jsonobj = {
                        "orgid": orgId,
                        "apiname": apiname,
                        "appname": appname,
                        "count": count,
                        "p95": p95,
                        "p90": p90,
                        "500": p500,
                        "520": p520,
                        "521": p521,
                        "400": p400,
                        "metricdate": metricdate
                    }
                    rows_to_insert.append(jsonobj)
                    jsonobj = {}

            if (rows_to_insert != []):
                errors = client.insert_rows_json(table_id, rows_to_insert)
                if errors == []:
                    print("Record inserted successfully into crmnrdata table")
                    rows_to_insert = []
                else:
                    print("Encountered errors while inserting rows: {}".format(errors))

        i = i + 1

    delete_query = "delete from {} where 1=1".format(table_id1)
    job = client.query(delete_query)
    time.sleep(10)
    insert_query = """
                    INSERT INTO {} (
              SELECT nr.*, org.orgName, org.cluster, org.isActive, org.orgCategory, CAST(org.shardName AS INTEGER)
              FROM {} AS nr
              LEFT JOIN {} AS org
              ON nr.orgid = org.orgId
              AND org.cluster =
                CASE
                  WHEN nr.appname = 'tatacrm-intouch-api' THEN 'Tata'
                  WHEN nr.appname = 'eucrm-intouch-api' THEN 'EU'
                  WHEN nr.appname = 'sgcrm-intouch-api' THEN 'SG'
                  WHEN nr.appname = 'incrm-intouch-api' THEN 'India'
                  WHEN nr.appname = 'uscrm-intouch-api' THEN 'US'
                  WHEN nr.appname = 'asiacrm-intouch-api' THEN 'SG'
                END
            );
       """.format(table_id1, table_id, table_id2)
    job = client.query(insert_query)