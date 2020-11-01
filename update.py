import requests
from bs4 import BeautifulSoup

from bs4 import BeautifulSoup
from urllib.request import urlopen
from lxml.html import fromstring
import re
import csv
import pandas as pd
import json
import pymysql.cursors
import re




def get_list(url_param):

    url = url_param

    s = requests.session()
    response = s.get(url)
    data = BeautifulSoup(response.content, 'html.parser')


    heading = ''
    data_heading = data.find('h1',{'id':'firstHeading'})
    if data_heading:
        heading = data_heading.text.strip()

    links = data.find_all('table', {'class':'wikitable'})

    arr = []

    flag = -1

    level = 0

    oneTable = []

    final = []

    for link in links:

        trs = link.find_all('tr')

        data = []

        try:

            result = pd.read_html(str(link))



            df = result[0]





            k = 0
            while k < len(df.columns):


                if type(df.columns[k]) is not tuple:
                    data.append(str(df.columns[k]))
                    final.append(str(df.columns[k]))
                else:


                    b = set()
                    result = [element for element in df.columns[k]
                              if not (tuple(element) in b
                                      or b.add(tuple(element)))]


                    keyis = ''

                    for r in result:
                        keyis = keyis + r + ' '


                    data.append(str(keyis))
                    final.append(str(keyis))
                k = k+1


            if flag == -1:
                primary_key = data[0]
                flag = 1




            for tr in trs:
                if 'td' in str(tr) and 'Legend' not in str(tr):

                    datat = tr.text.strip().split('\n')

                    temp = {}
                    j = 0
                    k = 0


                    for d in data:
                        if k in range(len(datat)):

                            if '[' in datat[k] and ']' in datat[k]:
                                temp_data = datat[k]
                                temp_val = datat[k].split('[')
                                if len(temp_val) >= 2:
                                    temp_tr  = tr.find('a', text='[' + temp_val[1])
                                    if temp_tr:

                                        temp_url = url + temp_tr['href']


                                        temp_data = str('<a href="' +  temp_url + '">' + temp_val[0] + '</a>')


                                temp[data[j]] = temp_data


                            else:
                                temp[data[j]] = datat[k]
                        else:
                            temp[data[j]] = ''
                        j=j+1
                        k = k+2


                    if level == 0:
                        oneTable.append(temp)
                    elif level == 1:
                        for index in oneTable:
                            try:
                                if index[primary_key] == temp[primary_key]:
                                    index.update(temp)
                            except:
                                break



            level =  1
        except:
            break




    try:
        connection = pymysql.connect(host="localhost",
                                     user="yash_wikitable",
                                     passwd="moaAuoV2Ca",
                                     db="yash_wikitable",
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO wiki_url (url, title) VALUES (%s,%s)",
                    (url, heading))
                connection.commit()
                print("Inserted")
        except Exception as e:
            print('Failed Query')
            print(e)
        finally:
            connection.close()
    except pymysql.Error as e:
        print("Connection refused by the server..")



    url_id = -1

    try:
        connection = pymysql.connect(host="localhost",
                                     user="yash_wikitable",
                                     passwd="moaAuoV2Ca",
                                     db="yash_wikitable",
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT * FROM wiki_url WHERE url = %s"
                adr = (str(url))

                cursor.execute(sql, adr)

                myresult = cursor.fetchall()

                for x in myresult:
                    print("dat" + str(x))
                    url_id = x['id']
                    break

                connection.commit()

        except Exception as e:
            print('Failed Query')
            print(e)
        finally:
            connection.close()
    except pymysql.Error as e:
        print("Connection refused by the server..")




    for d in oneTable:
        for key in final:
            if key not in d.keys():
                d[key] = ''




    if url_id != -1:
        order = 1
        for d in oneTable:
            attribute_order = 1
            print(attribute_order)
            for key in d.keys():
                db_key = key
                db_value = d[key]

                try:
                    connection = pymysql.connect(host="localhost",
                                             user="yash_wikitable",
                                             passwd="moaAuoV2Ca",
                                             db="yash_wikitable",
                                             charset='utf8mb4',
                                             cursorclass=pymysql.cursors.DictCursor)
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(
                            "INSERT INTO wiki_tables (url, table_key, table_value, table_order, attribute_order) VALUES (%s,%s,%s, %s, %s)",
                            (url_id, str(db_key), str(db_value), order, attribute_order))
                            connection.commit()
                            print("Inserted")
                    except Exception as e:
                        print('Failed Query')
                        print(e)
                    finally:
                        connection.close()
                except pymysql.Error as e:
                    print("Connection refused by the server..")

                attribute_order = attribute_order + 1
                print(attribute_order)

            order = order + 1




# arr.append(oneTable)




if __name__ == '__main__':
    input_var = input("Enter URL: ")
    print("you entered " + input_var)
    get_list(input_var)
