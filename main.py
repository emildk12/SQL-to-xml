import pyodbc
import sys
import xml.etree.ElementTree as ET
import os
import sched, time
from tqdm import tqdm
from tkinter import filedialog
# må lage: rowversion.txt: 0 supplier.txt: [cobuilder_id]\n[company_name]\n[country_code] (feks NO)
#          db_connection_string.txt: [db_connection_string]

def createDateTimeString(dateTime):
    dateTimeString = dateTime.isoformat(timespec = "minutes")
    return dateTimeString[0:4] + dateTimeString[5:7] + dateTimeString[8:10] + dateTimeString[11:13] + dateTimeString[14:16]


def create_ele(name, text = None):
    ele = ET.Element(name)
    if text:
        ele.text = str(text)
    return ele


def create_xml_tree(response_lines, supplier):

    response = response_lines[0]

    if response.oReference == "":
        response.oReference = "-"

    xmlns = {"xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
             "xmlns:xsd":"http://www.w3.org/2001/XMLSchema",
             "xmlns":"urn:nordicebuilding:orders:1.2.2"}

    root = ET.Element("Order", attrib = xmlns)
    root.append(order_header := ET.Element("OrderHeader", attrib = {"xmlns":""}))

    order_header.append(create_ele("OrderNumber", response.OrderID))

    order_header.append(order_date_or_time := create_ele("OrderDateOrTime"))
    order_date_or_time.append(create_ele("OrderDateAndTime", createDateTimeString(response.OrderDate)))

    order_header.append(reference_to_document := create_ele("ReferenceToDocument"))
    reference_to_document.append(create_ele("ProjectNumber", "NOPROJECTNUMBER"))

    order_header.append(buyer_neb := create_ele("BuyerNeB"))
    buyer_neb.append(buyer_address_neb := create_ele("AddressNeB"))
    buyer_address_neb.append(create_ele("PartyIdentifier", response.cCompanyNo))
    buyer_address_neb.append(create_ele("PartyName", response.cName))
    buyer_address_neb.append(create_ele("StreetName", response.cMailingAddress))
    buyer_address_neb.append(create_ele("CityName", response.cMailingCity))
    buyer_address_neb.append(create_ele("PostalCodeNeB", response.cMailingZip))
    buyer_neb.append(buyers_contact_neb := create_ele("BuyersContactNeB"))
    buyers_contact_neb.append(create_ele("Name", "NULL"))
    buyers_contact_neb.append(create_ele("PhoneNumber", "NULL"))
    buyers_contact_neb.append(create_ele("EmailAdress", "NULL"))

    order_header.append(supplier_neb := create_ele("SupplierNeB"))
    supplier_neb.append(supplier_address_neb := create_ele("AddressNeB"))
    supplier_address_neb.append(create_ele("PartyIdentifier", supplier[0].strip()))
    supplier_address_neb.append(create_ele("PartyName", supplier[1].strip()))
    supplier_address_neb.append(create_ele("CountryCode", supplier[2].strip()))

    order_header.append(delivery_neb := create_ele("DeliveryNeB"))
    delivery_neb.append(create_ele("DeliveryPlaceLocation", response.oReference))
    delivery_neb.append(delivery_address_neb := create_ele("AddressNeB"))
    delivery_address_neb.append(create_ele("PartyIdentifier", response.cCompanyNo))
    delivery_address_neb.append(create_ele("PartyName", response.cName))
    delivery_address_neb.append(create_ele("StreetName", response.oReference))

    i = 0
    for line in response_lines:
        i += 1
        root.append(order_line := ET.Element("OrderLine", attrib = {"xmlns":""}))
        order_line.append(create_ele("LineNumber", i))
        order_line.append(article_identifiers := create_ele("ArticleIdentifiers"))
        if not line.aNobbNo == "":
            article_identifiers.append(create_ele("NOBBArticleNumber", line.aNobbNo))
        if not line.EanNo == "":
            article_identifiers.append(create_ele("EnumberNorwegian", line.EanNo))
        order_line.append(quantities := create_ele("Quantities"))
        quantities.append(ordered_quantity_neb := create_ele("OrderedQuantityNeB"))
        ordered_quantity_neb.append(create_ele("OrderedQuantity", line.olCount))

    root.append(order_trailer := ET.Element("OrderTrailer", attrib = {"xmlns":""}))
    order_trailer.append(create_ele("ControlTotal-NumberOfLines", i))

    return root


def connect_to_database():

    try:
        with open("db_connection_string.txt") as file:
            db_connection_string = file.readline().strip()
    except:
        print("\n==========================ERROR===============================\n \
               Could not access db_connection_string.txt\n \
               Please create a file called db_connection_string.txt in the same directory as this program\n \
               The file should contain: [db_connection_string]")
        sys.exit(0)

    connection = pyodbc.connect(db_connection_string)
    return connection


def recurring(cursor, scheduling, supplier, output_filepath):

    filename_previous_largest_rowversion = "rowversion.txt"

    with open(filename_previous_largest_rowversion, "r") as file:
        rowversion_in = int(file.readline().strip())

    # ignore test, cash and cash register customer (CustomerID = 1, 6, 11) NB!!!!! remove TOP 2
    cursor.execute(f"SELECT TOP 2 CONVERT (INT, RowVersion) \
                     FROM Orders \
                     WHERE RowVersion > ? AND CustomerID <> 1 \
                     AND CustomerID <> 6 AND CustomerID <> 11 \
                     ORDER BY RowVersion", rowversion_in)

    rowversion_list = cursor.fetchall()
    largest_rowversion = rowversion_list[-1][0]

    order_id_processed_list = []
    for rowversion in tqdm(rowversion_list):
        rowversion = rowversion[0]
        cursor.execute(f'SELECT \
                            o.OrderID AS OrderID, \
                            o.OrderDate AS OrderDate, \
                            o.Reference AS oReference, \
                            c.Name AS cName, \
                            c.CompanyNo AS cCompanyNo, \
                            c.MailingAddress AS cMailingAddress, \
                            c.MailingCity AS cMailingCity, \
                            c.MailingZip AS cMailingZip, \
                            a.NobbNo AS aNobbNo, \
                            EanNo, \
                            ol.Count AS olCount \
                         FROM Orders AS o \
                         JOIN Customers AS c ON o.CustomerID = c.CustomerID \
                         JOIN OrderLines AS ol ON o.OrderID = ol.OrderID \
                         JOIN Articles AS a ON ol.ArticleID = a.ArticleID \
                         LEFT JOIN EanNos ON a.ArticleID = EanNos.ArticleID \
                         WHERE o.RowVersion = ?', rowversion)

        response = cursor.fetchall()
        root = create_xml_tree(response, supplier)
        order_id_processed_list.append(response[0].OrderID)
        # NB hvordan filene lagres
        try:
            ET.ElementTree(root).write(output_filepath + "/order_id_" + str(response[0].OrderID) + ".xml")#in
        except:
            print("\n==========================ERROR===============================\n \
                   Could not write to output directory\n")
            sys.exit(0)
    # with open(filename_previous_largest_rowversion, "w") as file:#in
    #     file.write(f"{str(largest_rowversion)}")#in

    print()
    print(f"Orders processed: {len(order_id_processed_list)}")
    for order_id in order_id_processed_list:
        print(f"OrderID: {order_id}")

    scheduling.enter(5, 1, recurring, argument=(cursor, scheduling, supplier, output_filepath)) # NB!!!!!!!!!!!! 5 -> 1800
    scheduling.run()


def main():
    connection = connect_to_database()
    cursor = connection.cursor()
    scheduling = sched.scheduler(time.time, time.sleep)

    output_filepath = None
    try:
        with open("supplier.txt") as file:
            supplier = file.readlines()
    except:
        print("\n==========================ERROR===============================\n \
               Could not access supplier.txt\n \
               Please create a file called supplier.txt in the same directory as this program\n \
               The file should contain: [cobuilderId] \
                                        [company name] \
                                        [country code] (ex: NO)")
        sys.exit(0)
    try:
        with open("output_filepath.txt", "r") as file:
            output_filepath = file.readline().strip()
    except:
        output_filepath = filedialog.askdirectory()
        if(not output_filepath):
            print("No output filepath chosen, please restart the program and select a valid filepath")
            sys.exit(0)
        with open("output_filepath.txt", "w") as file:
            file.write(output_filepath)

    recurring(cursor, scheduling, supplier, output_filepath)


if __name__ == "__main__":
    main()
