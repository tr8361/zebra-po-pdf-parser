import azure.functions as func
import logging
import os
import tempfile
import io
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
import oracledb
from datetime import datetime,date
from typing import List
import pandas as pd
from azure.storage.blob import BlobServiceClient
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.prompts import ChatPromptTemplate,HumanMessagePromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel,Field
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
import re
import sys
import glob
import shutil
import googlemaps


load_dotenv(override=True)

class PurchaseOrder(BaseModel):
    po_number: str = Field(description="Purchase Order Number")
    ship_to: str = Field(description="Ship To Address")
    freight_acc_no: str = Field(description="Ship Via Method Number.")
    pc_no: str = Field(description="PC Number")
    part_numbers: List[str] = Field(description= "Item Numbers")




def document_load_and_parse(temp_pdf_path,prompt,client,parser):
    print("Document loading from Blob....")
    print("pdf path :", temp_pdf_path)
    loader = PyPDFLoader(temp_pdf_path)
    document = loader.load()
    print(document[0].page_content)

    document_query =  """Extract the values of PO Number, Ship To address, Ship Via method, PC Number and part Numbers.
    PC numbers will be mentioned as PC Number. Please note that some documents 
    might not have a PC Number mentioned, in that case assign value as 'PC number not Found'.
    Freight account number is mentioned inside the Ship Via method label/column.
    Please note that some documents might not have The Ship Via method mention, 
    in that  case assign value as Freight Method Not Found.
    PO Numbers can be mentioned as Order Numbers as well. 
    Do not consider Customer PO, only consider Purchase Order PO
    While extracting Part Numbers,if part number is mentioned after the term 'Zebra', consider ONLY that preceeding string to be the actual Part Number.
    Part numbers format examples: 
    SE2707-LU000R (Actual Part Number format)
    AFT-SYG-FGH (Actual Part Number format)
    Special cases to extract Part Numbers/Item number:
    For example, the actual part number is ABC-2HG3-IOX but the part of the string can be present as ABC-2HG3- in the first line and rest IOX in the immediate next line.
    In that case, consider the whole string as the part number without separating it into two part numbers. 
    """+ document[0].page_content

    prompt_format = prompt.format_prompt(question = document_query)
    result = client.invoke(prompt_format.to_messages())
    parsed = parser.parse(result.content)
    print("Parsing Completed....")
    print(parsed)
    return parsed      

def validate_parsed_values_with_database(username,password,dsn,parsed):
    
    print("Document Validation starts.......")
    global connection
    remarks = []
    correct_remarks = []
    global flag 
    flag = False
    try:        
        if not flag:
            #cx_Oracle.init_oracle_client(lib_dir= r"C:\\Oracle19c-64bit\\product\\client_1\\bin\\")
            #for Linux
            instant_client_dir = None

            if sys.platform.startswith("win"):
                instant_client_dir = r"C:\\Oracle19c-64bit\\product\\client_1\\bin\\"

            # This can be called at most once per process.
            if instant_client_dir is not None:
                oracledb.init_oracle_client(lib_dir=instant_client_dir)

            connection = oracledb.connect(user=username, password=password, dsn=dsn)
            print(f"\nConnected successfully!")
            flag = True

    except oracledb.DatabaseError as e:
        print(f"Error connecting to the database: {e}")


    cursor = connection.cursor()
    po_no_validate = f"SELECT X_ZEB_PURCHASE_ORDNUM FROM siebel.S_ORDER where X_ZEB_PURCHASE_ORDNUM= :value"
    freight_account_number_validate = "SELECT X_CUST_FREIGHT_ACCOUNT FROM siebel.S_ORDER WHERE X_CUST_FREIGHT_ACCOUNT= :value"
    expired_pc = "SELECT EFF_END_DT From siebel.s_doc_agree where AGREE_NUM = :value"
    part_number_validate = "SELECT NAME From siebel.S_PROD_INT where NAME=:part"
    
    
    for i in parsed.part_numbers:
        i.strip()
        i = i.replace(" ", "")
        cursor.execute(part_number_validate,part = i)
        if cursor.fetchone() is None:
            print(f"Part number {i} is not Valid")
            remarks.append(f"Part number {i} is not Valid")
        else:
            print(f"Part number {i} is Valid")
            correct_remarks.append(f"Part number {i} is Valid")


    comparePONumber = cursor.execute(po_no_validate,value = parsed.po_number)
    cursor_fetchone = cursor.fetchone()
    if cursor_fetchone is None:
        print(f"PO number: {parsed.po_number} is new & should be processed further")
        remarks.append(f"\nPO number {parsed.po_number} is new and should be processed further.")
    else:
        print(f"PO number {parsed.po_number} already exists.")
        correct_remarks.append(f"\nPO number {parsed.po_number} already exists.")

 #fac is needed when #COLLECT is mentioned
    if "BESTWAY" in parsed.freight_acc_no or "Prepay & Add" in parsed.freight_acc_no or "Prepay" in parsed.freight_acc_no: #PREPAY to be added 
        print(f"No Validation needed for Freight account number: {parsed.freight_acc_no}")
        correct_remarks.append("\nNo Validation needed for Freight account number")
    elif parsed.freight_acc_no == 'Freight Method Not Found':
        print(f"Freight Account Number not found")
        remarks.append("\nFreight Account Number not found") 
    # elif "COLLECT" in parsed.freight_acc_no:
    #     freight_method = re.findall("\D", parsed.freight_acc_no)#contains No Digits
    #     freight_account_number = re.findall("\d",  parsed.freight_acc_no)#contains only digits
    #     print(''.join(freight_method))
    #     print("Printing freight account number ........:"+''.join(freight_account_number))

    #     if  freight_method and not freight_account_number:
    #         remarks.append(f"Freight account number is missing for Freight method - {''.join(freight_method)}")
    #         print(f"Freight account number is missing for {''.join(freight_method)}")
    #     else:
    #         print(f"Freight Account Number is present")

    else:
        freight_method = re.findall("\D", parsed.freight_acc_no)#contains No Digits
        freight_account_number = re.findall("\d",  parsed.freight_acc_no)#contains only digits
        print(''.join(freight_method))
        print("Printing freight account number ........:"+''.join(freight_account_number))

        if  freight_method and not freight_account_number:
            remarks.append(f"Freight account number is missing for Freight method - {''.join(freight_method)}")
            print(f"\nFreight account number is missing for {''.join(freight_method)}")
        else:
            print(f"Freight Account Number is present")
            correct_remarks.append(f"\nFreight Account Number is present")

    comparePcNumber = cursor.execute(expired_pc,value = parsed.pc_no)
    cursor_fetchone_pc = cursor.fetchone()
    flag_pc = False
    try: 
        if datetime.now() > cursor_fetchone_pc[0]: 
            print(f"PC number {parsed.pc_no} is expired")
            remarks.append(f"\nPC number {parsed.pc_no} is expired")
        else:
            print(f"PC number {parsed.pc_no} is not expired")
            correct_remarks.append(f"\nPC number {parsed.pc_no} is not expired")
        
    except Exception as e:
        flag_pc = True
    
    if flag_pc==True:
        print(f"Pc Expired Date is not found in PO")
        correct_remarks.append(f"\nPc Expired Date is not found in PO\n")


    gmaps = googlemaps.Client(os.environ.get('google_maps_api_key'))
    address = parsed.ship_to
    result = gmaps.addressvalidation([address], regionCode='US', enableUspsCass=True)
    #print(result)
    validated_address = result['result']['address']

    if validated_address==address:
        #validated_address = result['result']['address']
        print("Address is Valid")
        correct_remarks.append("\nAddress is valid.")
        
    else:
        print(f"Recommended address: {validated_address['formattedAddress']}")
        remarks.append(f"\nRecommended address: {validated_address['formattedAddress']}")

    print(remarks)
    cursor.close()
    connection.close()
    print("DB Validation is done....")
    return remarks,correct_remarks


def create_excel_file(blob_service_client,container_name,upload_excel_blob_name,parsed,remarks,correct_remarks):
    print("Creating Excel File with the Validation Errors Info.....")
    dict_data1 = parsed.dict()
    print(dict_data1)
    
    combined_part_numbers=','.join(parsed.part_numbers)
    dict_data1['part_numbers']=combined_part_numbers
    print(f"dict_data1...updated,,,:{dict_data1}")
    combined_remarks = ','.join(remarks)
    combined_correct_remarks = ','.join(correct_remarks)
    blob_client = blob_service_client.get_blob_client(container_name,upload_excel_blob_name)
    print(f"blob client :{blob_client}")
    if blob_client.exists():
        # Download the blob content
        blob_content = blob_client.download_blob()
        print(f"The file {blob_content} already exists.")
        df = pd.read_excel(blob_content.content_as_bytes())
        print("df .....:", df)
        # new_row = dict_data1.copy()
        # new_row['Remarks'] = combined_remarks
        # new_row['Sl. No.'] = len(df) + 1

        dict_data1['Remarks'] = combined_remarks
        dict_data1['Correct_Remarks'] = combined_correct_remarks
        dict_data1['Sl. No.'] = len(df) + 1
        print("demo dict_data1 : ",dict_data1)
        df = df._append(dict_data1, ignore_index=True)
        print("new_row appended to df   :", df)

        # Save the modified data to a new Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
        xlsx_data = output.getvalue()
        #print(f"The DataFrame has been updated to {xlsx_data}. ")

    else:
        data1 = pd.DataFrame(dict_data1,index=[0])
        data1['Remarks'] = combined_remarks
        data1['Correct_Remarks'] = combined_correct_remarks
        data1.insert(0, 'Sl. No.', range(1, 1 + len(data1)))
        print("printing data1..... ")
        print(data1)
        #save the dataframe to excelfile
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            data1.to_excel(writer, sheet_name='Sheet1', index=False)

        xlsx_data = output.getvalue()
        #print(f"New DataFrame has been saved to {xlsx_data}.")
    
    return xlsx_data
        

def upload_excel_blob(blob_service_client,container_name, xlsx_data, upload_excel_blob_name):
    print("Uploading error Excel to the Blob.....")
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=upload_excel_blob_name)
        blob_client.upload_blob(xlsx_data, overwrite=True)
        if blob_client.exists:
            print(f"Uploaded {upload_excel_blob_name} successfully!")
        else:
            print(f"Error uploading {upload_excel_blob_name}")

    except Exception as e:
        print(f"Error uploading {upload_excel_blob_name}: {e}")

def send_alert_mail_using_sendgrid(API,upload_excel_blob_name,xlsx_data):

    message = Mail(
        from_email = os.environ.get("FROM"),
        to_emails = os.environ.get("TO"),
        subject = "ZEBRA OM GENAI PO Parser Alert Email",
        html_content = "Alert Mail with Excel Sheet is sent Successfully!" 
    )
    
    encoded_file = base64.b64encode(xlsx_data).decode()

    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName(upload_excel_blob_name),
        FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        Disposition('attachment')
    )
    message.attachment = attachedFile
    try:
        sg = SendGridAPIClient(api_key=API)
        print("sendgrid: email sent to user.")
        response = sg.send(message)
        print(f"Mail Response: {response}")
        print(f"Email sent! Status code: {response.status_code}")
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(f"Exception{e}")
    print("email sent status")

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="po-container/{name}.pdf",
                               connection="AzureWebJobsStorage") 
def BlobTrigger1(myblob: func.InputStream):
    try:
        # logging.info(f"Python blob trigger function processed blob Blob Name:", myblob.name)
        # print(f"Python blob trigger function processed blob Blob Name:", myblob.name)
        print("Python blob trigger function processed blob"
                    f"Blob Name: {myblob.name}"
                    f"Blob Size: {myblob.length} bytes")

        connection_string = os.environ.get("STORAGE_ACCOUNT_CONNECTION_STRING")
        container_name = os.environ.get("CONTAINER_NAME")
        blob_name = myblob.name.split("/")[1]
        storage_acc_name = os.environ.get("AZURE_STORAGE_ACCOUNT")
        storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
        username = os.environ.get("username")
        password = os.environ.get("password")
        upload_excel_blob_name = "status_excel_"+str(date.today())+".xlsx"
        sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
        dest_container_name = os.environ.get("DEST_CONTAINER_NAME")

        print("loading dsn for oracle db.....!")
        dsn = oracledb.makedsn(
            host=os.environ.get("host"),
            port=os.environ.get("port"),
            service_name = os.environ.get("service_name")
        )

        print("ChatPromptTemplate.....!")
        parser = PydanticOutputParser(pydantic_object = PurchaseOrder)
        prompt = ChatPromptTemplate(
        messages=[
            HumanMessagePromptTemplate.from_template("answer the user questions as best as possible.\n{format_instructions}\n{question}"
            )
        ],
        input_variables=["question"],
        partial_variables={
            "format_instructions": parser.get_format_instructions(),
        },
        )

        client = AzureChatOpenAI(
        model='gpt-4',
        azure_deployment="chat-endpoint",
        api_key = os.environ.get("AZURE_OPENAI_API_KEY"),
        api_version = os.environ.get("OPENAI_API_VERSION")
        )


        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        # Get a BlobClient for your blob
        blob_client = blob_service_client.get_blob_client(container_name,blob_name)

        # if blob_client.exists():
        #     blob_url = blob_client.url
        #     print("Blob URL: ", blob_url)
        # else:
        #     print("Blob does not exist.", blob_name)

        print("Creating a temp directory")
        temp_dir = tempfile.mkdtemp()
        # Iterate through each blob in the container
        processed_files = set() 
        for blob in container_client.list_blobs():
            print("blobs inside for ...."+blob.name)

            blob_client1 = container_client.get_blob_client(blob.name)
            blob_data = blob_client1.download_blob()
            # Construct a unique file path in the temp_dir using the blob's name
            file_name = os.path.join(temp_dir, blob.name)
            print(f":::::::::file_path ::::::::::::::::..",file_name)
            # Save the blob's content to the local file
            with open(file_name, "wb") as file:
               file.write(blob_data.readall())
            print(f"Downloaded blob: {blob.name} to {file_name}")

            print("All files downloaded and saved in temp_dir:", temp_dir)  
            # Iterate over PDF files in the specified directory
        for temp_pdf_path in glob.glob(os.path.join(temp_dir, "*")):
            print(f"processed files set : {processed_files}")
            if temp_pdf_path in processed_files:
                    continue
            else:
                if temp_pdf_path.endswith(".pdf"):
                    print("temp_pdf_path = "+temp_pdf_path)
                    print("Document Parsing starts......")
                    parsed_return_value  = document_load_and_parse(temp_pdf_path,prompt,client,parser)
                    remarks_tuple = validate_parsed_values_with_database(username,password,dsn,parsed_return_value)
                    print("remarks tuple :", remarks_tuple)
                    xlsx_data = create_excel_file(blob_service_client,container_name,upload_excel_blob_name,parsed_return_value,remarks_tuple[0],remarks_tuple[1])
                    print("uploading excel to blob container starts.....")
                    upload_excel_blob(blob_service_client, container_name, xlsx_data, upload_excel_blob_name)
                    print(os.path.basename(temp_pdf_path))
            if temp_pdf_path.endswith('.pdf'): 
                blob_to_move = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(temp_pdf_path))       
                # Get a Destination BlobClient for your blob
                destination_blob_client = blob_service_client.get_blob_client(container=dest_container_name,blob=os.path.basename(temp_pdf_path))
                destination_blob_client.start_copy_from_url(blob_to_move.url)
                blob_to_move.delete_blob()
                print(f"Blob '{blob_name}' moved from '{container_name}' to '{dest_container_name}'")
            processed_files.add(temp_pdf_path)
        print("sending mail alert ....")
        send_alert_mail_using_sendgrid(sendgrid_api_key,upload_excel_blob_name,xlsx_data)

        # count = 0
        # for temp_pdf_path in glob.glob(os.path.join(temp_dir, "*")):
        #     print("temp_pdf_path : ", temp_pdf_path)
        #     #if temp_pdf_path not in processed_files and temp_pdf_path.endswith(".pdf"):
        #     if temp_pdf_path.endswith(".pdf"):
        #         count=count+1
        #         print("Document Parsing starts......")
        #         parsed_return_value  = document_load_and_parse(temp_pdf_path,prompt,client)
        #         remarks_list = validate_parsed_values_with_database(username,password,dsn,parsed_return_value)
        #         xlsx_data = create_excel_file(blob_service_client,container_name,upload_excel_blob_name,parsed_return_value,remarks_list)
        #         print("uploading excel to blb container starts.....")
        #         upload_excel_blob(blob_service_client, container_name, xlsx_data, upload_excel_blob_name)
        #         #processed_files.add(temp_pdf_path)
        #         #os.remove(temp_pdf_path)
        # print("count of for loop is :", count)    
        # print("sending mail alert ....")
        # send_alert_mail_using_sendgrid(sendgrid_api_key,upload_excel_blob_name,xlsx_data)   
                
        # for temp_pdf_path in glob.glob(os.path.join(temp_dir, "*")): 
        #     if temp_pdf_path.endswith(".pdf"): 
        #         blob_to_move = container_client.get_blob_client(os.path.basename(temp_pdf_path))
        #         destination_blob_client.start_copy_from_url(blob_to_move.url)
        #         blob_to_move.delete_blob()
        #         print(f"Blob '{blob_name}' moved from '{container_name}' to '{dest_container_name}'")
        shutil.rmtree(temp_dir)  # Remove the temporary directory and its contents

    except Exception as e:
        print(f"Error in code as :{e}")
        
