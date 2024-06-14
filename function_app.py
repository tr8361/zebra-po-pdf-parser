import azure.functions as func
import logging
import os
import tempfile
import io
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
import config
import cx_Oracle
from datetime import datetime,date
from typing import List
import pandas as pd
import openpyxl
from azure.storage.blob import BlobServiceClient
from langchain_community.document_loaders import PyPDFLoader
import pypdf
from langchain_core.prompts import ChatPromptTemplate,HumanMessagePromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel,Field
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
import re

load_dotenv()

class PurchaseOrder(BaseModel):
    po_number: str = Field(description="Purchase Order Number")
    ship_to: str = Field(description="Shipping Address")
    freight_acc_no: str = Field(description="Ship Via Method Number.")
    pc_no: str = Field(description="PC Number")
    part_numbers: List[str] = Field(description= "Item Numbers")

parser = PydanticOutputParser(pydantic_object = PurchaseOrder)



def document_load_and_parse(temp_pdf_path,prompt,client):
    logging.info("Document loading from Blob....")
    loader = PyPDFLoader(temp_pdf_path)
    document = loader.load()
    print(document[0].page_content)

    document_query =  """Extract the values of PO Number, Ship To address, Ship Via method, PC Number and part Numbers.
    PC numbers will be mentioned as PC Number. Please note that some documents 
    might not have a PC Number mentioned, in that case assign value as 'PC number not Found'.
    Please note that some documents might not have The Ship Via method mention, 
    in that  case assign value as Freight Method Not Found.
    While extracting Part Numbers,if part number is mentioned after the term 'Zebra', consider ONLY that preceeding string to be the actual Part Number.
    Part numbers format examples: 
    521-678 (Not an actual part number)
    SE2707-LU000R (Actual Part Number format)
    AFT-SYG-FGH (Actual Part Number format)
    Special cases to extract Part Numbers/Item number:
    For example, the actual part number is ABC-2HG3-IOX but the part of the string can be present as ABC-2HG3- in the first line and rest IOX in the immediate next line.
       In that case, consider the whole string as the part number without separating it as two part numbers. 
    In case, no part numbers is found as per requirement, assign value as 'Part Number Not found'
    """+ document[0].page_content

    prompt_format = prompt.format_prompt(question = document_query)
    result = client.invoke(prompt_format.to_messages())
    parsed = parser.parse(result.content)
    logging.info("Parsing Completed....")
    logging.info(parsed)
    return parsed      

def validate_parsed_values_with_database(username,password,dsn,parsed):
    
    logging.info("Document Validation starts.......")
    global connection
    remarks = []
    global flag 
    flag = False
    try:        
        if not flag:
            cx_Oracle.init_oracle_client(lib_dir= r"C:\\Oracle19c-64bit\\product\\client_1\\bin\\")
            connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
            logging.info(f"\nConnected successfully!")
            flag = True

    except cx_Oracle.DatabaseError as e:
        logging.info(f"Error connecting to the database: {e}")


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
            logging.info(f"Part number {i} is not Valid")
            remarks.append(f"Part number {i} is not Valid")
        else:
            logging.info(f"Part number {i} is Valid")


    comparePONumber = cursor.execute(po_no_validate,value = parsed.po_number)
    cursor_fetchone = cursor.fetchone()
    if cursor_fetchone is None:
        print(f"PO number: {parsed.po_number} is new & should be processed further")
        remarks.append(f"PO number {parsed.po_number} is new and should be processed further.")
    else:
        print(f"PO number {parsed.po_number} already exists.")


    if "BESTWAY" in parsed.freight_acc_no or "Prepay & Add" in parsed.freight_acc_no:
        print(f"No Validation needed for Freight account number: {parsed.freight_acc_no}")
        remarks.append("No Validation needed for Freight account number")
    elif parsed.freight_acc_no == 'Ship Via/Freight Method Not Found':
        print(f"Freight Account Number not found")
        remarks.append("Freight Account Number not found") 
    else:
        freight_method = re.findall("\D", parsed.freight_acc_no)#contains No Digits
        freight_account_number = re.findall("\d",  parsed.freight_acc_no)#contains only digits
        print(''.join(freight_method))
        print("Printing freight account number ........:"+''.join(freight_account_number))

        if  freight_method and not freight_account_number:
            remarks.append(f"Freight account number is missing for Freight method - {''.join(freight_method)}")
            print(f"Freight account number is missing for {''.join(freight_method)}")
        else:
            print(f"Freight Account Number is present")

    comparePcNumber = cursor.execute(expired_pc,value = parsed.pc_no)
    cursor_fetchone_pc = cursor.fetchone()
    try: 
        if datetime.now() > cursor_fetchone_pc[0]: 
            print(f"PC number {parsed.pc_no} is expired")
            remarks.append(f"PC number {parsed.pc_no} is expired")
        else:
            print(f"PC number {parsed.pc_no} is not expired")
        
    except Exception as e:
        print(f"{e} :Pc Expired Date is not found in Database")
        remarks.append(f"Pc Expired Date is not found in Database")

    print(remarks)
    cursor.close()
    connection.close()
    logging.info("DB Validation is done....")
    return remarks


def create_excel_file(blob_service_client,container_name,upload_excel_blob_name,parsed,remarks):
    logging.info("Creating Excel File with the Validation Errors Info.....")
    dict_data1 = parsed.dict()
    print(dict_data1)
    
    combined_part_numbers=','.join(parsed.part_numbers)
    dict_data1['part_numbers']=combined_part_numbers
    logging.info(f"dict_data1...updated,,,:{dict_data1}")
    combined_remarks = ','.join(remarks)
    blob_client = blob_service_client.get_blob_client(container_name,upload_excel_blob_name)
    logging.info(f"blob client :{blob_client}")
    if blob_client.exists():
        # Download the blob content
        blob_content = blob_client.download_blob()
        logging.info(f"The file {blob_content} already exists.")
        df = pd.read_excel(blob_content.content_as_bytes())
        new_row = dict_data1.copy()
        new_row['Remarks'] = combined_remarks
        new_row['Sl. No.'] = len(df) + 1
        df = df._append(new_row, ignore_index=True)

        # Save the modified data to a new Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
        xlsx_data = output.getvalue()
        logging.info(f"The DataFrame has been updated to {xlsx_data}. ")

    else:
        data1 = pd.DataFrame(dict_data1,index=[0])
        data1['Remarks'] = combined_remarks
        data1.insert(0, 'Sl. No.', range(1, 1 + len(data1)))
        print("printing data1..... ")
        print(data1)
        #save the dataframe to excelfile
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            data1.to_excel(writer, sheet_name='Sheet1', index=False)

        xlsx_data = output.getvalue()
        logging.info(f"New DataFrame has been saved to {xlsx_data}.")
    
    return xlsx_data
        

def upload_excel_blob(blob_service_client,container_name, xlsx_data, upload_excel_blob_name):
    logging.info("Uploading error Excel to the Blob.....")
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=upload_excel_blob_name)
        blob_client.upload_blob(xlsx_data, overwrite=True)
        if blob_client.exists:
            logging.info(f"Uploaded {upload_excel_blob_name} successfully!")
        else:
            logging.info(f"Error uploading {upload_excel_blob_name}")

    except Exception as e:
        logging.info(f"Error uploading {upload_excel_blob_name}: {e}")

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
        logging.info(f"Mail Response: {response}")
        logging.info(f"Email sent! Status code: {response.status_code}")
        logging.info(response.body)
        logging.info(response.headers)
    except Exception as e:
        print(f"Exception{e}")
    logging.info("email sent status")

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="po-container/{name}.pdf",
                               connection="AzureWebJobsStorage") 
def BlobTrigger1(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Blob Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    connection_string = os.environ.get("STORAGE_ACCOUNT_CONNECTION_STRING")
    container_name = os.environ.get("CONTAINER_NAME")
    blob_name = myblob.name.split("/")[1]
    storage_acc_name = os.environ.get("AZURE_STORAGE_ACCOUNT")
    storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
    username = config.username
    password = config.password
    upload_excel_blob_name = "status_excel_"+str(date.today())+".xlsx"
    sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
    

    dsn = cx_Oracle.makedsn(
        host=config.host,
        port=config.port,
        service_name = config.service_name
    )

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

    # Get a BlobClient for your blob
    blob_client = blob_service_client.get_blob_client(container_name,blob_name)

    if blob_client.exists():
        blob_url = blob_client.url
        logging.info(f"Blob URL: {blob_url}")
    else:
        logging.info(f"Blob '{blob_name}' does not exist.")



    if blob_client.exists():
        logging.info(f"Blob '{blob_name}' exist.")
        pdf_data = blob_client.download_blob().readall()

        pdf_bytesio = io.BytesIO(pdf_data)

        # Create a temp folder and save the BytesIO object
        temp_dir = tempfile.mkdtemp()
        temp_pdf_path = os.path.join(temp_dir, 'temp.pdf')
        with open(temp_pdf_path, 'wb') as temp_file:
            temp_file.write(pdf_bytesio.getbuffer())

        
        logging.info("Document Parsing starts......")
        parsed_return_value  = document_load_and_parse(temp_pdf_path,prompt,client)
        remarks_list = validate_parsed_values_with_database(username,password,dsn,parsed_return_value)
        xlsx_data = create_excel_file(blob_service_client,container_name,upload_excel_blob_name,parsed_return_value,remarks_list)
        logging.info("uploading excel to blb container starts.....")
        upload_excel_blob(blob_service_client, container_name, xlsx_data, upload_excel_blob_name)
        logging.info("sending mail alert ....")
        send_alert_mail_using_sendgrid(sendgrid_api_key,upload_excel_blob_name,xlsx_data)
    else:
        logging(f"Blob '{blob_name}' does not exist.")
        
