import os
import tempfile
import io
import azure.functions as func
import logging
import config
import cx_Oracle
from datetime import datetime
from typing import List
import pandas as pd
import openpyxl
from azure.storage.blob import BlobServiceClient
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.prompts import ChatPromptTemplate,HumanMessagePromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel,Field
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv

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
    Please note that PO can also be mentioned as Purchase Contract as well, in that case consider PO number as the number mentioned.
    Extract PO number as only a numeric Value excluding any trailing and leading alphabets and symbols. 
    Example 1: 'PO-12345' should be extracted as '12345' withount any alphabets and symbols.
    Example 2: 'PO0012-345' should be extracted as '0012345' withount any alphabets and symbols.
    Example 3: 'FG00131572' should be extracted as '00131572' withount any prefix alphabets.
    PC numbers will be mentioned as PC Number. Please note that some documents 
    might not have a PC Number mentioned, in that case assign value as PC number not Found.
    Please note that some documents might not have The Ship Via method mention, 
    in that  case assign value as Freight Method Not Found.
    Please note that if in the part/description table, a number/alphanumeric number is mentioned after the term Zebra, consider ONLY that to be the actual Part Number otherwise consider Part Numbers to be only alphabets/alphanumeric without any spaces inside the Item/description table.
    Please note that multiple Part Numbers might also be mentioned in the table with column as 'SCHEDULE OF SUPPLIES AND SERVICES, in that case extract only those the part numbers.
    Please note Part numbers """+ document[0].page_content

    prompt_format = prompt.format_prompt(question = document_query)
    result = client.invoke(prompt_format.to_messages())
    parsed = parser.parse(result.content)
    logging.info("Parsing Completed....")
    logging.info(parsed)
    return parsed      

def validateParsedValuesWithDatabase(username,password,dsn,parsed):
    
    logging.info("Document Validation starts.......")
    global connection
    try:        
        cx_Oracle.init_oracle_client(lib_dir= r"C:\\Oracle19c-64bit\\product\\client_1\\bin\\")
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
        logging.info(f"\nConnected successfully!")

    except cx_Oracle.DatabaseError as e:
        logging.info(f"Error connecting to the database: {e}")

    logging.info(f"Connected to Oracle Database version: {connection.version}")
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
    if cursor.fetchone() is None:
        logging.info(f"PO number {parsed.po_number} is not Valid")
        remarks.append(f"PO number {parsed.po_number} is not Valid")
    else:
        logging.info(f"PO number {parsed.po_number} is Valid")

        # Bestway--- then no need to valid; Other than BestWay
    # compareFreightAccountNumber = cursor.execute(freight_account_number_validate,value = parsed.freight_acc_no)
    # if cursor.fetchone() is None:
    #     logging.info(f"Freight account number {parsed.freight_acc_no} is NOt Valid")
    #     remarks.append(f"Freight account number {parsed.freight_acc_no} is NOt Valid")
    # else:
    #     logging.info(f"Freight account number {parsed.freight_acc_no} is Valid")
    

    comparePcNumber = cursor.execute(expired_pc,value = parsed.pc_no)
    cursor_fetchone_pc = cursor.fetchone()
    try:
        if cursor_fetchone_pc is not None:
            if datetime.now() > cursor.fetchone()[0]: 
                logging.info(f"PC number {parsed.pc_no} is expired")
                remarks.append(f"PC number {parsed.pc_no} is expired")
            else:
                print(f"PC number {parsed.pc_no} is not expired")    
    except Exception as e:
        logging.info(f"{e} : Pc Expired Date is not found in Database")
        remarks.append(f"Pc Expired Date is not found in Database")

    print(remarks)
    cursor.close()
    connection.close()
    logging.info("DB Validation is done....")


def create_excel_file(file_path,parsed):
    logging.info("Creating Excel File with the Validation Errors Info.....")
    dict_data1 = parsed.dict()
    print(dict_data1)
    combined_remarks = ','.join(remarks)

    if os.path.exists(file_path):
        print(f"The file {file_path} already exists.")
        df = pd.read_excel(open(file_path, 'rb'))

        #df.loc[len(df)] = dict_data1.values()
        df = df._append(dict_data1, ignore_index=True)
        df['Remarks'] = combined_remarks
        df.to_excel(file_path, index=False)
        print(f"The DataFrame has been updated to {file_path}.")

    else:
        data1 = pd.DataFrame(dict_data1)
        data1['Remarks'] = combined_remarks
        #data1.rename(columns={'po_no': 'PO Number', 'ship_to': 'Ship To', 'freight_acc_no':'Freight Account Number','pc_no':'PC Number', 'part_numbers':'Part Numbers'}, inplace=True)
        data1.insert(0, 'Sl. No.', range(1, 1 + len(data1)))
        print("printing data1.....")
        print(data1)
        data1.to_excel(file_path, index=False)
        print(f"New DataFrame has been saved to {file_path}.")

    
def upload_excel_blob(account_name, account_key, container_name, local_file_path, upload_excel_blob_name):
    logging.info("Uploading error Excel to the Blob.....")
    try:
        # Create a BlobServiceClient using your storage account credentials
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)

        # Get a container client
        container_client = blob_service_client.get_container_client(container_name)

        

        # Upload the local Excel file to the blob
        with open(local_file_path, "rb") as data:
            blob_client = container_client.upload_blob(name=upload_excel_blob_name, data=data, overwrite=True)
            print(f"Uploaded {upload_excel_blob_name} successfully!")

    except Exception as e:
        print(f"Error uploading {upload_excel_blob_name}: {e}")


app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="po-container/{name}.pdf",connection="doc1storageacc_STORAGE") 
def blob_trigger1(myblob: func.InputStream):
    print("entering logs")
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    # Replace with your actual connection string and blob/container names
    global remarks
    remarks = []
    connection_string = os.environ.get("CONNECTION_STRING")
    container_name = "po-container"
    blob_name = myblob.name.split("/")[1]
    storage_acc_name = os.environ.get("AZURE_STORAGE_ACCOUNT")
    azure_account_key = os.environ.get("AZURE_ACCOUNT_KEY")
    username = config.username
    password = config.password
    local_excel_file_path = r"C:\Users\TR8361\my_env\sample_excel.xlsx"
    upload_excel_blob_name = "sample_excel.xlsx"

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
        
        validateParsedValuesWithDatabase(username,password,dsn,parsed_return_value)
        create_excel_file(local_excel_file_path,parsed_return_value)
        upload_excel_blob(storage_acc_name, azure_account_key, container_name, local_excel_file_path, upload_excel_blob_name)

    else:
        logging(f"Blob '{blob_name}' does not exist.")

    



    