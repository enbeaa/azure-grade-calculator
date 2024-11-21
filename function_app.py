import datetime
import os
import azure.functions as func
import logging
import pandas as pd
from io import BytesIO, StringIO
import azure.durable_functions as duf
import numpy as np
from azure.storage.blob import BlobServiceClient

# initialise durable function app
app = duf.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.blob_trigger(arg_name="myblob", path="students", connection="myfreedbresourcegroaebb_STORAGE")
@app.durable_client_input(client_name="client")
async def GradeCalcTrigger(myblob: func.InputStream, client):

    # Getting the upload time of the blob
    connection_string = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "students"
    filename = os.path.basename(myblob.name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_properties = blob_client.get_blob_properties()
    start_time = blob_properties['last_modified'].isoformat()
    logging.info(f"Blob '{myblob.name}' uploaded at: {start_time}") 

    # Reading the blob and grouping by studentID
    student_file = pd.read_csv(BytesIO(myblob.read()), sep='\t')
    student_dfs = student_file.groupby('StudentID')

    students=[]
    for student_id, df in student_dfs:
        student = df.to_csv(None, sep='\t', encoding='utf-8', index=False, header=True)
        students.append(student)

    # Start the orchestrator function
    instance_id = await client.start_new("GradeCalcOrchestrator", None, {"students": students, "start_time": start_time, "output_file": f"results_{filename}"})
    logging.info(f"Started orchestration with instance ID = {instance_id}")

@app.function_name(name="GradeCalcOrchestrator")
@app.orchestration_trigger(context_name="context")
def GradeCalcOrchestrator(context: duf.DurableOrchestrationContext):

    input_data = context.get_input()
    students = input_data["students"]
    tasks = []
    for student in students:
        tasks.append(context.call_activity("GradeCalcWorker", student))

    results = yield context.task_all(tasks)

    results_list = []
    for result in results:
        student_id = result[0]
        average = result[1]
        degree_class = result[2]

        results_list.append([student_id, average, degree_class])
    
    df_results = pd.DataFrame(results_list, columns=["StudentID","Average","Degree Class"])
    content = df_results.to_csv(None, sep='\t', encoding='utf-8', index=False, header=True)
    
    output_file = input_data["output_file"]
    yield context.call_activity("GradeCalcOutput", {"content": content, "blob_name": output_file})

    # Calculate time
    start_time = datetime.datetime.fromisoformat(input_data["start_time"])
    end_time = context.current_utc_datetime
    total_duration = end_time - start_time
    logging.info(f"Workflow total runtime: {total_duration}")
    return ('Grades successfully logged.')


@app.function_name(name="GradeCalcWorker")
@app.activity_trigger(input_name="dataframe")
def GradeCalcWorker(dataframe: str):

    try:          
            student = pd.read_csv(StringIO(dataframe), sep='\t')
            failed_important = any(
                    (student['PassForProgress'] == True) & 
                    (student['Result'] < 40)
                )
            
            grade = 0
            if failed_important:
                grade = 0
            else:
                grade = np.average(
                    student['Result'],
                    weights=student['Credits']
                ).round()

            if grade >= 70:
                degreeClass = "First"
            elif grade >= 60:
                degreeClass = "2:1"
            elif grade >= 50:
                degreeClass = "2:2"
            elif grade >= 40:
                degreeClass = "Pass"
            else:
                degreeClass = "Fail"
            student_grade = [student.iloc[0]['StudentID'], grade, degreeClass]
            logging.info(f"Student Grade Calculated: {student_grade}")
            return student_grade
    
    except Exception as e:
        # Log the error for debugging
        logging.error(f"Error in GradeCalcWorker:{e}")
 


@app.function_name(name="GradeCalcOutput")
@app.activity_trigger(input_name="outputvalues")
@app.blob_output(arg_name="outputblob", path="results", connection="myfreedbresourcegroaebb_STORAGE")
def GradeCalcOutput(outputvalues: dict, outputblob: func.Out[str]):
    logging.info('output started')
    content = outputvalues['content']
    blob_name = outputvalues['blob_name']

    connection_string = os.environ.get('AzureWebJobsStorage')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "results"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    blob_client.upload_blob(content, overwrite=True)
    logging.info(f"Successfully uploaded {blob_name} to Blob Storage.")
    return "Success"       