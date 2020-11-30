import pandas as pd
import mysql.connector


db_username = ''
db_password = ''
conn = mysql.connector.connect(host='os25.neb-one.gc.ca',
                               database='pcmr',
                               user=db_username,
                               password=db_password)

print(conn.is_connected())

query_project = '''
    SELECT A.application_id, application_title, application_title_short, company
    FROM projects A JOIN (
        SELECT distinct application_id, company FROM pdfs
    ) B ON A.application_id = B.application_id;
'''

query_pdf = '''
    SELECT application_id, pdfName AS name, pdfId AS pdf_id_regdoc, filingId AS filing_id, date AS date_time, monitoring_year, monitoring_year_ordinal, submitter
    FROM pdfs; 
'''

query_pdf_consultant = '''
    SELECT pdfName, consultantName
    FROM pdfsconsultants;
'''

query_pdf_table = '''
    SELECT pdfName, A.tableId as table_id_old, tableTitle AS title, pipelineName AS pipeline_name
    FROM tables A JOIN (
        SELECT DISTINCT tableId, pipelineName FROM issues) B ON A.tableId = B.tableId;
'''

query_issue = '''
    SELECT tableId as table_id_old, rowIndex AS row_index, vec_simple, subvec_simple
    FROM issues;
'''

query_location = '''
    SELECT tableId as table_id_old, rowIndex, locNo AS no, locText AS text, locFormat AS format, startLoc AS start, endLoc AS end
    FROM locations;
'''


df_project = pd.read_sql(query_project, con=conn)
df_pdf = pd.read_sql(query_pdf, con=conn)
df_pdf_consultant = pd.read_sql(query_pdf_consultant, con=conn)
df_pdf_table = pd.read_sql(query_pdf_table, con=conn)
df_issue = pd.read_sql(query_issue, con=conn)
df_location = pd.read_sql(query_location, con=conn)

conn.close()

# export to project.csv
df_project = df_project.reset_index().rename(columns={'index': 'id'})
df_project.to_csv('project.csv', index=False)

# export to pdf.csv
pdf_cols = ['project_id', 'name', 'pdf_id_regdoc', 'filing_id', 'date_time', 'monitoring_year',
            'monitoring_year_ordinal', 'submitter']
df_pdf = df_pdf.merge(
    df_project.rename(columns={'id': 'project_id'}))\
    [pdf_cols]
df_pdf = df_pdf.reset_index().rename(columns={'index': 'id'})
df_pdf.to_csv('pdf.csv', index=False)

# export to consultant.csv
df_consultant = pd.DataFrame(df_pdf_consultant['consultantName'].unique(), columns=['name'])
df_consultant = df_consultant.reset_index().rename(columns={'index': 'id'})
df_consultant.to_csv('consultant.csv', index=False)

# export to pdf_consultant_mapping.csv
df_pdf_consultant.merge(
        df_pdf.rename(columns={'id': 'pdf_id'})[['pdf_id', 'name']], left_on='pdfName', right_on='name')\
    .merge(
        df_consultant.rename(columns={'id': 'consultant_id'}), left_on='consultantName', right_on='name')\
    [['pdf_id', 'consultant_id']].to_csv('pdf_consultant_mapping.csv', index_label='id')

# export to pdf_table.csv
df_pdf_table = df_pdf_table.reset_index().rename(columns={'index': 'id'})
df_pdf_table.merge(
    df_pdf.reset_index().rename(columns={'index': 'pdf_id'})[['pdf_id', 'name']], left_on='pdfName', right_on='name')\
    [['id', 'pdf_id', 'table_id_old', 'title', 'pipeline_name']].to_csv('pdf_table.csv', index=False)

# export to issue
df_issue = df_issue.reset_index().rename(columns={'index': 'id'})
df_issue.merge(
    df_pdf_table.rename(columns={'id': 'pdf_table_id'}))\
    [['id', 'pdf_table_id', 'table_id_old', 'row_index', 'vec_simple', 'subvec_simple']].to_csv('issue.csv', index=False)

# export to location
df_location = df_location.reset_index().rename(columns={'index': 'id'})
df_location.merge(
    df_issue.rename(columns={'id': 'issue_id'}), left_on=['table_id_old', 'rowIndex'], right_on=['table_id_old', 'row_index'])\
    [['id', 'issue_id', 'table_id_old', 'no', 'text', 'format', 'start', 'end']].to_csv('location.csv', index=False)
