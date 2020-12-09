import pyodbc
import pandas as pd


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DSQL23CAP;'
                      'Database=DS_TEST;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

# does not allow BULK INSERT
# pandas dataframe.to_sql() doesn't work

# ====== create and populate project table ======
cursor.execute('''
    DROP TABLE IF EXISTS project;
    
    CREATE TABLE project (
        id							bigint primary key,
        application_id				int,
        application_title			varchar(255),
        application_title_short		varchar(255),
        company						varchar(255)
    );
''')
conn.commit()

cursor.execute('TRUNCATE TABLE project;')
conn.commit()

df_project = pd.read_csv('project.csv')
for index, row in df_project.iterrows():
    cursor.execute('INSERT INTO project VALUES (?,?,?,?,?)', (
        int(row['id']),
        int(row['application_id']),
        row['application_title'],
        row['application_title_short'],
        row['company'])
    )
    conn.commit()

# ====== create and populate pdf table ======
cursor.execute('''
    DROP TABLE IF EXISTS pdf;
    
    CREATE TABLE pdf (
        id							BIGINT PRIMARY KEY,
        project_id				    BIGINT REFERENCES project(id),
        name						VARCHAR(255),
        pdf_id_regdoc				INT,
        filing_id					INT,
        date_time					DATETIME,
        monitoring_year				INT,
        monitoring_year_ordinal		INT,
        submitter					VARCHAR(255)
    );
''')
conn.commit()

df_pdf = pd.read_csv('pdf.csv')
for index, row in df_pdf.iterrows():
    cursor.execute(
        'INSERT INTO pdf VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (
            int(row['id']),
            int(row['project_id']),
            row['name'],
            int(row['pdf_id_regdoc']),
            int(row['filing_id']),
            str(row['date_time']),
            int(row['monitoring_year']),
            int(row['monitoring_year_ordinal']),
            row['submitter']
        )
    )
    conn.commit()

# ====== create and populate consultant table ======
cursor.execute('''
    DROP TABLE IF EXISTS consultant;
    
    CREATE TABLE consultant (
        id							BIGINT PRIMARY KEY,
        name						VARCHAR(255)
    );
''')
conn.commit()

df_consultant = pd.read_csv('consultant.csv')
for index, row in df_consultant.iterrows():
    cursor.execute(
        'INSERT INTO consultant VALUES (?, ?)', (
            int(row['id']),
            row['name']
        )
    )
    conn.commit()

# ====== create and populate pdf_consultant_mapping table ======
cursor.execute('''
    DROP TABLE IF EXISTS pdf_consultant_mapping;
    
    CREATE TABLE pdf_consultant_mapping (
        id				BIGINT PRIMARY KEY,
        pdf_id			BIGINT REFERENCES pdf(id),
        consultant_id	BIGINT REFERENCES consultant(id)
    );
''')
conn.commit()

df_pdf_consultant_mapping = pd.read_csv('pdf_consultant_mapping.csv')
for index, row in df_pdf_consultant_mapping.iterrows():
    cursor.execute(
        'INSERT INTO pdf_consultant_mapping VALUES (?,?,?)', (
            int(row['id']),
            int(row['pdf_id']),
            int(row['consultant_id'])
        )
    )
    conn.commit()

# ======= create and populate pdf_table table =======
cursor.execute('''
    DROP TABLE IF EXISTS pdf_table;
    
    CREATE TABLE pdf_table (
        id				BIGINT PRIMARY KEY,
        pdf_id			BIGINT REFERENCES pdf(id),
        table_id_old    VARCHAR(36),
        title			VARCHAR(255),
        pipeline_name	VARCHAR(255)
    );
''')
conn.commit()

df_pdf_table = pd.read_csv('pdf_table.csv').sort_values('id')
for index, row in df_pdf_table.iterrows():
    cursor.execute(
        'INSERT INTO pdf_table VALUES (?,?,?,?,?)', (
            int(row['id']),
            int(row['pdf_id']),
            row['table_id_old'],
            row['title'],
            row['pipeline_name'] if type(row['pipeline_name']) is str else None
        )
    )
    conn.commit()

# ====== create and populate issue table ======
cursor.execute('''
    DROP TABLE IF EXISTS issue;
    
    CREATE TABLE issue (
        id				BIGINT PRIMARY KEY,
        pdf_table_id	BIGINT REFERENCES pdf_table(id),
        row_index		INT,
        vec_simple		VARCHAR(255),
        subvec_simple	VARCHAR(255)
    );
''')
conn.commit()

df_issue = pd.read_csv('issue.csv').sort_values('id')
for index, row in df_issue.iterrows():
    cursor.execute(
        'INSERT INTO issue VALUES (?,?,?,?,?)', (
            int(row['id']),
            int(row['pdf_table_id']),
            int(row['row_index']),
            row['vec_simple'],
            row['subvec_simple'] if type(row['subvec_simple']) is str else None
        )
    )
    conn.commit()

# ====== create and populate location table ======
cursor.execute('''
    DROP TABLE IF EXISTS location;
    
    CREATE TABLE location (
        id				BIGINT PRIMARY KEY,
        issue_id		BIGINT REFERENCES issue(id),
        loc_no			INT,
        loc_text		TEXT,
        loc_format		VARCHAR(4),
        loc_start		VARCHAR(20),
        loc_end			VARCHAR(20)
    );
''')
conn.commit()

df_location = pd.read_csv('location.csv').sort_values('id')
for index, row in df_location.iterrows():
    cursor.execute(
        'INSERT INTO location VALUES (?,?,?,?,?,?,?)', (
            int(row['id']),
            int(row['issue_id']),
            int(row['no']),
            row['text'],
            row['format'],
            row['start'],
            row['end'] if type(row['end']) is str else None
        )
    )
    conn.commit()


cursor.close()
conn.close()
