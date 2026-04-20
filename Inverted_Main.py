# ==========================================
# 1. Importing Libraries
# ==========================================
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg
from pydantic import BaseModel
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import bcrypt
import time

# ==========================================
# 2. Defining Constants & Configuration
# ==========================================
load_dotenv()
BASE_URL = "https://portal.university.edu.ph"
LOGIN_URL = f"{BASE_URL}/some_login_endpoint"
GRADES_ENDPOINT = f"{BASE_URL}/some_grades_endpoint"

templates = Jinja2Templates(directory="templates")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': f"{BASE_URL}/some_specific_grades_endpoint",
    'Accept': 'application/json, text/javascript, */*; q=0.01'
}

term_mapping = {'termcode1' : 20252, 'termcode2' : 20251, 'termcode3' : 20242,
                'termcode4' : 20241, 'termcode5' : 20232, 'termcode6' : 20231}

terms = ['termcode1', 'termcode2', 'termcode3', 'termcode4', 'termcode5', 'termcode6']

term_dict = {
    'termcode1' : ['2', '2025'], 'termcode2' : ['1', '2025'],
    'termcode3' : ['2', '2024'], 'termcode4' : ['1', '2024'],
    'termcode5' : ['2', '2023'], 'termcode6' : ['1', '2023'],
}

column_dtype_mapping = {
    'student_number' : int, 'program' : str, 'subject_number' : int,
    'code' : str, 'descriptive' : str, 'units' : int,
    'final_average' : float, 'equivalent_grade' : float, 'remarks' : str,
    'semester' : int, 'school_year' : int, 'term_code' : int
}

# ==========================================
# 3. Defining Logging Functions
# ==========================================
def log_to_db(student_num, category, portal_time, total_time, rows, term, status="SUCCESS"):
    with psycopg.connect(
        host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')) as conn:
        with conn.cursor() as curr:
            curr.execute("""
                INSERT INTO audit_logs_session 
                (student_number, user_category, portal_latency_sec, total_session_sec, rows_processed, last_term, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (str(student_num), category, portal_time, total_time, rows, term, status))
            conn.commit()

def log_to_db_errors(student_num, error):
    with psycopg.connect(
        host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')) as conn:
        with conn.cursor() as curr:
            curr.execute("""
                INSERT INTO system_logs_errors (student_number, error)
                VALUES (%s, %s)
            """, (str(student_num), str(error))) 
            conn.commit()

# ==========================================
# 4. Defining Response Data Types
# ==========================================
class StudentResponse(BaseModel):
    student_number: int
    year_level: int
    semester : int
    general_weighted_average: float
    academic_honors : str

# ==========================================
# 5. Start of Application
# ==========================================
app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={"results": None})

@app.post('/student', response_class=HTMLResponse)
async def StudentLogging(request: Request, student_number: int = Form(...), password: str = Form(...)):
    global_start_time = time.time()
    USERNAME = student_number
    PASSWORD = password
    
    # Initializing variables to prevent reference errors
    old_last_term = [[0]]
    existing_credentials = []
    clean_columns = [] 
    rows_data = []

    # # Extracting existing credentials from the database
    with psycopg.connect(
        host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')) as conn:
        with conn.cursor() as curr:
            credentials_select_query = 'SELECT student_number, password FROM stg_student_auth'
            curr.execute(credentials_select_query)
            data = curr.fetchall()
            for row in data:
                existing_credentials.append(row)

    existing_credentials2 = []
    for row in existing_credentials:
        for entry in row:
            existing_credentials2.append(entry)
            
    existing_terms = []

    # # Checking if login credentials are correct
    if str(USERNAME) in [str(x) for x in existing_credentials2]:
        terms_to_extract = []
        with psycopg.connect(
            host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')) as conn:
            with conn.cursor() as curr:
                curr.execute("SELECT password FROM stg_student_auth WHERE student_number = %s", (str(USERNAME),))
                result = curr.fetchone()
                if result:
                    stored_hash = result[0]
                    is_valid = bcrypt.checkpw(PASSWORD.encode('utf-8'), stored_hash.encode('utf-8'))
                
                if not is_valid:
                    return templates.TemplateResponse(
                        request=request, 
                        name="index.html", 
                        context={"error": "Invalid Password for this Student Number", "results": None})
                
                # # Checking if there are unextracted data in the system
                curr.execute('SELECT term_code FROM fct_student_metrics WHERE student_number = %s', (str(USERNAME),))
                termcodes = curr.fetchall()
                for termcode in termcodes:
                    existing_terms.append(termcode[0])
                
                curr.execute("SELECT last_term FROM dim_student_status WHERE student_number = %s", (str(USERNAME),))
                old_last_term_data = curr.fetchall()
                if old_last_term_data:
                    old_last_term = old_last_term_data

        for key, values in term_mapping.items():
            if values not in existing_terms and values > int(old_last_term[0][0]): 
                terms_to_extract.append(key)

        # # Optimizing portal requests: If only the latest term is left, extract from DB
        if terms_to_extract == ['termcode1']: 
            with psycopg.connect(
                    host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
                    port=os.getenv('DB_PORT')) as conn:
                    with conn.cursor() as curr:
                        curr.execute("SELECT student_number, year_level, semester, general_weighted_average, academic_honors FROM fct_student_metrics WHERE student_number = %s", (str(USERNAME),))
                        data = curr.fetchall()
                        student_data = [row for row in data]
                        student_showable = pd.DataFrame(student_data, columns=['student_number', 'year_level', 'semester', 'general_weighted_average', 'academic_honors'])
                        student_data = student_showable.to_dict(orient='records')

                        curr.execute("SELECT descriptive, equivalent_grade, units, remarks, academic_level, semester FROM stg_student_grades WHERE student_number = %s", (str(USERNAME),))
                        raw_data = curr.fetchall()
                        student_raw = [{"descriptive": r[0], "equivalent_grade": r[1], "units": r[2], "remarks": r[3], "academic_level": r[4], "semester": r[5]} for r in raw_data]
                        
                        global_duration = time.time() - global_start_time
                        log_to_db(USERNAME, "CACHED_USER", 0, global_duration, len(student_data), old_last_term[0][0])
                        return templates.TemplateResponse(request=request, name="index.html", context={"results": student_data, "raw_results": student_raw})

        user_category = 'RETURNING_USER'
        terms_final = terms_to_extract
    else: 
        user_category = 'NEW_USER'
        terms_final = terms

    # # Initializing Portal Session
    session = requests.Session()
    try:
        res = session.get(LOGIN_URL, headers=HEADERS, timeout=10)
        res.raise_for_status() 
    except Exception as e:
        log_to_db(USERNAME, "PORTAL_ERROR", 0, 0, 0, 0, status=f"Connection Failed: {str(e)[:50]}")
        return templates.TemplateResponse(request=request, name="index.html", context={"error": "Portal is busy, try again later."})

    soup = BeautifulSoup(res.text, 'html.parser')
    token = soup.find('input', dict(name='_token'))['value']

    login_payload = {'Username': USERNAME, 'password': PASSWORD, '_token': token}

    # # Logging portal response duration
    start_time = time.time()
    login_response = session.post(LOGIN_URL, data=login_payload, headers=HEADERS)
    portal_duration = time.time() - start_time

    if login_response.url == LOGIN_URL:
        return templates.TemplateResponse("index.html", {"request": request, "results": None, "error": "Invalid Student Number or Password"})
    
    # # Extraction Loop: Scoping through relevant academic terms
    for term in terms_final:
        grades_payload = {"event": "grades", "term": term, "_token": token}
        try:
            response = session.post(GRADES_ENDPOINT, data=grades_payload, headers=HEADERS)
            data = response.json()

            if 'list' in data:
                grade_soup = BeautifulSoup(data['list'], 'html.parser')
                unclean_columns = grade_soup.find_all('th')
                clean_columns = [column.text.strip() for column in unclean_columns] + ['semester', 'school_year', 'student_number']
                term_data = term_dict.get(term, ['No Term Data'])
                all_rows = grade_soup.find_all('tr')

                for tr in all_rows:
                    cells = tr.find_all('td')
                    if len(cells) == 8:
                        first_entry = cells[0].get_text(strip=True)
                        if first_entry.endswith('.') and first_entry[:-1].isdigit():
                            new_row = [c.get_text(strip=True) for c in cells] + term_data + [USERNAME]
                            if new_row not in rows_data:
                                rows_data.append(new_row)
        except Exception as e:
            log_to_db_errors(USERNAME, str(e))

    # # Data Transformation using Pandas
    try:
        df = pd.DataFrame(rows_data, columns=clean_columns)
        df.columns = (df.columns.str.strip().str.lower().str.replace(' ', '_'))
        df = df[df['final_average'] != '']
        df['term_code'] = df['school_year'].astype(str) + df['semester'].astype(str)

        program_data = grade_soup.find(class_="form-control").text.strip()
        program = 'Bachelor' + program_data.split('Bachelor')[-1]

        df['#'] = df['#'].str.replace('.', '').astype(int)
        df['section'] = program
        df = df.rename(columns={'#': 'subject_number', 'section': 'program'})
        df = df[['student_number', 'program', 'subject_number', 'code', 'descriptive', 'units', 'final_average', 'equivalent_grade', 'remarks', 'semester', 'school_year', 'term_code']]
        df = df.astype(column_dtype_mapping)

        # # Loading transformed data to PostgreSQL
        conn_url = f"postgresql+psycopg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(conn_url)
        df.to_sql(name='stg_raw_grades', con=engine, if_exists='replace', index=False)
        
        last_term = df['term_code'].max()
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(PASSWORD.encode('utf-8'), salt).decode('utf-8')
        df_auth = pd.DataFrame([[program, USERNAME, hashed_password, last_term]], columns=['program','student_number', 'password', 'last_term'])
        df_auth.to_sql(name='stg_student_auth', con=engine, if_exists='replace', index=False) 

    except Exception as e:
        if str(e) == 'final_average': print('No New Data Extracted')

    # # Executing SQL Stored Procedure for further processing
    with psycopg.connect(
            host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')) as conn:
            with conn.cursor() as curr:
                curr.execute("CALL prc_transform_student_data();")
                conn.commit()

                # # Fetching Final Aggregated and Raw data for display
                curr.execute("SELECT student_number, year_level, semester, general_weighted_average, academic_honors FROM fct_student_metrics WHERE student_number = %s", (str(USERNAME),))
                student_data = [row for row in curr.fetchall()]
                student_showable = pd.DataFrame(student_data, columns=['student_number', 'year_level', 'semester', 'general_weighted_average', 'academic_honors'])
                
                curr.execute("SELECT descriptive, equivalent_grade, units, remarks, academic_level, semester FROM stg_student_grades WHERE student_number = %s", (str(USERNAME),))
                student_raw = [{"descriptive": r[0], "equivalent_grade": r[1], "units": r[2], "remarks": r[3], "academic_level": r[4], "semester": r[5]} for r in curr.fetchall()]

            global_duration = time.time() - global_start_time
            log_to_db(USERNAME, user_category, portal_duration, global_duration, len(student_data), last_term)
            return templates.TemplateResponse(request=request, name="index.html", context={"results": student_showable.to_dict(orient='records'), "raw_results": student_raw})