import streamlit as st
import csv
import re
import difflib
import io
import zipfile
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from collections import defaultdict

# --- Page Config ---
st.set_page_config(page_title="Medical Claims & HR Processor", layout="wide")

# --- Custom CSS for Dark Blue Header & Footer & Table ---
def apply_custom_styling():
    st.markdown("""
        <style>
            /* Apply background to the top header bar */
            header[data-testid="stHeader"] {
                background-color: #002b5b !important;
            }
            
            /* Change color of the "hamburger" menu and elements in header */
            header[data-testid="stHeader"] svg {
                fill: white !important;
            }

            /* Custom Header Container */
            .header-box {
                background-color: #002b5b;
                padding: 1.5rem;
                border-radius: 10px;
                color: white;
                text-align: center;
                margin-bottom: 2rem;
            }

            /* Custom Footer */
            .footer-box {
                position: fixed;
                left: 0;
                bottom: 0;
                width: 100%;
                background-color: #002b5b;
                color: white;
                text-align: center;
                padding: 10px;
                font-size: 14px;
                z-index: 100;
            }

            /* Main body padding to avoid footer overlap */
            .main .block-container {
                padding-bottom: 100px;
            }

            /* Make footer text white */
            footer {
                visibility: hidden;
            }
            
            /* Adjust Subheaders */
            h2, h3 {
                color: #002b5b;
            }
            
            /* Custom Styled Dashboard Table */
            .dash-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 1rem;
                margin-bottom: 2rem;
                font-family: sans-serif;
                background-color: white;
            }
            .dash-table th {
                background-color: #002b5b;
                color: white;
                padding: 12px;
                text-align: left;
                border: 1px solid #ddd;
            }
            .dash-table td {
                padding: 10px;
                border: 1px solid #ddd;
                color: #333;
            }
            .dash-table tr:nth-child(even) {
                background-color: #f2f6fa;
            }
            /* Style the very last row (Grand Total / Footer) */
            .dash-table tr:last-child td {
                background-color: #002b5b !important;
                color: white !important;
                font-weight: bold;
            }
        </style>
        
        <div class="header-box">
            <h1>🏥 Medical Claims & HR Processor Dashboard</h1>
            <p>Upload files to process claims, match HR records, and generate business insights</p>
        </div>
        
        <div class="footer-box">
            © 2026 Mediclaim Audit System | Dark Blue Dashboard Theme | Secure Processing
        </div>
    """, unsafe_allow_html=True)

apply_custom_styling()

# --- Helper Functions ---
def parse_date(d_str):
    if not d_str: return None
    d_str = str(d_str).strip().split(' ')[0].replace('/', '-')
    formats = ['%d-%b-%y', '%d-%b-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d-%m-%y']
    for fmt in formats:
        try: return datetime.strptime(d_str, fmt)
        except ValueError: pass
    return None

def dt_to_str(dt):
    return dt.strftime("%d-%b-%y")

def get_combo_key(raw_cust_id, raw_date_str):
    if not raw_cust_id or str(raw_cust_id).strip() == "":
        return None
        
    c_id = str(raw_cust_id).strip()
    if c_id.endswith('.0'): 
        c_id = c_id[:-2]
        
    d_str = str(raw_date_str).strip().split(' ')[0].replace('/', '-')
    parsed_dt = None
    formats = ['%d-%b-%y', '%d-%b-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d-%m-%y', '%m-%d-%Y']
    for fmt in formats:
        try:
            parsed_dt = datetime.strptime(d_str, fmt)
            break
        except ValueError:
            pass
    
    if parsed_dt:
        date_part = parsed_dt.strftime("%d-%m-%Y")
    else:
        date_part = d_str
        
    return f"{c_id}_{date_part}"

def find_idx(header, possible_names, default_idx):
    for i, col in enumerate(header):
        clean_col = col.strip().lower()
        for name in possible_names:
            if name.lower() == clean_col: return i
    return default_idx

def is_city_match(loc1, loc2, threshold=0.8):
    if not loc1 or not loc2 or str(loc1).strip() == "" or str(loc2).strip() == "":
        return False
        
    loc1_clean = re.sub(r'[^\w\s]', ' ', str(loc1)).strip().lower()
    loc2_clean = re.sub(r'[^\w\s]', ' ', str(loc2)).strip().lower()
    
    if loc1_clean == loc2_clean: return True
    if loc1_clean in loc2_clean or loc2_clean in loc1_clean: return True
        
    tokens1 = loc1_clean.split()
    tokens2 = loc2_clean.split()
    
    for t1 in tokens1:
        for t2 in tokens2:
            if len(t1) <= 3 or len(t2) <= 3:
                continue
            similarity = difflib.SequenceMatcher(None, t1, t2).ratio()
            if similarity >= threshold:
                return True
    return False

def generate_html_table(data):
    """Converts the dashboard dictionary list into a styled HTML table"""
    if not data: return ""
    headers = data[0].keys()
    html = '<table class="dash-table"><thead><tr>'
    for h in headers:
        html += f'<th>{h}</th>'
    html += '</tr></thead><tbody>'
    for row in data:
        html += '<tr>'
        for h in headers:
            html += f'<td>{row[h]}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html

# --- Core Processing Pipeline ---
def process_data(claim_file, hr_file, gg_file):
    output_files = {}
    
    claim_content = claim_file.getvalue().decode('utf-8', errors='ignore')
    hr_content = hr_file.getvalue().decode('utf-8', errors='ignore')
    gg_content = gg_file.getvalue().decode('utf-8', errors='ignore')

    # --- STEP 0: Process GG App Data File ---
    gg_lookup = {}
    gg_target_cols = [
        "lead_id", "entity_type", "creator_cust_id", "ownership_type", 
        "solution_type", "channel", "created_at", "status", "id", 
        "audit_trail_id", "solution_key2", "solution_key3", "solution_value"
    ]
    gg_indexes = []
    
    gg_reader = csv.reader(io.StringIO(gg_content))
    gg_header = next(gg_reader, [])
    
    idx_gg_cust = find_idx(gg_header, ["cust_id", "cust id", "customer id", "customer_id"], -1)
    idx_gg_created = find_idx(gg_header, ["created_at"], -1)
    
    for col in gg_target_cols:
        gg_indexes.append(find_idx(gg_header, [col], -1))
    
    if idx_gg_cust != -1 and idx_gg_created != -1:
        for row in gg_reader:
            if len(row) > idx_gg_cust and len(row) > idx_gg_created:
                raw_c_id = row[idx_gg_cust]
                raw_dt = row[idx_gg_created]
                
                combo_key = get_combo_key(raw_c_id, raw_dt)
                
                if combo_key:
                    vals = [row[i].strip() if i != -1 and i < len(row) else "" for i in gg_indexes]
                    gg_lookup[combo_key] = vals

    # --- STEP 1: Process Claim File (Expand Dates & Merge GG Data) ---
    expanded_claims = []
    filtered_count = 0
    match_count = 0
    
    first_line = claim_content.split('\n')[0]
    delim = '\t' if '\t' in first_line else ','
    claim_reader = csv.reader(io.StringIO(claim_content), delimiter=delim)
    
    raw_header = next(claim_reader, [])
    extended_raw_header = raw_header + gg_target_cols
    
    idx_cid = find_idx(raw_header, ["Claim ID", "Claim_ID"], 1)
    idx_emp = find_idx(raw_header, ["E code", "EmpID", "Employee Code"], 2)
    idx_adm = find_idx(raw_header, ["Date of Admission", "Admit", "Admit Date"], 7)
    idx_dis = find_idx(raw_header, ["Date of Discharge", "Discharge", "Discharge Date"], 8)
    idx_claim_cust = find_idx(raw_header, ["cust_id", "cust id", "customer id", "customer_id"], -1)
    idx_emp_loc = find_idx(raw_header, ["Employee office Location"], -1)
    idx_hosp_loc = find_idx(raw_header, ["Hospital Location"], -1)
    
    for row in claim_reader:
        if not row: continue
        while len(row) < len(raw_header): row.append("")
        if len(row) <= max(idx_cid, idx_emp, idx_adm, idx_dis): continue
        
        skip_row = False
        for cell in row:
            clean_cell = str(cell).replace('\xa0', ' ').replace('\u200b', '').strip().lower()
            if "duplicate claim" in clean_cell or "rejected case" in clean_cell:
                skip_row = True
                break
        
        if skip_row:
            filtered_count += 1
            continue
        
        raw_claim_cust_id = row[idx_claim_cust] if idx_claim_cust != -1 and idx_claim_cust < len(row) else ""
        raw_admit_dt = row[idx_adm] if idx_adm != -1 and idx_adm < len(row) else ""
        
        combo_key = get_combo_key(raw_claim_cust_id, raw_admit_dt)
        
        gg_vals = [""] * len(gg_target_cols)
        if combo_key and combo_key in gg_lookup:
            gg_vals = gg_lookup[combo_key]
            match_count += 1

        extended_row = row + gg_vals

        empid = row[idx_emp].strip()
        cid = row[idx_cid].strip()
        admit = row[idx_adm].strip()
        discharge = row[idx_dis].strip()
        
        dt_admit = parse_date(admit)
        dt_disch = parse_date(discharge)
        
        if dt_admit and dt_disch:
            curr = dt_admit
            while curr <= dt_disch:
                row_for_day = list(extended_row)
                if idx_adm != -1 and idx_adm < len(row_for_day):
                    row_for_day[idx_adm] = dt_to_str(curr)
                    
                expanded_claims.append({
                    'original_row': row_for_day, 
                    'empid': empid,
                    'cid': cid,
                    'expanded_date': dt_to_str(curr)
                })
                curr += timedelta(days=1)

    expanded_header = extended_raw_header + ["Expanded_Date"]
    mapped_header = expanded_header + ["HR_Status", "GG_app_status"]

    f_expanded = io.StringIO()
    writer_exp = csv.writer(f_expanded)
    writer_exp.writerow(expanded_header)
    for c in expanded_claims:
        writer_exp.writerow(c['original_row'] + [c['expanded_date']])
    output_files["output_with_days_expanded.csv"] = f_expanded.getvalue()

    # --- STEP 2: Process HR File ---
    hr_data = {} 
    hr_reader = csv.reader(io.StringIO(hr_content))
    hr_header = next(hr_reader, [])
    date_headers = [parse_date(h) for h in hr_header[1:]]
    
    f_hr_out = io.StringIO()
    writer_hr = csv.writer(f_hr_out)
    writer_hr.writerow(["Employee Code", "Date", "Status"])
    
    for row in hr_reader:
        if not row or not row[0].strip(): continue
        empid = row[0].strip()
        for i, status in enumerate(row[1:]):
            status = status.strip()
            if status and i < len(date_headers) and date_headers[i]:
                dt_str = dt_to_str(date_headers[i])
                hr_data[(empid, dt_str)] = status
                writer_hr.writerow([empid, dt_str, status])
    output_files["HR_output.csv"] = f_hr_out.getvalue()

    # --- STEP 3: Mapping ---
    mapped_data = []
    f_mapped = io.StringIO()
    writer_mapped = csv.writer(f_mapped)
    writer_mapped.writerow(mapped_header)
    
    idx_entity_type = find_idx(extended_raw_header, ["entity_type"], -1)

    for c in expanded_claims:
        status = hr_data.get((c['empid'], c['expanded_date']), "NOT_FOUND")
        c['status'] = status
        
        # Populate GG_app_status conditionally
        entity_type_val = c['original_row'][idx_entity_type].strip() if idx_entity_type != -1 and idx_entity_type < len(c['original_row']) else ""
        
        if entity_type_val:
            gg_app_status = "Field activity during hospitalisation"
        else:
            gg_app_status = "No field activity during hospitalization"
            
        c['gg_app_status'] = gg_app_status 
        
        # Save the exact final row to memory so we can split it later
        c['final_out_row'] = c['original_row'] + [c['expanded_date'], status, gg_app_status]
        
        mapped_data.append(c)
        writer_mapped.writerow(c['final_out_row'])
        
    output_files["final_output.csv"] = f_mapped.getvalue()

    # --- STEP 4 & 5: Summary and Remarks ---
    summary_stats = defaultdict(lambda: {
        'original_row': [],
        'empid': "",
        'gg_app_status': "",
        'total': 0, 
        'statuses': defaultdict(int)
    })
    unique_statuses = set()

    for data in mapped_data:
        cid = data['cid']
        st_val = data['status']
        unique_statuses.add(st_val)
        
        if summary_stats[cid]['total'] == 0:
            summary_stats[cid]['original_row'] = data['original_row']
            summary_stats[cid]['empid'] = data['empid']
            summary_stats[cid]['gg_app_status'] = data['gg_app_status']
        
        summary_stats[cid]['total'] += 1
        summary_stats[cid]['statuses'][st_val] += 1

    unique_statuses = sorted(list(unique_statuses))
    
    f_summary = io.StringIO()
    f_final_sg = io.StringIO()
    
    writer_sum = csv.writer(f_summary)
    writer_sg = csv.writer(f_final_sg)
    
    header_base = extended_raw_header + ["Total_Expanded_Days"] + unique_statuses
    writer_sum.writerow(header_base)
    
    writer_sg.writerow(header_base + ["Remarks", "Location_Match", "GG_app_status"])

    dashboard_stats_1 = defaultdict(lambda: {
        'unique_empids': set(), 'hosp_days': 0, 'Present': 0, 'A': 0, 
        'CasualLeave': 0, 'PresentrivilegeLeave': 0, 'WO': 0, 'HD': 0, 'R': 0
    })
    
    dashboard_stats_2 = defaultdict(lambda: {
        'unique_empids': set(), 'hosp_days': 0, 'Present': 0, 'A': 0, 
        'CasualLeave': 0, 'PresentrivilegeLeave': 0, 'WO': 0, 'HD': 0, 'R': 0
    })

    # Keep track of mapping for splitting final_output rows
    cid_to_remark = {}

    for cid, data in summary_stats.items():
        total = data['total']
        
        # --- NEW LOGIC: Calculate Adjusted Hospitalized Days (Subtract 1) ---
        # Ensure we don't drop below 0 if a claim somehow only has 1 expanded day
        adjusted_hosp_days = max(0, total - 1)
        
        row_base = data['original_row'] + [adjusted_hosp_days]
        for st_val in unique_statuses:
            row_base.append(data['statuses'][st_val])
        
        writer_sum.writerow(row_base)

        P = data['statuses'].get('Present', 0)
        A = data['statuses'].get('A', 0)
        WO = data['statuses'].get('WO', 0)
        NF = data['statuses'].get('NOT_FOUND', 0)
        HD = data['statuses'].get('HD', 0)
        R = data['statuses'].get('R', 0)
        PL = data['statuses'].get('PresentrivilegeLeave', 0)
        CL = data['statuses'].get('CasualLeave', 0)
        
        other_except_WO = HD + R + PL + CL + NF
        other_except_NF = HD + R + PL + CL + WO
        other_except_PA = HD + R + PL + CL + WO + NF
        other_except_PL = HD + R + P + CL + WO + NF + A
        other_except_P = HD + R + PL + CL + NF + WO

        if total == 0: remark = "Attendance Not Available as per HR Record"
        elif P > 0 and A == 0 and other_except_WO == 0: remark = "Present for entire tenure of Hospitalization"
        elif P > 0 and other_except_WO > 0: remark = "Present for partial tenure during hospitalization"
        elif A > 0 and P == 0 and other_except_WO == 0: remark = "Absent During the Hospitalization"
        elif A > 0 and P > 0 and other_except_PA == 0: remark = "Absent During the Hospitalization"
        elif A > 0 and other_except_P > 0: remark = "Present for partial tenure during hospitalization"
        elif PL > 0 and other_except_PL == 0: remark = "Absent During the Hospitalization"
        elif P == 0 and A == 0 and NF == 0 and WO == 0 and total > 0: remark = "Attendance Not Available as per HR Record"
        elif NF > 0 and other_except_NF == 0 and total > 0: remark = "Attendance Not Available as per HR Record"
        else: remark = "Review"

        # Store for Splitting logic later
        cid_to_remark[cid] = remark

        # Populate Dashboard 1 Stats (using adjusted_hosp_days)
        dashboard_stats_1[remark]['unique_empids'].add(data['empid'])
        dashboard_stats_1[remark]['hosp_days'] += adjusted_hosp_days
        dashboard_stats_1[remark]['Present'] += P
        dashboard_stats_1[remark]['A'] += A
        dashboard_stats_1[remark]['CasualLeave'] += CL
        dashboard_stats_1[remark]['PresentrivilegeLeave'] += PL
        dashboard_stats_1[remark]['WO'] += WO
        dashboard_stats_1[remark]['HD'] += HD
        dashboard_stats_1[remark]['R'] += R
        
        # Populate Dashboard 2 Stats (using adjusted_hosp_days)
        gg_app_status = data['gg_app_status']
        key_2 = (gg_app_status, remark)
        dashboard_stats_2[key_2]['unique_empids'].add(data['empid'])
        dashboard_stats_2[key_2]['hosp_days'] += adjusted_hosp_days
        dashboard_stats_2[key_2]['Present'] += P
        dashboard_stats_2[key_2]['A'] += A
        dashboard_stats_2[key_2]['CasualLeave'] += CL
        dashboard_stats_2[key_2]['PresentrivilegeLeave'] += PL
        dashboard_stats_2[key_2]['WO'] += WO
        dashboard_stats_2[key_2]['HD'] += HD
        dashboard_stats_2[key_2]['R'] += R

        location_match = False
        if idx_emp_loc != -1 and idx_hosp_loc != -1:
            employee_actual_location = data['original_row'][idx_emp_loc]
            hospital_location = data['original_row'][idx_hosp_loc]
            if employee_actual_location and hospital_location:
                location_match = is_city_match(employee_actual_location, hospital_location)

        writer_sg.writerow(row_base + [remark, location_match, gg_app_status])

    output_files["final_summary.csv"] = f_summary.getvalue()
    output_files["final_SG_summary.csv"] = f_final_sg.getvalue()

    # --- STEP 6: Split `final_output` into smaller files based on Dashboards ---
    dash1_splits = defaultdict(list)
    dash2_splits = defaultdict(list)

    for c in mapped_data:
        cid = c['cid']
        remark = cid_to_remark.get(cid, "Review")
        gg_status = c['gg_app_status']
        
        # Split for Dashboard 1 (Remove invalid filename characters)
        safe_remark = re.sub(r'[\\/*?:"<>|]', "", remark).strip()
        dash1_splits[safe_remark].append(c['final_out_row'])
        
        # Split for Dashboard 2 
        if "No field activity" in gg_status:
            dash2_splits["No_field_activity"].append(c['final_out_row'])
        else:
            dash2_splits["Field_activity_present"].append(c['final_out_row'])

    # Save Dashboard 1 split files into Zip
    for rmk, rows in dash1_splits.items():
        f_out = io.StringIO()
        writer = csv.writer(f_out)
        writer.writerow(mapped_header)
        writer.writerows(rows)
        output_files[f"Dashboard1_{rmk}.csv"] = f_out.getvalue()
        
    # Save Dashboard 2 split files into Zip
    for d2_status, rows in dash2_splits.items():
        f_out = io.StringIO()
        writer = csv.writer(f_out)
        writer.writerow(mapped_header)
        writer.writerows(rows)
        output_files[f"Dashboard2_{d2_status}.csv"] = f_out.getvalue()


    # --- Formulate Dashboard 1 Table ---
    dashboard_table_1 = []
    all_unique_empids_1 = set()
    t_hosp_1 = t_p_1 = t_a_1 = t_cl_1 = t_pl_1 = t_wo_1 = t_hd_1 = t_r_1 = 0

    for rmk, stats in sorted(dashboard_stats_1.items()):
        all_unique_empids_1.update(stats['unique_empids'])
        t_hosp_1 += stats['hosp_days']
        t_p_1 += stats['Present']
        t_a_1 += stats['A']
        t_cl_1 += stats['CasualLeave']
        t_pl_1 += stats['PresentrivilegeLeave']
        t_wo_1 += stats['WO']
        t_hd_1 += stats['HD']
        t_r_1 += stats['R']
        
        dashboard_table_1.append({
            "Remarks": rmk,
            "Count of unique empid": len(stats['unique_empids']),
            "Number of Hospitised days": stats['hosp_days'],
            "Count of Present": stats['Present'],
            "Count of A": stats['A'],
            "Count of CasualLeave": stats['CasualLeave'],
            "Count of PresentrivilegeLeave": stats['PresentrivilegeLeave'],
            "Count of WO": stats['WO'],
            "Count of HD": stats['HD'],
            "Count of R": stats['R']
        })

    dashboard_table_1.append({
        "Remarks": "GRAND TOTAL",
        "Count of unique empid": len(all_unique_empids_1),
        "Number of Hospitised days": t_hosp_1,
        "Count of Present": t_p_1,
        "Count of A": t_a_1,
        "Count of CasualLeave": t_cl_1,
        "Count of PresentrivilegeLeave": t_pl_1,
        "Count of WO": t_wo_1,
        "Count of HD": t_hd_1,
        "Count of R": t_r_1
    })

    f_dash1 = io.StringIO()
    if dashboard_table_1:
        writer_dash1 = csv.DictWriter(f_dash1, fieldnames=dashboard_table_1[0].keys())
        writer_dash1.writeheader()
        writer_dash1.writerows(dashboard_table_1)
    output_files["Dashboard_1_Summary.csv"] = f_dash1.getvalue()
    
    # --- Formulate Dashboard 2 Table ---
    dashboard_table_2 = []
    all_unique_empids_2 = set()
    t_hosp_2 = t_p_2 = t_a_2 = t_cl_2 = t_pl_2 = t_wo_2 = t_hd_2 = t_r_2 = 0

    for (gg_status, rmk), stats in sorted(dashboard_stats_2.items()):
        all_unique_empids_2.update(stats['unique_empids'])
        t_hosp_2 += stats['hosp_days']
        t_p_2 += stats['Present']
        t_a_2 += stats['A']
        t_cl_2 += stats['CasualLeave']
        t_pl_2 += stats['PresentrivilegeLeave']
        t_wo_2 += stats['WO']
        t_hd_2 += stats['HD']
        t_r_2 += stats['R']
        
        dashboard_table_2.append({
            "GG_app_status": gg_status,
            "Remarks": rmk,
            "Count of unique empid": len(stats['unique_empids']),
            "Number of Hospitised days": stats['hosp_days'],
            "Count of Present": stats['Present'],
            "Count of A": stats['A'],
            "Count of CasualLeave": stats['CasualLeave'],
            "Count of PresentrivilegeLeave": stats['PresentrivilegeLeave'],
            "Count of WO": stats['WO'],
            "Count of HD": stats['HD'],
            "Count of R": stats['R']
        })

    dashboard_table_2.append({
        "GG_app_status": "GRAND TOTAL",
        "Remarks": "",
        "Count of unique empid": len(all_unique_empids_2),
        "Number of Hospitised days": t_hosp_2,
        "Count of Present": t_p_2,
        "Count of A": t_a_2,
        "Count of CasualLeave": t_cl_2,
        "Count of PresentrivilegeLeave": t_pl_2,
        "Count of WO": t_wo_2,
        "Count of HD": t_hd_2,
        "Count of R": t_r_2
    })
    
    f_dash2 = io.StringIO()
    if dashboard_table_2:
        writer_dash2 = csv.DictWriter(f_dash2, fieldnames=dashboard_table_2[0].keys())
        writer_dash2.writeheader()
        writer_dash2.writerows(dashboard_table_2)
    output_files["Dashboard_2_GG_app_HR_Status.csv"] = f_dash2.getvalue()

    # Package all files into a ZIP
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for file_name, file_content in output_files.items():
            zip_file.writestr(file_name, file_content)

    return zip_buffer.getvalue(), dashboard_table_1, dashboard_table_2, match_count, filtered_count

# --- Streamlit UI Components ---
st.markdown("<p style='text-align: center; color: #002b5b; font-weight: bold;'>FILE UPLOAD SECTION</p>", unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

with col1:
    claim_upload = st.file_uploader("1. Raw Claim File (CSV)", type=['csv', 'txt'])
with col2:
    hr_upload = st.file_uploader("2. HR Attendance (CSV)", type=['csv'])
with col3:
    gg_upload = st.file_uploader("3. GG App Data (CSV)", type=['csv'])

st.divider()

if st.button("Run Pipeline", type="primary", use_container_width=True):
    if not claim_upload or not hr_upload or not gg_upload:
        st.warning("⚠️ Please upload all three files before running the pipeline.")
    else:
        with st.status("Processing Pipeline...", expanded=True) as status:
            try:
                st.write("Processing data records...")
                zip_data, d_table_1, d_table_2, matches, filtered = process_data(claim_upload, hr_upload, gg_upload)
                status.update(label="✅ Processing Complete!", state="complete", expanded=False)
                
                st.success(f"Pipeline finished! Found **{matches}** matches with GG Data.")
                
                st.download_button(
                    label="📥 Download All Output Files (ZIP)",
                    data=zip_data,
                    file_name="Processed_Claims_Outputs.zip",
                    mime="application/zip",
                    type="primary"
                )
                
                st.divider()
                
                # --- DASHBOARD 1 RENDER ---
                st.markdown("<h3 style='color: #002b5b;'>📊 Dashboard_1_HR_Attendence</h3>", unsafe_allow_html=True)
                
                # 1. Plotly Pie Chart (Excluding Grand Total)
                df1 = pd.DataFrame([row for row in d_table_1 if row["Remarks"] != "GRAND TOTAL"])
                if not df1.empty:
                    fig1 = px.pie(
                        df1, 
                        names='Remarks', 
                        values='Count of unique empid', 
                        color_discrete_sequence=px.colors.qualitative.Pastel,
                        title='Employee Count Breakdown by Remark',
                        hole=0.4
                    )
                    fig1.update_traces(textposition='inside', textinfo='percent+label')
                    fig1.update_layout(plot_bgcolor="white", paper_bgcolor="white", font=dict(color="#002b5b"), showlegend=False)
                    st.plotly_chart(fig1, use_container_width=True)

                # 2. Data Table
                html_table_1 = generate_html_table(d_table_1)
                st.markdown(html_table_1, unsafe_allow_html=True)
                
                st.markdown("<br><br>", unsafe_allow_html=True)
                
                # --- DASHBOARD 2 RENDER ---
                st.markdown("<h3 style='color: #002b5b;'>📊 Dashboard_2_GG_app_HR_Status</h3>", unsafe_allow_html=True)
                
                # 1. Plotly Pie Chart (Excluding Grand Total)
                df2 = pd.DataFrame([row for row in d_table_2 if row["GG_app_status"] != "GRAND TOTAL"])
                if not df2.empty:
                    fig2 = px.pie(
                        df2, 
                        names='GG_app_status', 
                        values='Count of unique empid', 
                        color_discrete_sequence=px.colors.qualitative.Set2,
                        title='GG App Status Distribution',
                        hole=0.4
                    )
                    fig2.update_traces(textposition='inside', textinfo='percent+label')
                    fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white", font=dict(color="#002b5b"))
                    st.plotly_chart(fig2, use_container_width=True)

                # 2. Data Table
                html_table_2 = generate_html_table(d_table_2)
                st.markdown(html_table_2, unsafe_allow_html=True)
                
            except Exception as e:
                status.update(label="❌ Error during processing", state="error")
                st.error(f"An error occurred: {str(e)}")