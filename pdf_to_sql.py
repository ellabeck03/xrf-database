"""
pdf_to_sql.py

Converts XRF outputs into SQL entries.

Ella Beck
15/05/25
"""

import sqlite3
import pdfplumber
import logging
from pdfminer import settings as pdfminer_settings

# silence pdfminer/pdfplumber warnings
pdfminer_settings.STRICT = False
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)

DB_PATH  = "xrf_results.db"
PDF_PATH = "report.pdf"

def prompt_nullable(sample_name, field_name, cast=str):
    val = input(f"{sample_name}: Add value for {field_name} (leave blank for NULL): ").strip()
    return None if val == "" else cast(val)

def parse_page(page, cur, prompt_user):
    full_text = page.extract_text()
    lines = [L.strip() for L in full_text.splitlines() if L.strip()]

    # find header to extract sample name, date, duration
    for i, ln in enumerate(lines):
        if ln.startswith("Name") and "Duration" in ln:
            label_idx = i
            break
    else:
        print("  → 'Name ... Duration' not found; skipping page.")
        return False

    for j in range(label_idx+1, len(lines)):
        if lines[j].startswith("Element"):
            element_idx = j
            break
    else:
        print("  → 'Element' not found; skipping page.")
        return False

    #collect all text before sample results
    val_lines = lines[label_idx+1:element_idx]
    tokens = " ".join(val_lines).split()

    #find sample name by collecting everything before 'Class' entry
    CLASS_TOKEN = "Alloy_LE_FP"
    if CLASS_TOKEN not in tokens:
        print(f"  → Class token '{CLASS_TOKEN}' missing; skipping page.")
        return False
    ci = tokens.index(CLASS_TOKEN)

    #extract information
    sample_name = " ".join(tokens[:ci]) +" " + " ".join(tokens[ci+5:]) if len(tokens) > ci + 4 else " ".join(tokens[:ci])
    date        = tokens[ci+1]
    time_txt    = tokens[ci+2]
    duration    = float(tokens[ci+3])

    #ask user to provide additional information for Samples table
    mass_g      = prompt_nullable(sample_name, "Mass_g", float)
    length_mm   = prompt_nullable(sample_name, "Length_mm", float)
    width_mm    = prompt_nullable(sample_name, "Width_mm", float)
    height_mm   = prompt_nullable(sample_name, "Height_mm", float)
    notes       = prompt_nullable(sample_name, "Notes", str)
    description = prompt_nullable(sample_name, "Description", str)

    #preview to user, allow them to confirm information is correct
    print("\n=== Sample Preview ===")
    print(f"Name        : {sample_name}")
    print(f"Date        : {date}")
    print(f"Duration_s  : {duration}")
    print(f"Mass_g      : {mass_g}")
    print(f"Length_mm   : {length_mm}")
    print(f"Width_mm    : {width_mm}")
    print(f"Height_mm   : {height_mm}")
    print(f"Notes       : {notes}")
    print(f"Description : {description}")
    if prompt_user and input("Proceed with this sample? [y/N] ").strip().lower() != 'y':
        print("  → Sample skipped.")
        return False

    #insert information into Samples
    cur.execute("""
        INSERT INTO Samples
          (SampleName, CollectionDate, Mass_g, Length_mm, Width_mm, Height_mm, Notes, Description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (sample_name, date, mass_g, length_mm, width_mm, height_mm, notes, description))
    sample_id = cur.lastrowid

    #parse in measurements, skipping ND entries
    measurements = []

    for k in range(element_idx, len(lines)):
        if not lines[k].startswith("Element "):
            continue

        parts   = lines[k].split()
        symbols = [parts[m] for m in range(1, len(parts), 2)]
        pct_line   = lines[k+1].split()
        unc_tokens = lines[k+2].lstrip("± ").split()

        for sym, pct_txt, unc_txt in zip(symbols, pct_line, unc_tokens):
            if pct_txt.upper() == "ND":
                continue

            pct = float(pct_txt)
            unc = None if unc_txt.upper()=="ND" else float(unc_txt)
            measurements.append((sym, pct, unc, duration))

    #preview measurements
    print("\n=== Measurements Preview ===")
    for idx, (sym, pct, unc, mlen) in enumerate(measurements, start=1):
        print(f"{idx:2}: {sym:3} | %={pct:<6} | unc={unc:<6} | length_s={mlen}")

    if prompt_user and input("Proceed with measurements? [y/N] ").strip().lower() != 'y':
        print("  → Measurements skipped; rolling back sample.")
        return False

    #insert measurements
    for sym, pct, unc, mlen in measurements:
        cur.execute("SELECT ElementID FROM Elements WHERE Symbol=?", (sym,))
        r = cur.fetchone()
        if r:
            eid = r[0]
        else:
            raise Exception('ERROR: Element not found.')
    
        cur.execute("""
            INSERT INTO Measurements
              (SampleID, ElementID, Concentration, Uncertainty, Measurement_Length_s)
            VALUES (?, ?, ?, ?, ?)
        """, (sample_id, eid, pct, unc, mlen))

    print(f"  → Committed '{sample_name}' with {len(measurements)} measurements.")
    return True

def parse_to_pdf(pdf_path, db_path):
    #ask user which pages they want to process
    page_input = input("Enter pages/ranges (e.g. 1-3,5): ").strip()
    pages = set()

    for part in page_input.split(","):
        if "-" in part:
            a,b = map(int, part.split("-",1))
            pages.update(range(a, b+1))
        else:
            pages.add(int(part))
    pages = sorted(pages)

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    with pdfplumber.open(pdf_path) as pdf:
        for p in pages:
            if not (1 <= p <= len(pdf.pages)):
                print(f"Page {p} out of range; skipping.")
                continue
            conn.execute("BEGIN")
            try:
                ok = parse_page(pdf.pages[p-1], cur, prompt_user=True)
                if ok:
                    conn.commit()
                else:
                    conn.rollback()
            except Exception as e:
                conn.rollback()
                print(f"Error on page {p}: {e}")
    conn.close()

if __name__ == "__main__":
    parse_to_pdf(PDF_PATH, DB_PATH)
