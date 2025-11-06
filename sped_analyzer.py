"""
Unified SPED Auditor
====================

This module consolidates the functionality of multiple SPED audit scripts into a single
command‑line application.  It parses one or more SPED Fiscal (EFD ICMS/IPI) text files
and produces a comprehensive Excel workbook with a variety of audit reports.  Key
features include:

* Extraction of master data (company name, CNPJ, state, etc.) from the |0000|,
  |0002|, |0005|, |0015| and |0100| registers.
* Parsing of product definitions (|0200|) to map product codes to NCM codes and
  descriptions.
* Detailed item level breakdown of incoming (ind_oper = 0) invoices using
  |C170| records, including cross‑checks against a TIPI table for IPI rate
  conformity when provided.
* Summary of incoming items by CFOP and by NCM/CFOP.
* Detailed note level breakdown of outgoing (ind_oper = 1) invoices using
  |C190| records, including aggregated tax values and effective ICMS rates.
* Summary of outgoing invoices by CFOP and CST.
* Identification of outgoing invoices that are missing corresponding |C190|
  registers.
* Extraction of CT‑e data using |D100| and |D190| registers.
* Optional cross‑check against XML files (NFe and CTe) to validate values
  reported in the SPED files.
* Optional cross‑check of IPI rates against a TIPI table (CSV or XLSX).
* Computation of aggregated recoverable (credit) and payable (debit) ICMS/IPI
  balances per company and competence (month/year).
* Identification of CFOPs associated with immobilized assets and consumption
  (1556, 1407, 1551, 1406, 2551, 2556, 2406, 2407) and reporting of potential
  undue credits.
* Collection of adjustments (|C197|, |E111|, |E115|, |E116|) and summary of
  statutory blocks (E200, E300, E500) to support ST/DIFAL/IPI audits.

The script is designed to be run from the command line.  It requires at
least one SPED text file.  Optionally you may provide a directory of XML
files (containing NFe and CTe documents) and a TIPI table file for IPI
cross‑checks.  The resulting Excel file will contain multiple sheets with
the various reports.

Usage example (from a shell):

    python unified_auditor.py --sped /path/to/file1.txt /path/to/file2.txt \
        --tipi /path/to/tipi.xlsx --xml_dir /path/to/xmls \
        --output auditoria_resultado.xlsx

If the TIPI or XML arguments are omitted the corresponding cross‑checks are
skipped.  The script attempts to detect encodings automatically using
``chardet`` if available, falling back to latin‑1.

Note: This script focuses on core auditing logic and does not include any
graphical user interface.  It is intended for batch processing of SPED files.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Iterable

import pandas as pd

# Additional imports for optional GUIs and web interface.  These
# imports are isolated here to avoid hard dependencies when the
# respective front‑ends are not used.  ``tkinter`` is used for the
# desktop GUI and ``streamlit`` is used for the web interface.
try:
    import tkinter as _tk
    from tkinter import filedialog as _filedialog, messagebox as _messagebox
    import threading as _threading
except Exception:
    # If tkinter is unavailable (e.g. on headless servers) the GUI
    # functionality will simply not be exposed.
    _tk = None
    _filedialog = None
    _messagebox = None
    _threading = None

# We delay importing streamlit until the web app is actually invoked to
# avoid requiring it for command‑line usage.  The import is done inside
# ``run_streamlit_app``.


# ---------------------------------------------------------------------------
# Encoding detection
# ---------------------------------------------------------------------------

def detect_encoding(file_path: str) -> str:
    """Attempt to detect the character encoding of ``file_path``.

    If the optional ``chardet`` library is installed the function reads
    a sample of bytes from the file and uses ``chardet.detect`` to guess
    the encoding.  If chardet is unavailable or detection confidence is
    low, the function falls back to latin‑1 (ISO‑8859‑1).  This fallback
    is appropriate for most SPED Fiscal files generated in Brazil.

    Parameters
    ----------
    file_path : str
        Path to the SPED text file.

    Returns
    -------
    str
        The name of the detected encoding, defaulting to 'latin-1'.
    """
    try:
        import chardet  # type: ignore
    except ImportError:
        return 'latin-1'

    try:
        with open(file_path, 'rb') as f:
            raw = f.read(20000)
        result = chardet.detect(raw)
        enc = result.get('encoding') or 'latin-1'
        confidence = result.get('confidence', 0.0)
        # Many SPED files are latin‑1; treat ASCII or low confidence as latin‑1
        if confidence < 0.7 or enc.lower() in {'ascii'}:
            return 'latin-1'
        return enc
    except Exception:
        return 'latin-1'


# ---------------------------------------------------------------------------
# Helpers to normalise strings and parse floats
# ---------------------------------------------------------------------------

def norm_text(s: str) -> str:
    """Return a normalised version of a text string for comparisons.

    The returned string is lower‑cased and stripped of accents, punctuation
    and excess whitespace.  Useful for heuristic matching (e.g. in DIFAL
    detection).
    """
    if s is None:
        return ''
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ascii', 'ignore').decode('utf-8')
    s = s.lower()
    s = re.sub(r'[\s\./\-_,]+', ' ', s).strip()
    return s


def parse_float_br(value: str) -> float:
    """Parse a Brazilian formatted number to float.

    Handles numbers using either dot as thousand separator and comma as
    decimal separator (e.g. '1.234,56') or purely numeric strings.  If
    parsing fails returns 0.0.
    """
    if not value:
        return 0.0
    value = value.strip()
    if not value:
        return 0.0
    # Remove thousand separators and normalise decimal separator
    value = value.replace('.', '').replace(',', '.')
    try:
        return float(value)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# TIPI loading
# ---------------------------------------------------------------------------

def load_tipi_table(path: str) -> Dict[str, float]:
    """Load a TIPI table from a CSV or XLSX file.

    The TIPI (Tabela de Incidência do IPI) associates NCM codes to nominal
    IPI rates.  The table must contain columns labelled 'NCM' and
    'ALIQUOTA' (case‑insensitive, accents and punctuation ignored).  Any
    missing or invalid rows are skipped.  The returned dictionary maps
    8‑digit NCM strings to a floating point percentage (e.g. 5.0 for
    5%).

    Parameters
    ----------
    path : str
        Path to the TIPI file (.csv or .xlsx).

    Returns
    -------
    Dict[str, float]
        Mapping from NCM to IPI rate.
    """
    if not path:
        return {}
    if not os.path.isfile(path):
        raise FileNotFoundError(f"TIPI file not found: {path}")
    if path.lower().endswith('.xlsx'):
        df = pd.read_excel(path)
    elif path.lower().endswith('.csv'):
        df = pd.read_csv(path, sep=';', decimal=',')
    else:
        raise ValueError("Unsupported TIPI format; expected .csv or .xlsx")
    # Normalise column names
    cols = {}
    for col in df.columns:
        nc = unicodedata.normalize('NFKD', col)
        nc = nc.encode('ascii', 'ignore').decode('utf-8')
        nc = nc.upper().strip()
        nc = re.sub(r'[^A-Z0-9]', '', nc)
        cols[col] = nc
    df = df.rename(columns=cols)
    if 'NCM' not in df.columns or 'ALIQUOTA' not in df.columns:
        raise KeyError("TIPI file must contain 'NCM' and 'ALIQUOTA' columns")
    tipi_map: Dict[str, float] = {}
    for _, row in df.iterrows():
        ncm = str(row['NCM']).strip()
        if not ncm:
            continue
        try:
            # Allow comma or dot decimal separators
            aliquot = float(str(row['ALIQUOTA']).replace(',', '.'))
        except Exception:
            continue
        tipi_map[ncm] = aliquot
    return tipi_map


# ---------------------------------------------------------------------------
# XML parsing for NFe and CTe
# ---------------------------------------------------------------------------

def parse_xml_nfe(path: str) -> Optional[Dict[str, any]]:
    """Parse a single NFe XML file and extract totals and parties.

    Returns a dictionary keyed by:
        'Chave'               : access key (44 digits)
        'Valor ICMS XML'      : total ICMS value (float)
        'Valor IPI XML'       : total IPI value (float)
        'Valor Produtos XML'  : total products value (float)
        'Emitente XML'        : emitter name
        'CNPJ Emitente XML'   : emitter CNPJ
        'Destinatário XML'    : recipient name
        'CNPJ Destinatário XML': recipient CNPJ

    If parsing fails None is returned.
    """
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        data: Dict[str, any] = {}
        # Access key
        inf = root.find('.//nfe:infNFe', ns)
        if inf is not None:
            key = inf.get('Id')
            if key and key.startswith('NFe'):
                key = key[3:]
            data['Chave'] = key
        # Totals
        tot = root.find('.//nfe:ICMSTot', ns)
        if tot is not None:
            vICMS = tot.find('nfe:vICMS', ns)
            data['Valor ICMS XML'] = float(vICMS.text) if vICMS is not None and vICMS.text else 0.0
            vIPI = tot.find('nfe:vIPI', ns)
            data['Valor IPI XML'] = float(vIPI.text) if vIPI is not None and vIPI.text else 0.0
            vProd = tot.find('nfe:vProd', ns)
            data['Valor Produtos XML'] = float(vProd.text) if vProd is not None and vProd.text else 0.0
        # Parties
        emit = root.find('.//nfe:emit', ns)
        if emit is not None:
            data['Emitente XML'] = emit.find('nfe:xNome', ns).text if emit.find('nfe:xNome', ns) is not None else 'N/A'
            data['CNPJ Emitente XML'] = emit.find('nfe:CNPJ', ns).text if emit.find('nfe:CNPJ', ns) is not None else 'N/A'
        dest = root.find('.//nfe:dest', ns)
        if dest is not None:
            data['Destinatário XML'] = dest.find('nfe:xNome', ns).text if dest.find('nfe:xNome', ns) is not None else 'N/A'
            data['CNPJ Destinatário XML'] = dest.find('nfe:CNPJ', ns).text if dest.find('nfe:CNPJ', ns) is not None else 'N/A'
        return data if 'Chave' in data else None
    except Exception:
        return None


def parse_xml_cte(path: str) -> Optional[Dict[str, any]]:
    """Parse a single CTe XML file and extract totals and parties.

    Returns a dictionary keyed by:
        'Chave'                   : access key (CTe)
        'Valor Total Prestação XML': total freight value
        'BC ICMS XML'             : ICMS base
        'Valor ICMS XML'          : ICMS value
        'Alíquota ICMS XML'       : ICMS rate
        'CST XML'                 : CST code
        'Tipo Tomador XML'        : type of payer (Remetente, Destinatário, etc.)
        'Nome Tomador XML'        : name of payer
        'Emitente XML'            : emitter name
        'CNPJ Emitente XML'       : emitter CNPJ
        'Destinatário XML'        : recipient name
        'CNPJ Destinatário XML'   : recipient CNPJ

    If parsing fails None is returned.
    """
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        ns = {'cte': 'http://www.portalfiscal.inf.br/cte'}
        data: Dict[str, any] = {}
        infCte = root.find('.//cte:infCte', ns)
        if infCte is not None:
            key = infCte.get('Id')
            if key and key.startswith('CTe'):
                key = key[3:]
            data['Chave'] = key
        vPrest = root.find('.//cte:vPrest', ns)
        if vPrest is not None:
            v = vPrest.find('cte:vTPrest', ns)
            data['Valor Total Prestação XML'] = float(v.text) if v is not None and v.text else 0.0
        # ICMS
        icms = root.find('.//cte:ICMS/cte:ICMSOutraUF', ns)
        if icms is not None:
            data['BC ICMS XML'] = float(icms.find('cte:vBCOutraUF', ns).text) if icms.find('cte:vBCOutraUF', ns) is not None and icms.find('cte:vBCOutraUF', ns).text else 0.0
            data['Valor ICMS XML'] = float(icms.find('cte:vICMSOutraUF', ns).text) if icms.find('cte:vICMSOutraUF', ns) is not None and icms.find('cte:vICMSOutraUF', ns).text else 0.0
            data['Alíquota ICMS XML'] = float(icms.find('cte:pICMSOutraUF', ns).text) if icms.find('cte:pICMSOutraUF', ns) is not None and icms.find('cte:pICMSOutraUF', ns).text else 0.0
            cst = icms.find('cte:CST', ns)
            data['CST XML'] = cst.text if cst is not None else 'N/A'
        else:
            # Search for other ICMS types (00, 20, 90, etc.)
            icms_any = None
            for t in ['ICMS00', 'ICMS20', 'ICMS90', 'ICMS40', 'ICMS51', 'ICMS60', 'ICMS70', 'ICMSPart', 'ICMSST', 'ICMSCons', 'ICMSUFDest']:
                icms_any = root.find(f'.//cte:ICMS/cte:{t}', ns)
                if icms_any is not None:
                    break
            if icms_any is not None:
                data['BC ICMS XML'] = float(icms_any.find('cte:vBC', ns).text) if icms_any.find('cte:vBC', ns) is not None and icms_any.find('cte:vBC', ns).text else 0.0
                data['Valor ICMS XML'] = float(icms_any.find('cte:vICMS', ns).text) if icms_any.find('cte:vICMS', ns) is not None and icms_any.find('cte:vICMS', ns).text else 0.0
                data['Alíquota ICMS XML'] = float(icms_any.find('cte:pICMS', ns).text) if icms_any.find('cte:pICMS', ns) is not None and icms_any.find('cte:pICMS', ns).text else 0.0
                cst = icms_any.find('cte:CST', ns)
                data['CST XML'] = cst.text if cst is not None else 'N/A'
            else:
                data['BC ICMS XML'] = 0.0
                data['Valor ICMS XML'] = 0.0
                data['Alíquota ICMS XML'] = 0.0
                data['CST XML'] = 'N/A'
        # Tomador
        toma3 = root.find('.//cte:toma3/cte:toma', ns)
        toma_value = toma3.text if toma3 is not None else ''
        tomador_tipo = 'Não Identificado'
        tomador_nome = 'N/A'
        if toma_value == '0':
            tomador_tipo = 'Remetente'
            emit = root.find('.//cte:rem', ns)
            if emit is not None:
                nome = emit.find('cte:xNome', ns)
                tomador_nome = nome.text if nome is not None else 'N/A'
        elif toma_value == '1':
            tomador_tipo = 'Expedidor'
            exped = root.find('.//cte:exped', ns)
            if exped is not None:
                nome = exped.find('cte:xNome', ns)
                tomador_nome = nome.text if nome is not None else 'N/A'
        elif toma_value == '2':
            tomador_tipo = 'Recebedor'
            receb = root.find('.//cte:receb', ns)
            if receb is not None:
                nome = receb.find('cte:xNome', ns)
                tomador_nome = nome.text if nome is not None else 'N/A'
        elif toma_value == '3':
            tomador_tipo = 'Destinatário'
            dest = root.find('.//cte:dest', ns)
            if dest is not None:
                nome = dest.find('cte:xNome', ns)
                tomador_nome = nome.text if nome is not None else 'N/A'
        data['Tipo Tomador XML'] = tomador_tipo
        data['Nome Tomador XML'] = tomador_nome
        # Parties
        emit = root.find('.//cte:emit', ns)
        if emit is not None:
            data['Emitente XML'] = emit.find('cte:xNome', ns).text if emit.find('cte:xNome', ns) is not None else 'N/A'
            data['CNPJ Emitente XML'] = emit.find('cte:CNPJ', ns).text if emit.find('cte:CNPJ', ns) is not None else 'N/A'
        dest = root.find('.//cte:dest', ns)
        if dest is not None:
            data['Destinatário XML'] = dest.find('cte:xNome', ns).text if dest.find('cte:xNome', ns) is not None else 'N/A'
            data['CNPJ Destinatário XML'] = dest.find('cte:CNPJ', ns).text if dest.find('cte:CNPJ', ns) is not None else 'N/A'
        return data if 'Chave' in data else None
    except Exception:
        return None


def parse_xml_directory(directory: str) -> Dict[str, Dict[str, any]]:
    """Parse all XML files in a directory to build a map of access keys.

    The returned dictionary maps each access key (NF‑e or CT‑e) to the parsed
    data dictionary.  Files that cannot be parsed or that do not yield
    a key are skipped.  XML files may have .xml or .XML extension.
    """
    results: Dict[str, Dict[str, any]] = {}
    if not directory or not os.path.isdir(directory):
        return results
    for root_dir, _, files in os.walk(directory):
        for filename in files:
            if not filename.lower().endswith('.xml'):
                continue
            path = os.path.join(root_dir, filename)
            data = parse_xml_nfe(path)
            if data and 'Chave' in data:
                results[data['Chave']] = data
                continue
            data = parse_xml_cte(path)
            if data and 'Chave' in data:
                results[data['Chave']] = data
    return results


# ---------------------------------------------------------------------------
# SPED parsing
# ---------------------------------------------------------------------------

class SpedRecord:
    """Container for parsed SPED data for a single file."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.entries: List[dict] = []      # detailed C170 items for entries
        self.outputs: List[dict] = []      # detailed C190 summary rows for outputs
        self.imob_uso: List[dict] = []     # subset of entries for immobilised/consumption
        self.cte: List[dict] = []          # CT-e D190 rows
        self.adjustments: List[dict] = []  # list of adjustment records (C197/E111/E115/E116)
        self.st_blocks: List[dict] = []    # E200/E210 summaries
        self.difal_blocks: List[dict] = [] # E300/E310/E316 summaries
        self.ipi_blocks: List[dict] = []   # E500/E510 summaries
        self.master_data: dict = {}        # master data (0000/0002/0005/0015/0100)
        self.block_flags: dict = {}        # presence flags for ST/DIFAL/CIAP etc.
        self.missing_c190: List[dict] = [] # output notes missing C190
        self.parsing_warnings: List[str] = []
        # Detailed item list for both entries and outputs.  Each dict contains
        # information about an item (C170) along with ``Tipo Nota`` to
        # distinguish entries (Entrada) from outputs (Saída).  This enables
        # summarising tax burdens by NCM, CFOP and item across all notes.
        self.items: List[dict] = []

    def extend(self, other: 'SpedRecord') -> None:
        self.entries.extend(other.entries)
        self.outputs.extend(other.outputs)
        self.imob_uso.extend(other.imob_uso)
        self.cte.extend(other.cte)
        self.adjustments.extend(other.adjustments)
        self.st_blocks.extend(other.st_blocks)
        self.difal_blocks.extend(other.difal_blocks)
        self.ipi_blocks.extend(other.ipi_blocks)
        self.missing_c190.extend(other.missing_c190)
        self.parsing_warnings.extend(other.parsing_warnings)
        # Merge item records as well
        self.items.extend(other.items)
        # Master data and flags are per‑file; do not merge them here.


def parse_sped_file(file_path: str, xml_map: Dict[str, Dict[str, any]], tipi: Dict[str, float]) -> SpedRecord:
    """Parse a single SPED Fiscal file and return a ``SpedRecord``.

    The function reads the file line by line, identifies register types and
    populates structures on a ``SpedRecord`` instance.  It keeps track
    of the current document (C100), product definitions (0200), CT‑e
    documents (D100), adjustments, and summary blocks.  Items belonging
    to incoming invoices (entries) are recorded at item level with
    associated NCM codes, descriptions, CFOPs and tax values.  Outgoing
    invoices are recorded at summary level based on the |C190| register.

    If ``xml_map`` is provided, the function will cross‑check note keys
    (``Chave``) with the XML data and attach XML values to the record.
    If ``tipi`` is provided, the function will cross‑check IPI rates by
    NCM and record whether the reported IPI rate matches the TIPI table.
    """
    record = SpedRecord(file_path)
    encoding = detect_encoding(file_path)
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
            # Temporary maps and state
            ncm_map: Dict[str, str] = {}            # cod_item -> NCM
            desc_map: Dict[str, str] = {}           # cod_item -> description
            current_note: Optional[dict] = None
            current_note_key: Optional[str] = None
            current_note_is_entry: bool = False
            current_note_has_c190 = False
            current_cte: Optional[dict] = None
            # Track block contexts (E200/E210, E300/E310/E316, E500/E510)
            current_e200 = None
            current_e300 = None
            current_e310 = None
            current_e500 = None
            # Master data placeholders
            master = {
                'competence': '',
                'company_name': '',
                'company_cnpj': '',
                'company_ie': '',
                'company_cod_mun': '',
                'company_im': '',
                'company_profile': '',
                'company_status': '',
                'company_activity_type': '',
                'company_trade_name': '',
                'company_phone': '',
                'company_address': '',
                'company_number': '',
                'company_complement': '',
                'company_district': '',
                'company_email': '',
                'ie_substituted': [],
                'accountant_name': '',
                'accountant_cpf': '',
                'accountant_crc': '',
                'accountant_phone': '',
                'accountant_email': ''
            }
            # Flags for presence of certain registers
            block_flags = {
                'has_c100_saida': False,
                'has_st_cfop': False,
                'has_st_cfop_divergence': False,
                'has_block_e200': False,
                'has_difal_cfop': False,
                'has_block_e300': False,
                'has_block_g110': False
            }
            # For adjustments summary
            def add_adjustment(reg_type: str, code: str, descr: str, value: float, note_id: str | None = None):
                rec_adj = {
                    'Arquivo': os.path.basename(file_path),
                    'Competência': master['competence'],
                    'Tipo Registro': reg_type,
                    'Código Ajuste': code,
                    'Descrição Ajuste': descr,
                    'Valor Ajuste': value,
                    'Nota': note_id or ''
                }
                record.adjustments.append(rec_adj)

            for raw_line in f:
                line = raw_line.strip()
                if not line or '|' not in line:
                    continue
                parts = line.split('|')
                rec = parts[1] if len(parts) > 1 else ''
                # --- Master data registers ---
                if rec == '0000':
                    # |0000|COD_VER|COD_FIN|DT_INI|DT_FIN|NOME|CNPJ|CPF? etc ...|UF|IE|COD_MUN|IM|SUFRAMA|IND_PERFIL|IND_ATIV|
                    if len(parts) > 8:
                        # Determine competence from DT_INI (parts[3]) or fallback to DT_FIN (parts[4])
                        dt_ini = parts[3] if len(parts) > 3 else ''
                        dt_fin = parts[4] if len(parts) > 4 else ''
                        date_source = ''
                        # Prefer dt_ini if valid
                        if len(dt_ini) == 8 and dt_ini.isdigit():
                            date_source = dt_ini
                        elif len(dt_fin) == 8 and dt_fin.isdigit():
                            date_source = dt_fin
                        if date_source:
                            mes, ano = date_source[2:4], date_source[4:8]
                            master['competence'] = f"{mes}/{ano}"
                        master['company_name'] = parts[6].strip() if len(parts) > 6 else ''
                        master['company_cnpj'] = parts[7].strip() if len(parts) > 7 else ''
                        # IE can be in part[9] or part[10] depending on layout; try both
                        master['company_ie'] = parts[9].strip() if len(parts) > 9 else ''
                        master['company_cod_mun'] = parts[10].strip() if len(parts) > 10 else ''
                        master['company_im'] = parts[11].strip() if len(parts) > 11 else ''
                        master['company_profile'] = parts[14].strip() if len(parts) > 14 else ''
                        master['company_status'] = parts[15].strip() if len(parts) > 15 else ''
                elif rec == '0002':
                    if len(parts) > 2:
                        master['company_activity_type'] = parts[2].strip()
                elif rec == '0005':
                    # |0005|FANTASIA|TEL|EMAIL|END|NUM|COMP|BAIRRO|?
                    if len(parts) > 2:
                        master['company_trade_name'] = parts[2].strip()
                    if len(parts) > 3:
                        master['company_phone'] = parts[3].strip()
                    if len(parts) > 4:
                        master['company_address'] = parts[4].strip()
                    if len(parts) > 5:
                        master['company_number'] = parts[5].strip()
                    if len(parts) > 6:
                        master['company_complement'] = parts[6].strip()
                    if len(parts) > 7:
                        master['company_district'] = parts[7].strip()
                    if len(parts) > 10:
                        master['company_email'] = parts[10].strip()
                elif rec == '0015':
                    if len(parts) > 2:
                        ie_sub = parts[2].strip()
                        if ie_sub:
                            master['ie_substituted'].append(ie_sub)
                elif rec == '0100':
                    if len(parts) > 4:
                        master['accountant_name'] = parts[2].strip()
                        master['accountant_cpf'] = parts[3].strip() if len(parts) > 3 else ''
                        master['accountant_crc'] = parts[4].strip() if len(parts) > 4 else ''
                        master['accountant_phone'] = parts[11].strip() if len(parts) > 11 else ''
                        master['accountant_email'] = parts[13].strip() if len(parts) > 13 else ''
                # --- Product definitions ---
                if rec == '0200':
                    # |0200|REG|COD_ITEM|DESCR_ITEM|COD_BARRA|COD_ANT_ITEM|UNID_INV|TIPO_ITEM|COD_NCM|EX_IPI|COD_GEN|COD_LST|ALIQ_ICMS|
                    cod_item = parts[2].strip() if len(parts) > 2 else ''
                    descr_item = parts[3].strip() if len(parts) > 3 else ''
                    ncm = parts[8].strip() if len(parts) > 8 else ''
                    if cod_item:
                        if ncm:
                            ncm_map[cod_item] = ncm
                        if descr_item:
                            desc_map[cod_item] = descr_item
                # --- C100: document header ---
                if rec == 'C100':
                    # Flush previous note if it was a saída and had no C190
                    if current_note is not None and not current_note_is_entry and not current_note_has_c190:
                        # Record missing C190 note
                        record.missing_c190.append(current_note.copy())
                    # Reset for new note
                    current_note = None
                    current_note_key = None
                    current_note_is_entry = False
                    current_note_has_c190 = False
                    if len(parts) > 2:
                        ind_oper = parts[2].strip()
                        # Only track notes with ind_oper 0 (entrada) or 1 (saida)
                        if ind_oper == '0' or ind_oper == '1':
                            current_note_is_entry = (ind_oper == '0')
                            try:
                                serie = parts[7].strip() if len(parts) > 7 else ''
                                numero = parts[8].strip() if len(parts) > 8 else ''
                                chave = parts[9].strip() if len(parts) > 9 else ''
                                # Total note value: attempt fields[11] or [12]
                                vl_doc = 0.0
                                if len(parts) > 12 and parts[12].strip():
                                    vl_doc = parse_float_br(parts[12])
                                elif len(parts) > 11 and parts[11].strip():
                                    vl_doc = parse_float_br(parts[11])
                                # BC ICMS: try part[21] else [20]
                                bc_icms = 0.0
                                if len(parts) > 21 and parts[21].strip():
                                    bc_icms = parse_float_br(parts[21])
                                elif len(parts) > 20 and parts[20].strip():
                                    bc_icms = parse_float_br(parts[20])
                                # ICMS value: part[22] else [21]
                                vl_icms = 0.0
                                if len(parts) > 22 and parts[22].strip():
                                    vl_icms = parse_float_br(parts[22])
                                elif len(parts) > 21 and parts[21].strip():
                                    vl_icms = parse_float_br(parts[21])
                                # IPI value: part[25] else [24]
                                vl_ipi = 0.0
                                if len(parts) > 25 and parts[25].strip():
                                    vl_ipi = parse_float_br(parts[25])
                                elif len(parts) > 24 and parts[24].strip():
                                    vl_ipi = parse_float_br(parts[24])
                                current_note = {
                                    'Arquivo': os.path.basename(file_path),
                                    'Competência': master['competence'],
                                    'CNPJ': master['company_cnpj'],
                                    'Razão Social': master['company_name'],
                                    'UF': master['company_cod_mun'],
                                    'Série da nota': serie,
                                    'Número da nota': numero,
                                    'Chave': chave,
                                    'Data de emissão': parts[10].strip() if len(parts) > 10 else '',
                                    'Valor Total Nota': vl_doc,
                                    'BC ICMS Nota': bc_icms,
                                    'Valor ICMS Nota': vl_icms,
                                    'Valor IPI Nota': vl_ipi,
                                    'Tipo Nota': 'Entrada' if current_note_is_entry else 'Saída'
                                }
                                current_note_key = chave
                                if not current_note_is_entry:
                                    block_flags['has_c100_saida'] = True
                            except Exception:
                                current_note = None
                                current_note_key = None
                                current_note_is_entry = False
                                current_note_has_c190 = False
                # --- C170: item detail (for both entries and outputs) ---
                if rec == 'C170' and current_note is not None:
                    # We require at least up to field 24 for item level values to extract taxes
                    if len(parts) < 25:
                        continue
                    try:
                        num_item = parts[2].strip()
                        cod_item = parts[3].strip()
                        cfop = parts[11].strip() if len(parts) > 11 else ''
                        cst_icms = parts[10].strip() if len(parts) > 10 else ''
                        cst_ipi = parts[20].strip() if len(parts) > 20 else ''
                        val_item = parse_float_br(parts[7]) if len(parts) > 7 else 0.0
                        bc_icms_item = parse_float_br(parts[13]) if len(parts) > 13 else 0.0
                        aliq_icms_item = parse_float_br(parts[14]) if len(parts) > 14 else 0.0
                        vl_icms_item = parse_float_br(parts[15]) if len(parts) > 15 else 0.0
                        aliq_ipi_item = parse_float_br(parts[23]) if len(parts) > 23 else 0.0
                        vl_ipi_item = parse_float_br(parts[24]) if len(parts) > 24 else 0.0
                        eff_aliq = 0.0
                        if val_item > 0:
                            eff_aliq = (vl_icms_item / val_item) * 100.0
                        # Look up NCM and description
                        ncm = ncm_map.get(cod_item, '')
                        descr = desc_map.get(cod_item, '')
                        # Determine TIPI conformity
                        ipi_status = ''
                        if aliq_ipi_item == 0.0:
                            ipi_status = 'Conforme'
                        elif not tipi:
                            ipi_status = 'TIPI não carregada'
                        elif not ncm:
                            ipi_status = 'NCM não encontrado'
                        elif ncm not in tipi:
                            ipi_status = 'NCM não encontrado na TIPI'
                        else:
                            ipi_expected = tipi[ncm]
                            if abs(aliq_ipi_item - ipi_expected) < 0.001:
                                ipi_status = 'Conforme'
                            else:
                                ipi_status = f'Divergente (TIPI: {ipi_expected:.2f}%)'
                        # Build base item record from current_note and additional fields
                        item_rec = current_note.copy()
                        item_rec.update({
                            'Num. Item': num_item,
                            'Cód. Item': cod_item,
                            'Descrição do Produto': descr,
                            'CFOP': cfop,
                            'CST ICMS': cst_icms,
                            'CST IPI': cst_ipi,
                            'Valor Total Item': val_item,
                            'BC ICMS Item': bc_icms_item,
                            'Alíquota ICMS Item (%)': aliq_icms_item,
                            'Valor ICMS Item': vl_icms_item,
                            'Alíq. Efetiva (%)': eff_aliq,
                            'Alíquota IPI Item (%)': aliq_ipi_item,
                            'Valor IPI Item': vl_ipi_item,
                            'NCM Item': ncm,
                            'Conformidade IPI x TIPI': ipi_status
                        })
                        # Store the item for overall analysis
                        record.items.append(item_rec)
                        # If this note is an entry, include in entries list and check imobilizado/uso-consumo
                        if current_note_is_entry:
                            record.entries.append(item_rec)
                            # Capture immobilised/consumption items (specific CFOPs)
                            if cfop.replace('.', '') in {'1556', '1407', '1551', '1406', '2551', '2556', '2406', '2407'}:
                                uso_rec = item_rec.copy()
                                # Determine credit situation
                                if vl_icms_item > 0.001 or vl_ipi_item > 0.001:
                                    uso_rec['Situação Crédito'] = '❌ Inconsistente – Crédito indevido sobre Uso e Consumo'
                                else:
                                    uso_rec['Situação Crédito'] = '✅ Consistente – Nenhum Crédito indevido sobre Uso e Consumo'
                                record.imob_uso.append(uso_rec)
                        # End of entry-specific handling
                    except Exception:
                        # silently ignore errors for item lines
                        pass
                # --- C190: summary for outputs ---
                if rec == 'C190' and current_note is not None and not current_note_is_entry:
                    current_note_has_c190 = True
                    try:
                        cst_icms = parts[2].strip() if len(parts) > 2 else ''
                        cfop = parts[3].strip() if len(parts) > 3 else ''
                        aliq_icms = parse_float_br(parts[4]) if len(parts) > 4 else 0.0
                        vl_opr = parse_float_br(parts[5]) if len(parts) > 5 else 0.0
                        bc_icms = parse_float_br(parts[6]) if len(parts) > 6 else 0.0
                        vl_icms = parse_float_br(parts[7]) if len(parts) > 7 else 0.0
                        vl_ipi = parse_float_br(parts[11]) if len(parts) > 11 else 0.0
                        eff_aliq = 0.0
                        if bc_icms > 0:
                            eff_aliq = (vl_icms / bc_icms) * 100.0
                        out_rec = current_note.copy()
                        out_rec.update({
                            'CST ICMS': cst_icms,
                            'CFOP': cfop,
                            'Alíquota ICMS': aliq_icms,
                            'Valor Operação': vl_opr,
                            'BC ICMS': bc_icms,
                            'Valor ICMS': vl_icms,
                            'Alíq. Efetiva (%)': eff_aliq,
                            'Valor IPI (C190)': vl_ipi
                        })
                        record.outputs.append(out_rec)
                        # Mark ST/DIFAL flags based on CFOP
                        if cfop.replace('.', '') in {'5401', '5403', '5405', '6401', '6403'}:
                            block_flags['has_st_cfop'] = True
                        if cfop.replace('.', '') in {'5401', '5403', '6403'}:
                            block_flags['has_st_cfop_divergence'] = True
                        if cfop.replace('.', '') in {'6107', '6108'}:
                            block_flags['has_difal_cfop'] = True
                    except Exception:
                        pass
                # --- D100/D190: CT‑e parsing ---
                if rec == 'D100':
                    current_cte = None
                    try:
                        serie = parts[7].strip() if len(parts) > 7 else ''
                        numero = parts[9].strip() if len(parts) > 9 else ''
                        chave = parts[10].strip() if len(parts) > 10 else ''
                        vl_total = parse_float_br(parts[15]) if len(parts) > 15 else 0.0
                        bc_icms_cte = parse_float_br(parts[18]) if len(parts) > 18 else 0.0
                        vl_icms_cte = parse_float_br(parts[20]) if len(parts) > 20 else 0.0
                        current_cte = {
                            'Arquivo': os.path.basename(file_path),
                            'Competência': master['competence'],
                            'Chave CT-e': chave,
                            'Série CT-e': serie,
                            'Número CT-e': numero,
                            'Data de emissão': parts[11].strip() if len(parts) > 11 else '',
                            'Valor Total CT-e': vl_total,
                            'BC ICMS CT-e': bc_icms_cte,
                            'Valor ICMS CT-e': vl_icms_cte
                        }
                    except Exception:
                        current_cte = None
                if rec == 'D190' and current_cte is not None:
                    try:
                        cst_cte = parts[2].strip() if len(parts) > 2 else ''
                        cfop = parts[3].strip() if len(parts) > 3 else ''
                        aliq = parse_float_br(parts[4]) if len(parts) > 4 else 0.0
                        vl_opr = parse_float_br(parts[5]) if len(parts) > 5 else 0.0
                        bc_icms = parse_float_br(parts[6]) if len(parts) > 6 else 0.0
                        vl_icms = parse_float_br(parts[7]) if len(parts) > 7 else 0.0
                        eff_aliq = 0.0
                        if vl_opr > 0:
                            eff_aliq = (vl_icms / vl_opr) * 100.0
                        cte_rec = current_cte.copy()
                        cte_rec.update({
                            'CST CT-e': cst_cte,
                            'CFOP CT-e': cfop,
                            'Alíquota ICMS CT-e': aliq,
                            'Valor Operação CT-e': vl_opr,
                            'BC ICMS CT-e (D190)': bc_icms,
                            'Valor ICMS CT-e (D190)': vl_icms,
                            'Alíq. Efetiva CT-e (%)': eff_aliq,
                            'Valor IPI CT-e': 0.0,
                            'NCM Item': 'Não se Aplica',
                            'Descrição do Produto': 'Serviço de Transporte'
                        })
                        record.cte.append(cte_rec)
                    except Exception:
                        pass
                # --- C195/C197: Observations and adjustments per doc ---
                if rec == 'C195' and current_note is not None:
                    # |C195|COD_OBS|TXT_COMPL|
                    # We'll treat observations containing certain keywords as DIFAL evidence
                    txt = parts[3].strip() if len(parts) > 3 else ''
                    if txt:
                        record.adjustments.append({
                            'Arquivo': os.path.basename(file_path),
                            'Competência': master['competence'],
                            'Tipo Registro': 'C195',
                            'Código Ajuste': '',
                            'Descrição Ajuste': txt,
                            'Valor Ajuste': 0.0,
                            'Nota': current_note_key or ''
                        })
                if rec == 'C197' and current_note is not None:
                    # |C197|COD_AJ|DESCR_COMPL_AJ|...|VL_AJ| ...
                    code = parts[2].strip() if len(parts) > 2 else ''
                    descr = parts[3].strip() if len(parts) > 3 else ''
                    # Find last numeric value in the rest of the fields as adjustment value
                    adj_value = 0.0
                    for item in parts[4:]:
                        v = parse_float_br(item)
                        if v > 0:
                            adj_value = v
                    add_adjustment('C197', code, descr, adj_value, current_note_key)
                # --- E111/E115/E116: Adjustments per period ---
                if rec == 'E111':
                    code = parts[2].strip() if len(parts) > 2 else ''
                    descr = parts[3].strip() if len(parts) > 3 else ''
                    value = parse_float_br(parts[4]) if len(parts) > 4 else 0.0
                    add_adjustment('E111', code, descr, value)
                if rec == 'E115':
                    code = parts[2].strip() if len(parts) > 2 else ''
                    value = parse_float_br(parts[3]) if len(parts) > 3 else 0.0
                    descr = parts[4].strip() if len(parts) > 4 else ''
                    add_adjustment('E115', code, descr, value)
                if rec == 'E116':
                    # |E116|COD_OR|VL_OR|DT_VCTO|COD_REC|NUM_PROC|IND_PROC|PROC|TXT_COMPL|
                    cod_or = parts[2].strip() if len(parts) > 2 else ''
                    value = parse_float_br(parts[3]) if len(parts) > 3 else 0.0
                    cod_rec = parts[5].strip() if len(parts) > 5 else ''
                    txt = parts[9].strip() if len(parts) > 9 else ''
                    descr = f"{cod_or} {cod_rec} {txt}".strip()
                    add_adjustment('E116', cod_rec or cod_or, descr, value)
                # --- Block presence trackers ---
                if rec.startswith('E2'):
                    block_flags['has_block_e200'] = True
                if rec.startswith('E3'):
                    block_flags['has_block_e300'] = True
                if rec == 'G110':
                    block_flags['has_block_g110'] = True
                # --- E200/E210 summaries ---
                if rec == 'E200':
                    # |E200|REG|UF|DT_INI|DT_FIN|IND_MOV|
                    if len(parts) > 4:
                        current_e200 = {
                            'Arquivo': os.path.basename(file_path),
                            'Competência': master['competence'],
                            'UF': parts[2].strip() if len(parts) > 2 else '',
                            'Data Início': parts[3].strip() if len(parts) > 3 else '',
                            'Data Fim': parts[4].strip() if len(parts) > 4 else '',
                            'Ind Mov': ''
                        }
                if rec == 'E210' and current_e200 is not None:
                    # |E210|REG|IND_MOV_ST|
                    current_e200['Ind Mov'] = parts[2].strip() if len(parts) > 2 else ''
                    record.st_blocks.append(current_e200.copy())
                # --- E300/E310/E316 summaries ---
                if rec == 'E300':
                    # |E300|REG|UF|DT_INI|DT_FIN|IND_MOV_DIFAL|
                    if len(parts) > 4:
                        current_e300 = {
                            'Arquivo': os.path.basename(file_path),
                            'Competência': master['competence'],
                            'UF': parts[2].strip() if len(parts) > 2 else '',
                            'Data Início': parts[3].strip() if len(parts) > 3 else '',
                            'Data Fim': parts[4].strip() if len(parts) > 4 else '',
                            'Ind Mov': ''
                        }
                        current_e310 = None
                if rec == 'E310' and current_e300 is not None:
                    # |E310|REG|IND_MOV|...|VL_SLD_APURADO|
                    current_e300['Ind Mov'] = parts[2].strip() if len(parts) > 2 else ''
                    vl_apur = parse_float_br(parts[9]) if len(parts) > 9 else 0.0
                    current_e310 = current_e300.copy()
                    current_e310['Saldo Apurado'] = vl_apur
                if rec == 'E316' and current_e310 is not None:
                    # |E316|REG|COD_REC|VL_RECOL|DT_RECOL|NUM_PROC|IND_PROC|PROC|TXT_COMPL|MES_REF|
                    cod_rec_e316 = parts[2].strip() if len(parts) > 2 else ''
                    vl_recol = parse_float_br(parts[3]) if len(parts) > 3 else 0.0
                    dt_recol = parts[4].strip() if len(parts) > 4 else ''
                    current_e310['Código Receita'] = cod_rec_e316
                    current_e310['Valor Recolhimento'] = vl_recol
                    current_e310['Data Recolhimento'] = dt_recol
                    record.difal_blocks.append(current_e310.copy())
                # --- E500/E510 summaries ---
                if rec == 'E500':
                    # |E500|REG|IND_APUR|DT_INI|DT_FIN|
                    current_e500 = {
                        'Arquivo': os.path.basename(file_path),
                        'Competência': master['competence'],
                        'Ind Apur': parts[2].strip() if len(parts) > 2 else '',
                        'Data Início': parts[3].strip() if len(parts) > 3 else '',
                        'Data Fim': parts[4].strip() if len(parts) > 4 else ''
                    }
                if rec == 'E510' and current_e500 is not None:
                    # |E510|REG|CFOP|CST_IPI|VL_CONT_IPI|VL_BC_IPI|VL_IPI|
                    cfop = parts[2].strip() if len(parts) > 2 else ''
                    cst = parts[3].strip() if len(parts) > 3 else ''
                    vl_cont = parse_float_br(parts[4]) if len(parts) > 4 else 0.0
                    vl_bc = parse_float_br(parts[5]) if len(parts) > 5 else 0.0
                    vl_ipi = parse_float_br(parts[6]) if len(parts) > 6 else 0.0
                    rec_e500 = current_e500.copy()
                    rec_e500.update({
                        'CFOP': cfop,
                        'CST IPI': cst,
                        'Valor Contábil IPI': vl_cont,
                        'Base IPI': vl_bc,
                        'Valor IPI': vl_ipi
                    })
                    record.ipi_blocks.append(rec_e500)
            # End for each line

        # After processing all lines, flush pending missing C190 note
        if current_note is not None and not current_note_is_entry and not current_note_has_c190:
            record.missing_c190.append(current_note.copy())

        # Save master data and flags
        record.master_data = master.copy()
        record.block_flags = block_flags.copy()

        # Attach XML cross‑check values to entries and outputs if available
        if xml_map:
            for item in record.entries:
                key = item.get('Chave')
                if key and key in xml_map:
                    xml_vals = xml_map[key]
                    item['Valor ICMS XML'] = xml_vals.get('Valor ICMS XML', 0.0)
                    item['Valor IPI XML'] = xml_vals.get('Valor IPI XML', 0.0)
                    item['Valor Produtos XML'] = xml_vals.get('Valor Produtos XML', 0.0)
            for out in record.outputs:
                key = out.get('Chave')
                if key and key in xml_map:
                    xml_vals = xml_map[key]
                    out['Valor ICMS XML'] = xml_vals.get('Valor ICMS XML', 0.0)
                    out['Valor IPI XML'] = xml_vals.get('Valor IPI XML', 0.0)
                    out['Valor Produtos XML'] = xml_vals.get('Valor Produtos XML', 0.0)
    except Exception as exc:
        record.parsing_warnings.append(f"Erro ao processar {os.path.basename(file_path)}: {exc}")
    return record


# ---------------------------------------------------------------------------
# Aggregation and reporting
# ---------------------------------------------------------------------------

def aggregate_records(records: List[SpedRecord]):
    """Aggregate multiple ``SpedRecord`` objects into summary DataFrames.

    Parameters
    ----------
    records : List[SpedRecord]
        List of SpedRecord instances to aggregate.

    Returns
    -------
    Dict[str, pd.DataFrame]
        A dictionary mapping sheet names to DataFrames for output.
    """
    # Concatenate detailed lists into DataFrames
    df_entries = pd.DataFrame([row for rec in records for row in rec.entries])
    df_outputs = pd.DataFrame([row for rec in records for row in rec.outputs])
    # All items (entries and outputs) are available via the ``items`` list
    df_items = pd.DataFrame([row for rec in records for row in getattr(rec, 'items', [])])
    df_imob = pd.DataFrame([row for rec in records for row in rec.imob_uso])
    df_cte = pd.DataFrame([row for rec in records for row in rec.cte])
    df_adjustments = pd.DataFrame([row for rec in records for row in rec.adjustments])
    df_st_blocks = pd.DataFrame([row for rec in records for row in rec.st_blocks])
    df_difal_blocks = pd.DataFrame([row for rec in records for row in rec.difal_blocks])
    df_ipi_blocks = pd.DataFrame([row for rec in records for row in rec.ipi_blocks])
    df_missing_c190 = pd.DataFrame([row for rec in records for row in rec.missing_c190])
    # Master data per file
    df_master = pd.DataFrame([rec.master_data for rec in records])
    # Flags per file
    df_flags = pd.DataFrame([rec.block_flags for rec in records])

    sheets: Dict[str, pd.DataFrame] = {}
    # Add detailed items sheet (both entries and outputs) and corresponding summary
    if not df_items.empty:
        # Ensure numeric columns are numeric for aggregation
        numeric_cols_items = ['Valor Total Item', 'BC ICMS Item', 'Valor ICMS Item', 'Valor IPI Item']
        for col in numeric_cols_items:
            if col in df_items.columns:
                df_items[col] = pd.to_numeric(df_items[col], errors='coerce').fillna(0.0)
        sheets['Detalhes Itens'] = df_items
        # Summary by Tipo Nota, Competência, CNPJ, Razão Social, NCM Item and CFOP
        # Include company identifiers in the grouping to support multi‑company analyses.
        grp_cols = []
        for c in ['Tipo Nota', 'Competência', 'CNPJ', 'Razão Social', 'NCM Item', 'CFOP']:
            if c in df_items.columns:
                grp_cols.append(c)
        if grp_cols:
            agg_cols = {c: 'sum' for c in numeric_cols_items if c in df_items.columns}
            df_items_sum = df_items.groupby(grp_cols).agg(agg_cols).reset_index()
            # Rename for clarity
            rename_map_items = {
                'Valor Total Item': 'Valor Contábil',
                'BC ICMS Item': 'BC ICMS',
                'Valor ICMS Item': 'ICMS',
                'Valor IPI Item': 'IPI'
            }
            df_items_sum = df_items_sum.rename(columns={k: v for k, v in rename_map_items.items() if k in df_items_sum.columns})
            sheets['Resumo Itens por NCM-CFOP'] = df_items_sum

        # Additional summary: group by CFOP, NCM and CST to provide a macro view
        # of CFOP per NCM and CST with total and tax values, including company identifiers.
        cfop_ncm_cst_cols = []
        # Define grouping columns: Tipo Nota, Competência, CNPJ, Razão Social, CFOP, NCM Item and CST ICMS
        for c in ['Tipo Nota', 'Competência', 'CNPJ', 'Razão Social', 'CFOP', 'NCM Item', 'CST ICMS']:
            if c in df_items.columns:
                cfop_ncm_cst_cols.append(c)
        if cfop_ncm_cst_cols:
            agg_cols_cfop_ncm_cst = {c: 'sum' for c in numeric_cols_items if c in df_items.columns}
            df_cfop_ncm_cst = df_items.groupby(cfop_ncm_cst_cols).agg(agg_cols_cfop_ncm_cst).reset_index()
            # Rename numeric columns for clarity
            df_cfop_ncm_cst = df_cfop_ncm_cst.rename(columns={k: v for k, v in rename_map_items.items() if k in df_cfop_ncm_cst.columns})
            sheets['Resumo CFOP-NCM-CST'] = df_cfop_ncm_cst

        # -------------------------------------------------------------------
        # Ranking and additional summaries for BI
        # These summaries include company identifiers (CNPJ, Razão Social) and
        # competence to facilitate multi‑company dashboards.  Rankings are
        # generated by aggregating item values across descriptions, NCMs,
        # CFOPs and UF (state).
        #
        # Ranking by product description, NCM and CFOP
        rank_cols = []
        for c in ['Competência', 'CNPJ', 'Razão Social', 'Descrição do Produto', 'NCM Item', 'CFOP']:
            if c in df_items.columns:
                rank_cols.append(c)
        if rank_cols:
            agg_cols_rank = {c: 'sum' for c in numeric_cols_items if c in df_items.columns}
            df_rank = df_items.groupby(rank_cols).agg(agg_cols_rank).reset_index()
            # Rename numeric columns
            df_rank = df_rank.rename(columns={k: v for k, v in rename_map_items.items() if k in df_rank.columns})
            # Order by value descending for ranking – optional sort
            if 'Valor Contábil' in df_rank.columns:
                df_rank = df_rank.sort_values(by='Valor Contábil', ascending=False)
            sheets['Ranking Produtos'] = df_rank

        # Summary by state (UF) and CFOP
        uf_cfop_cols = []
        for c in ['Competência', 'CNPJ', 'Razão Social', 'UF', 'CFOP']:
            if c in df_items.columns:
                uf_cfop_cols.append(c)
        if uf_cfop_cols:
            agg_cols_uf = {c: 'sum' for c in numeric_cols_items if c in df_items.columns}
            df_uf_cfop = df_items.groupby(uf_cfop_cols).agg(agg_cols_uf).reset_index()
            df_uf_cfop = df_uf_cfop.rename(columns={k: v for k, v in rename_map_items.items() if k in df_uf_cfop.columns})
            sheets['Resumo UF-CFOP'] = df_uf_cfop

        # Summary by NCM only (useful for high‑level ranking)
        ncm_cols = []
        for c in ['Competência', 'CNPJ', 'Razão Social', 'NCM Item']:
            if c in df_items.columns:
                ncm_cols.append(c)
        if ncm_cols:
            agg_cols_ncm = {c: 'sum' for c in numeric_cols_items if c in df_items.columns}
            df_ncm = df_items.groupby(ncm_cols).agg(agg_cols_ncm).reset_index()
            df_ncm = df_ncm.rename(columns={k: v for k, v in rename_map_items.items() if k in df_ncm.columns})
            sheets['Resumo NCM'] = df_ncm
    if not df_entries.empty:
        sheets['Detalhes Entradas'] = df_entries
        # Summary by CFOP (entries) including company identifiers
        if 'CFOP' in df_entries.columns:
            sum_cols = ['Valor Total Item', 'BC ICMS Item', 'Valor ICMS Item', 'Valor IPI Item']
            # Ensure numeric types
            for col in sum_cols:
                if col in df_entries.columns:
                    df_entries[col] = pd.to_numeric(df_entries[col], errors='coerce').fillna(0.0)
            # Group by competence, company and CFOP
            grp_cols_cfop = []
            for c in ['Competência', 'CNPJ', 'Razão Social', 'CFOP']:
                if c in df_entries.columns:
                    grp_cols_cfop.append(c)
            if grp_cols_cfop:
                df_cfop = df_entries.groupby(grp_cols_cfop).agg({c: 'sum' for c in sum_cols}).reset_index()
            else:
                df_cfop = df_entries.groupby(['Competência', 'CFOP']).agg({c: 'sum' for c in sum_cols}).reset_index()
            df_cfop = df_cfop.rename(columns={
                'Valor Total Item': 'Valor Contábil',
                'BC ICMS Item': 'BC ICMS',
                'Valor ICMS Item': 'ICMS',
                'Valor IPI Item': 'IPI'
            })
            sheets['Resumo Entradas por CFOP'] = df_cfop
            # Summary by NCM and CFOP including company identifiers
            if 'NCM Item' in df_entries.columns:
                grp_cols_ncm_cfop = []
                for c in ['Competência', 'CNPJ', 'Razão Social', 'NCM Item', 'CFOP']:
                    if c in df_entries.columns:
                        grp_cols_ncm_cfop.append(c)
                if grp_cols_ncm_cfop:
                    df_ncm_cfop = df_entries.groupby(grp_cols_ncm_cfop).agg({c: 'sum' for c in sum_cols}).reset_index()
                else:
                    df_ncm_cfop = df_entries.groupby(['Competência', 'NCM Item', 'CFOP']).agg({c: 'sum' for c in sum_cols}).reset_index()
                df_ncm_cfop = df_ncm_cfop.rename(columns={
                    'Valor Total Item': 'Valor Contábil',
                    'BC ICMS Item': 'BC ICMS',
                    'Valor ICMS Item': 'ICMS',
                    'Valor IPI Item': 'IPI'
                })
                sheets['Resumo Entradas por NCM-CFOP'] = df_ncm_cfop
    if not df_outputs.empty:
        sheets['Detalhes Saídas'] = df_outputs
        # Summary by CFOP and CST (outputs), including company identifiers
        sum_cols_out = ['Valor Total Nota', 'BC ICMS', 'Valor ICMS', 'Valor IPI Nota']
        for col in sum_cols_out:
            if col in df_outputs.columns:
                df_outputs[col] = pd.to_numeric(df_outputs[col], errors='coerce').fillna(0.0)
        grp_cols_saidas = []
        for c in ['Competência', 'CNPJ', 'Razão Social', 'CFOP', 'CST ICMS']:
            if c in df_outputs.columns:
                grp_cols_saidas.append(c)
        if grp_cols_saidas:
            df_saidas = df_outputs.groupby(grp_cols_saidas).agg({
                'Valor Total Nota': 'sum',
                'BC ICMS': 'sum',
                'Valor ICMS': 'sum',
                'Valor IPI Nota': 'sum'
            }).reset_index()
        else:
            df_saidas = df_outputs.groupby(['Competência', 'CFOP', 'CST ICMS']).agg({
                'Valor Total Nota': 'sum',
                'BC ICMS': 'sum',
                'Valor ICMS': 'sum',
                'Valor IPI Nota': 'sum'
            }).reset_index()
        df_saidas = df_saidas.rename(columns={
            'Valor Total Nota': 'Valor Contábil',
            'BC ICMS': 'BC ICMS',
            'Valor ICMS': 'ICMS',
            'Valor IPI Nota': 'IPI'
        })
        sheets['Resumo Saídas por CFOP-CST'] = df_saidas
    if not df_imob.empty:
        sheets['Entradas Imob_UsoConsumo'] = df_imob
    if not df_cte.empty:
        sheets['Detalhes CT-e'] = df_cte
        # Summary CT-e by CFOP and CST including company identifiers
        sum_cols_cte = ['Valor Operação CT-e', 'BC ICMS CT-e (D190)', 'Valor ICMS CT-e (D190)']
        for col in sum_cols_cte:
            if col in df_cte.columns:
                df_cte[col] = pd.to_numeric(df_cte[col], errors='coerce').fillna(0.0)
        grp_cols_cte = []
        for c in ['Competência', 'CNPJ', 'Razão Social', 'CFOP CT-e', 'CST CT-e']:
            if c in df_cte.columns:
                grp_cols_cte.append(c)
        if grp_cols_cte:
            df_cte_sum = df_cte.groupby(grp_cols_cte).agg({
                'Valor Operação CT-e': 'sum',
                'BC ICMS CT-e (D190)': 'sum',
                'Valor ICMS CT-e (D190)': 'sum'
            }).reset_index()
        else:
            df_cte_sum = df_cte.groupby(['Competência', 'CFOP CT-e', 'CST CT-e']).agg({
                'Valor Operação CT-e': 'sum',
                'BC ICMS CT-e (D190)': 'sum',
                'Valor ICMS CT-e (D190)': 'sum'
            }).reset_index()
        df_cte_sum = df_cte_sum.rename(columns={
            'Valor Operação CT-e': 'Valor Contábil',
            'BC ICMS CT-e (D190)': 'BC ICMS',
            'Valor ICMS CT-e (D190)': 'ICMS'
        })
        sheets['Resumo CT-e por CFOP-CST'] = df_cte_sum
    if not df_adjustments.empty:
        sheets['Ajustes'] = df_adjustments
    # When checking if a DataFrame is empty, use the ``empty`` property rather than
    # calling it as a function. Calling ``empty()`` attempts to invoke a boolean
    # and will raise ``TypeError: 'bool' object is not callable``.  See pandas docs.
    if not df_st_blocks.empty:
        sheets['Resumo E200_ICMS_ST'] = df_st_blocks
    if not df_difal_blocks.empty:
        sheets['Resumo E300_DIFAL'] = df_difal_blocks
    if not df_ipi_blocks.empty:
        sheets['Resumo E500_IPI'] = df_ipi_blocks
    # Use the ``empty`` property to check DataFrame emptiness without calling it
    if not df_missing_c190.empty:
        sheets['Notas Saída sem C190'] = df_missing_c190
    if not df_master.empty:
        sheets['Dados Mestres'] = df_master
    if not df_flags.empty:
        sheets['Presença Blocos'] = df_flags

    # -------------------------------------------------------------------
    # Fiscal DRE summary (Receita, Outras Saídas, Custos, Despesas)
    # -------------------------------------------------------------------
    # Many accounting users need a high‑level view of their fiscal data.  A
    # simplified DRE (Demonstração do Resultado do Exercício) can be
    # constructed directly from the SPED by mapping certain CFOP codes to
    # broad categories.  The following implementation creates a summary
    # grouping operations into Revenue (faturamento), Other Outputs,
    # Costs, and Expenses.  Each category aggregates the value of the
    # operation (Valor Contábil) as well as the ICMS and IPI amounts.

    df_dre_list: List[pd.DataFrame] = []

    # Prepare patterns for category mappings.  Remove punctuation from CFOP
    # codes to compare both dotted and undotted formats.
    def _clean_cfop(cfop: str) -> str:
        return str(cfop or '').replace('.', '')

    # Revenue (faturamento) CFOPs
    revenue_cfops = {
        '5101', '5102', '5403', '5405', '6101', '6102', '6403'
    }
    # Other outputs (non‑revenue) CFOPs: explicit list plus any CFOP
    # starting with 59 or 69.  Use a lambda to test for prefix.
    other_cfop_explicit = {'5949', '6949', '6910', '5910'}
    other_prefixes = ('59', '69')
    # Cost CFOPs (purchases and services used as cost of goods sold)
    cost_cfops = {
        '2102', '2101', '2403', '2405', '1102', '1101', '1403', '1405'
    }
    # Expense CFOPs (use and consumption, other expenses)
    expense_cfops = {
        '2551', '1551', '1933'
    }

    # Helper to build a category summary from a DataFrame.  Accepts the
    # input frame, the name of the category and the set of CFOP rules.
    def _build_category(df: pd.DataFrame, name: str, cfop_test) -> Optional[pd.DataFrame]:
        if df.empty:
            return None
        # Select rows matching the category
        mask = df['CFOP'].apply(lambda x: cfop_test(_clean_cfop(x)))
        sub = df.loc[mask].copy()
        if sub.empty:
            return None
        # Ensure numeric columns are numeric
        for col in ['Valor Contábil', 'ICMS', 'IPI']:
            if col in sub.columns:
                sub[col] = pd.to_numeric(sub[col], errors='coerce').fillna(0.0)
        # Aggregate by competence and company (CNPJ/razão social) if available
        group_cols = []
        for col in ['Competência', 'CNPJ', 'Razão Social']:
            if col in sub.columns:
                group_cols.append(col)
        if not group_cols:
            group_cols = ['Competência']
        grouped = sub.groupby(group_cols).agg({
            'Valor Contábil': 'sum',
            'ICMS': 'sum',
            'IPI': 'sum'
        }).reset_index()
        grouped['Categoria'] = name
        # Compute total tax (ICMS + IPI) for easier consumption
        grouped['Total Impostos'] = grouped['ICMS'] + grouped['IPI']
        return grouped

    # Build DRE for revenue and other outputs from the outputs DataFrame
    if not df_outputs.empty:
        # Prepare a simplified outputs DataFrame with necessary columns
        df_out = df_outputs.copy()
        # Ensure required columns exist
        # Derive standard columns: Valor Contábil, ICMS, IPI
        # Valor Contábil: prefer Valor Total Nota (note level). If absent, use Valor Operação (C190)
        if 'Valor Contábil' not in df_out.columns:
            if 'Valor Total Nota' in df_out.columns:
                df_out['Valor Contábil'] = pd.to_numeric(df_out['Valor Total Nota'], errors='coerce').fillna(0.0)
            elif 'Valor Operação' in df_out.columns:
                df_out['Valor Contábil'] = pd.to_numeric(df_out['Valor Operação'], errors='coerce').fillna(0.0)
            else:
                df_out['Valor Contábil'] = 0.0
        # ICMS: prefer Valor ICMS (C190); if absent, use Valor ICMS Nota
        if 'ICMS' not in df_out.columns:
            if 'Valor ICMS' in df_out.columns:
                df_out['ICMS'] = pd.to_numeric(df_out['Valor ICMS'], errors='coerce').fillna(0.0)
            elif 'Valor ICMS Nota' in df_out.columns:
                df_out['ICMS'] = pd.to_numeric(df_out['Valor ICMS Nota'], errors='coerce').fillna(0.0)
            else:
                df_out['ICMS'] = 0.0
        # IPI: prefer Valor IPI Nota; fallback to Valor IPI (C190)
        if 'IPI' not in df_out.columns:
            if 'Valor IPI Nota' in df_out.columns:
                df_out['IPI'] = pd.to_numeric(df_out['Valor IPI Nota'], errors='coerce').fillna(0.0)
            elif 'Valor IPI (C190)' in df_out.columns:
                df_out['IPI'] = pd.to_numeric(df_out['Valor IPI (C190)'], errors='coerce').fillna(0.0)
            else:
                df_out['IPI'] = 0.0
        # Revenue category (faturamento)
        def revenue_test(cfop: str) -> bool:
            return cfop in revenue_cfops
        cat_rev = _build_category(df_out, 'Receita', revenue_test)
        if cat_rev is not None:
            df_dre_list.append(cat_rev)
        # Other outputs category
        def other_test(cfop: str) -> bool:
            # match explicit codes or any prefix 59/69
            return cfop in other_cfop_explicit or any(cfop.startswith(p) for p in other_prefixes)
        cat_out = _build_category(df_out, 'Outras Saídas', other_test)
        if cat_out is not None:
            df_dre_list.append(cat_out)
    # Build DRE for costs and expenses from the entries DataFrame
    if not df_entries.empty:
        df_in = df_entries.copy()
        # Standardise IPI column name
        # Derive standard columns: Valor Contábil, ICMS, IPI
        if 'Valor Contábil' not in df_in.columns:
            if 'Valor Total Item' in df_in.columns:
                df_in['Valor Contábil'] = pd.to_numeric(df_in['Valor Total Item'], errors='coerce').fillna(0.0)
            else:
                df_in['Valor Contábil'] = 0.0
        # ICMS: from Valor ICMS Item or Valor ICMS Nota
        if 'ICMS' not in df_in.columns:
            if 'Valor ICMS Item' in df_in.columns:
                df_in['ICMS'] = pd.to_numeric(df_in['Valor ICMS Item'], errors='coerce').fillna(0.0)
            elif 'Valor ICMS Nota' in df_in.columns:
                df_in['ICMS'] = pd.to_numeric(df_in['Valor ICMS Nota'], errors='coerce').fillna(0.0)
            else:
                df_in['ICMS'] = 0.0
        # IPI: from Valor IPI Item or Valor IPI Nota
        if 'IPI' not in df_in.columns:
            if 'Valor IPI Item' in df_in.columns:
                df_in['IPI'] = pd.to_numeric(df_in['Valor IPI Item'], errors='coerce').fillna(0.0)
            elif 'Valor IPI Nota' in df_in.columns:
                df_in['IPI'] = pd.to_numeric(df_in['Valor IPI Nota'], errors='coerce').fillna(0.0)
            else:
                df_in['IPI'] = 0.0
        # Cost category
        def cost_test(cfop: str) -> bool:
            return cfop in cost_cfops
        cat_cost = _build_category(df_in, 'Custos', cost_test)
        if cat_cost is not None:
            df_dre_list.append(cat_cost)
        # Expense category
        def expense_test(cfop: str) -> bool:
            return cfop in expense_cfops
        cat_exp = _build_category(df_in, 'Despesas', expense_test)
        if cat_exp is not None:
            df_dre_list.append(cat_exp)

    # Combine category summaries into a single DRE DataFrame
    if df_dre_list:
        df_dre = pd.concat(df_dre_list, ignore_index=True)
        # Reorder columns to include company identifiers when present.  We
        # preserve the original order but include CNPJ and Razão Social if
        # available.
        dre_columns = []
        for col in ['Competência', 'CNPJ', 'Razão Social', 'Categoria', 'Valor Contábil', 'ICMS', 'IPI', 'Total Impostos']:
            if col in df_dre.columns:
                dre_columns.append(col)
        df_dre = df_dre[dre_columns]
        # Sort for a tidy presentation: competence, company, category order
        category_order = ['Receita', 'Outras Saídas', 'Custos', 'Despesas']
        df_dre['Categoria'] = pd.Categorical(df_dre['Categoria'], categories=category_order, ordered=True)
        sort_cols = []
        for col in ['Competência', 'CNPJ', 'Razão Social', 'Categoria']:
            if col in df_dre.columns:
                sort_cols.append(col)
        df_dre = df_dre.sort_values(by=sort_cols).reset_index(drop=True)
        sheets['DRE Fiscal'] = df_dre

        # -------------------------------------------------------------------
        # Fiscal KPI summary
        # -------------------------------------------------------------------
        # Calculate key performance indicators from the DRE.  For each
        # competence we compute total revenue, total costs, total taxes and
        # derive gross margin and effective tax burden (total taxes / revenue).
        # When the competence is missing (blank or NaN), we treat all data as
        # belonging to a single period.
        if not df_dre.empty:
            # Fill missing competence with a placeholder
            kpi_df = df_dre.copy()
            kpi_df['Competência'] = kpi_df['Competência'].fillna('Sem competência')
            # Ensure company identifiers exist as strings
            for col in ['CNPJ', 'Razão Social']:
                if col in kpi_df.columns:
                    kpi_df[col] = kpi_df[col].fillna('').astype(str)
            kpi_rows = []
            # Group by competence and company identifiers (if present) to compute KPIs per company
            grp_fields = []
            for col in ['Competência', 'CNPJ', 'Razão Social']:
                if col in kpi_df.columns:
                    grp_fields.append(col)
            if not grp_fields:
                grp_fields = ['Competência']
            for keys, comp_df in kpi_df.groupby(grp_fields):
                # keys can be a tuple if multiple fields; normalise to dict
                if isinstance(keys, tuple):
                    comp_dict = {grp_fields[i]: keys[i] for i in range(len(grp_fields))}
                else:
                    comp_dict = {grp_fields[0]: keys}
                total_rev = comp_df.loc[comp_df['Categoria'] == 'Receita', 'Valor Contábil'].sum()
                total_cost = comp_df.loc[comp_df['Categoria'] == 'Custos', 'Valor Contábil'].sum()
                total_taxes = comp_df.loc[comp_df['Categoria'] == 'Receita', 'Total Impostos'].sum()
                gross_margin = total_rev - total_cost
                tax_burden = 0.0
                if total_rev > 0:
                    tax_burden = total_taxes / total_rev
                row = {
                    'Receita': total_rev,
                    'Custos': total_cost,
                    'Margem Bruta': gross_margin,
                    'Total Impostos': total_taxes,
                    'Carga Tributária Efetiva (%)': tax_burden * 100
                }
                # Include grouping fields
                for k, v in comp_dict.items():
                    row[k] = v
                kpi_rows.append(row)
            df_kpi = pd.DataFrame(kpi_rows)
            # Order columns logically
            # Determine column order based on available grouping fields
            col_order = []
            for col in ['Competência', 'CNPJ', 'Razão Social']:
                if col in df_kpi.columns:
                    col_order.append(col)
            col_order += ['Receita', 'Custos', 'Margem Bruta', 'Total Impostos', 'Carga Tributária Efetiva (%)']
            df_kpi = df_kpi[col_order]
            # Ensure competence is a string to avoid NaN representation
            df_kpi['Competência'] = df_kpi['Competência'].astype(str)
            # Net tax burden over revenue will be computed after tax balances are calculated
            sheets['Indicadores Fiscais'] = df_kpi

    # Compute balances (credit vs debit) by company and competence
    balance_rows = []
    if not df_entries.empty or not df_outputs.empty:
        # Summaries per CNPJ and competence
        # Entries: credit side
        if not df_entries.empty:
            df_entries_num = df_entries.copy()
            for col in ['BC ICMS Item', 'Valor ICMS Item', 'Valor IPI Item']:
                if col in df_entries_num.columns:
                    df_entries_num[col] = pd.to_numeric(df_entries_num[col], errors='coerce').fillna(0.0)
            credit_sum = df_entries_num.groupby(['CNPJ', 'Competência']).agg({
                'BC ICMS Item': 'sum',
                'Valor ICMS Item': 'sum',
                'Valor IPI Item': 'sum'
            }).reset_index()
        else:
            credit_sum = pd.DataFrame(columns=['CNPJ', 'Competência', 'BC ICMS Item', 'Valor ICMS Item', 'Valor IPI Item'])
        # Outputs: debit side
        if not df_outputs.empty:
            df_outputs_num = df_outputs.copy()
            for col in ['BC ICMS', 'Valor ICMS', 'Valor IPI Nota']:
                if col in df_outputs_num.columns:
                    df_outputs_num[col] = pd.to_numeric(df_outputs_num[col], errors='coerce').fillna(0.0)
            debit_sum = df_outputs_num.groupby(['CNPJ', 'Competência']).agg({
                'BC ICMS': 'sum',
                'Valor ICMS': 'sum',
                'Valor IPI Nota': 'sum'
            }).reset_index()
        else:
            debit_sum = pd.DataFrame(columns=['CNPJ', 'Competência', 'BC ICMS', 'Valor ICMS', 'Valor IPI Nota'])
        # Merge credit and debit
        bal_df = pd.merge(credit_sum, debit_sum, on=['CNPJ', 'Competência'], how='outer', suffixes=('_Cred', '_Deb')).fillna(0.0)
        for _, row in bal_df.iterrows():
            cnpj = row['CNPJ']
            comp = row['Competência']
            icms_cred = row.get('Valor ICMS Item', 0.0)
            icms_deb = row.get('Valor ICMS', 0.0)
            ipi_cred = row.get('Valor IPI Item', 0.0)
            ipi_deb = row.get('Valor IPI Nota', 0.0)
            balance_rows.append({
                'CNPJ': cnpj,
                'Competência': comp,
                'ICMS a Recuperar (Entradas)': icms_cred,
                'ICMS a Pagar (Saídas)': icms_deb,
                'Saldo ICMS (Cred - Deb)': icms_cred - icms_deb,
                'IPI a Recuperar (Entradas)': ipi_cred,
                'IPI a Pagar (Saídas)': ipi_deb,
                'Saldo IPI (Cred - Deb)': ipi_cred - ipi_deb
            })
        if balance_rows:
            sheets['Saldo Impostos'] = pd.DataFrame(balance_rows)
        # After computing balances, update the 'Indicadores Fiscais' sheet with net tax burden over revenue
        if 'Indicadores Fiscais' in sheets and 'Saldo Impostos' in sheets:
            try:
                df_kpi = sheets['Indicadores Fiscais'].copy()
                df_balance = sheets['Saldo Impostos'].copy()
                # Prepare competence and company identifiers
                df_balance['Competência'] = df_balance['Competência'].fillna('Sem competência').astype(str)
                for col in ['CNPJ', 'Razão Social']:
                    if col in df_balance.columns:
                        df_balance[col] = df_balance[col].fillna('').astype(str)
                # Compute net payable per record
                df_balance['NetPayable'] = (
                    pd.to_numeric(df_balance.get('ICMS a Pagar (Saídas)', 0), errors='coerce').fillna(0.0) -
                    pd.to_numeric(df_balance.get('ICMS a Recuperar (Entradas)', 0), errors='coerce').fillna(0.0)
                ) + (
                    pd.to_numeric(df_balance.get('IPI a Pagar (Saídas)', 0), errors='coerce').fillna(0.0) -
                    pd.to_numeric(df_balance.get('IPI a Recuperar (Entradas)', 0), errors='coerce').fillna(0.0)
                )
                # Only positive amounts represent tax to be paid
                df_balance['NetPayable'] = df_balance['NetPayable'].apply(lambda x: x if x > 0 else 0.0)
                # Build keys for balance mapping
                # Use tuple of competence, CNPJ and Razão Social to identify each company period
                balance_key_cols = []
                for col in ['Competência', 'CNPJ', 'Razão Social']:
                    if col in df_balance.columns:
                        balance_key_cols.append(col)
                if not balance_key_cols:
                    balance_key_cols = ['Competência']
                df_balance['__key__'] = df_balance[balance_key_cols].apply(lambda row: tuple(row), axis=1)
                net_payable_map = df_balance.groupby('__key__')['NetPayable'].sum().to_dict()
                # Compute ratio per KPI row using the same key fields
                imposto_ratios = []
                for _, row in df_kpi.iterrows():
                    key_vals = []
                    for col in balance_key_cols:
                        key_vals.append(str(row.get(col, '')))
                    key_tuple = tuple(key_vals)
                    receita = row['Receita']
                    net_payable = net_payable_map.get(key_tuple, 0.0)
                    ratio = 0.0
                    if receita > 0:
                        ratio = (net_payable / receita) * 100
                    imposto_ratios.append(ratio)
                df_kpi['Imposto/Faturamento (%)'] = imposto_ratios
                sheets['Indicadores Fiscais'] = df_kpi
            except Exception:
                pass
        return sheets


def write_excel(sheets: Dict[str, pd.DataFrame], output_path: str) -> None:
    """Write a dictionary of DataFrames to an Excel file with formatting.

    The function creates an Excel workbook where each key in ``sheets``
    corresponds to a worksheet.  It applies some basic formatting such as
    freezing the header row and auto‑sizing columns based on content up
    to a reasonable maximum width.  The resulting file is saved at
    ``output_path``.
    """
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for sheet_name, df in sheets.items():
            # Truncate sheet name to 31 characters (Excel limit)
            name = sheet_name[:31]
            df.to_excel(writer, sheet_name=name, index=False)
            worksheet = writer.sheets[name]
            # Freeze header row
            worksheet.freeze_panes(1, 0)
            # Set zoom for better readability
            worksheet.set_zoom(90)
            # Auto‑fit columns up to a max width
            for i, col in enumerate(df.columns):
                # Compute max length of the column content (as string)
                max_len = max((len(str(x)) for x in [col] + df[col].astype(str).tolist()), default=0)
                # Limit width between 12 and 60 characters
                width = min(max(max_len + 2, 12), 60)
                worksheet.set_column(i, i, width)


def main(argv: Optional[Iterable[str]] = None) -> int:
    """Entry point for the unified auditor.  Parses command line arguments,
    processes the provided SPED files, and writes an Excel report.
    """
    parser = argparse.ArgumentParser(description='Unified SPED Fiscal Auditor')
    parser.add_argument('--sped', nargs='+', required=True, help='One or more SPED .txt files to audit')
    parser.add_argument('--xml_dir', default='', help='Directory containing NF-e/CT-e XML files for cross‑check')
    parser.add_argument('--tipi', default='', help='TIPI table file (.csv or .xlsx) for IPI cross‑check')
    parser.add_argument('--output', required=True, help='Path to save the resulting Excel report')
    args = parser.parse_args(argv)
    # Load TIPI table if provided
    tipi_map = {}
    if args.tipi:
        try:
            tipi_map = load_tipi_table(args.tipi)
        except Exception as exc:
            print(f"Aviso: Falha ao carregar TIPI ({exc}). A conformidade IPI não será verificada.")
            tipi_map = {}
    # Load XML map if provided
    xml_map = {}
    if args.xml_dir:
        try:
            xml_map = parse_xml_directory(args.xml_dir)
        except Exception as exc:
            print(f"Aviso: Falha ao carregar XMLs ({exc}). O cruzamento XML não será realizado.")
            xml_map = {}
    # Process each SPED file
    records: List[SpedRecord] = []
    for path in args.sped:
        if not os.path.isfile(path):
            print(f"Arquivo SPED não encontrado: {path}")
            continue
        rec = parse_sped_file(path, xml_map, tipi_map)
        records.append(rec)
    if not records:
        print('Nenhum arquivo SPED processado. Saindo.')
        return 1
    # Aggregate and write Excel
    sheets = aggregate_records(records)
    write_excel(sheets, args.output)
    print(f"Relatório salvo em: {args.output}")
    return 0


# ---------------------------------------------------------------------------
# GUI (Tkinter) implementation
# ---------------------------------------------------------------------------
class AuditorGUI(_tk.Tk if _tk else object):
    """Simple GUI wrapper for the unified SPED auditor.

    This class provides a desktop interface using Tkinter.  It allows
    selection of one or more SPED files, an optional TIPI file and an
    optional folder of XMLs.  When running the audit the results are
    written to an Excel file and automatically opened with the system
    default application.  If ``tkinter`` is not available the GUI
    functionality is disabled and instantiating this class will raise
    ``RuntimeError``.
    """

    def __init__(self) -> None:
        if not _tk:
            raise RuntimeError("Tkinter not available on this system.")
        super().__init__()
        self.title("SPED Analyzer ICMS e IPI")
        self.geometry("600x300")

        # Store selected file paths
        self.sped_files: List[str] = []
        self.tipi_file: str = ''
        self.xml_dir: str = ''
        self.output_file: str = ''

        # Create UI elements
        self._create_widgets()

    def _create_widgets(self) -> None:
        """Set up the GUI components."""
        # Frame for SPED selection
        sped_frame = _tk.LabelFrame(self, text="Selecionar SPED(s)")
        sped_frame.pack(fill="x", padx=10, pady=5)
        btn_select_sped = _tk.Button(
            sped_frame, text="Selecionar .txt", command=self._select_speds
        )
        btn_select_sped.pack(side="left", padx=5, pady=5)
        self.sped_label = _tk.Label(sped_frame, text="Nenhum arquivo selecionado")
        self.sped_label.pack(side="left", padx=5, pady=5)

        # Frame for TIPI selection
        tipi_frame = _tk.LabelFrame(self, text="TIPI (opcional)")
        tipi_frame.pack(fill="x", padx=10, pady=5)
        btn_select_tipi = _tk.Button(
            tipi_frame, text="Selecionar TIPI", command=self._select_tipi
        )
        btn_select_tipi.pack(side="left", padx=5, pady=5)
        self.tipi_label = _tk.Label(tipi_frame, text="Nenhum arquivo TIPI selecionado")
        self.tipi_label.pack(side="left", padx=5, pady=5)

        # Frame for XML directory
        xml_frame = _tk.LabelFrame(self, text="Pasta de XMLs (opcional)")
        xml_frame.pack(fill="x", padx=10, pady=5)
        btn_select_xml = _tk.Button(
            xml_frame, text="Selecionar Pasta", command=self._select_xml_dir
        )
        btn_select_xml.pack(side="left", padx=5, pady=5)
        self.xml_label = _tk.Label(xml_frame, text="Nenhuma pasta selecionada")
        self.xml_label.pack(side="left", padx=5, pady=5)

        # Frame for output
        out_frame = _tk.LabelFrame(self, text="Salvar Excel em")
        out_frame.pack(fill="x", padx=10, pady=5)
        btn_select_output = _tk.Button(
            out_frame, text="Escolher Arquivo", command=self._select_output_file
        )
        btn_select_output.pack(side="left", padx=5, pady=5)
        self.output_label = _tk.Label(out_frame, text="Nenhum arquivo de saída selecionado")
        self.output_label.pack(side="left", padx=5, pady=5)

        # Run button
        btn_run = _tk.Button(self, text="Executar Auditoria", command=self._run_audit)
        btn_run.pack(pady=10)

        # Status label
        self.status_label = _tk.Label(self, text="Pronto", anchor="w")
        self.status_label.pack(fill="x", padx=10, pady=5)

    def _select_speds(self) -> None:
        """Open file dialog to select one or more SPED txt files."""
        if not _filedialog:
            return
        files = _filedialog.askopenfilenames(
            title="Selecione arquivos SPED (.txt)",
            filetypes=[('Text Files', '*.txt')],
        )
        if files:
            self.sped_files = list(files)
            if len(self.sped_files) == 1:
                self.sped_label.config(text=os.path.basename(self.sped_files[0]))
            else:
                self.sped_label.config(text=f"{len(self.sped_files)} arquivos selecionados")

    def _select_tipi(self) -> None:
        """Open file dialog to select TIPI file (CSV or XLSX)."""
        if not _filedialog:
            return
        file = _filedialog.askopenfilename(
            title="Selecione o arquivo TIPI",
            filetypes=[('Excel Files', '*.xlsx'), ('CSV Files', '*.csv')],
        )
        if file:
            self.tipi_file = file
            self.tipi_label.config(text=os.path.basename(file))

    def _select_xml_dir(self) -> None:
        """Open directory selection dialog for XML folder."""
        if not _filedialog:
            return
        directory = _filedialog.askdirectory(title="Selecione a pasta de XMLs")
        if directory:
            self.xml_dir = directory
            self.xml_label.config(text=os.path.basename(directory))

    def _select_output_file(self) -> None:
        """Open file dialog to choose the Excel output file."""
        if not _filedialog:
            return
        file = _filedialog.asksaveasfilename(
            title="Salvar relatório Excel",
            defaultextension=".xlsx",
            filetypes=[('Excel Files', '*.xlsx')],
        )
        if file:
            if not file.lower().endswith('.xlsx'):
                file += '.xlsx'
            self.output_file = file
            self.output_label.config(text=os.path.basename(file))

    def _run_audit(self) -> None:
        """Run the audit in a separate thread to avoid freezing the GUI."""
        if not self.sped_files:
            if _messagebox:
                _messagebox.showwarning("SPED não selecionado", "Selecione pelo menos um arquivo SPED (.txt).")
            return
        if not self.output_file:
            if _messagebox:
                _messagebox.showwarning("Saída não definida", "Escolha um caminho para salvar o Excel de saída.")
            return
        # Disable UI elements during processing and show status
        self._set_status("Processando...")
        if _threading:
            _threading.Thread(target=self._audit_worker, daemon=True).start()
        else:
            # If threading not available, run synchronously
            self._audit_worker()

    def _audit_worker(self) -> None:
        """Worker function to perform the audit and update UI when done."""
        try:
            # Load TIPI if provided
            tipi_map: Dict[str, float] = {}
            if self.tipi_file:
                try:
                    self._set_status("Carregando TIPI...")
                    tipi_map = load_tipi_table(self.tipi_file)
                except Exception as exc:
                    if _messagebox:
                        _messagebox.showwarning(
                            "Falha TIPI",
                            f"Falha ao carregar TIPI ({exc}). A conformidade IPI não será verificada."
                        )
                    tipi_map = {}
            # Load XML map if provided
            xml_map: Dict[str, Dict[str, any]] = {}
            if self.xml_dir:
                try:
                    self._set_status("Carregando XMLs...")
                    xml_map = parse_xml_directory(self.xml_dir)
                except Exception as exc:
                    if _messagebox:
                        _messagebox.showwarning(
                            "Falha XML",
                            f"Falha ao carregar XMLs ({exc}). O cruzamento XML não será realizado."
                        )
                    xml_map = {}
            # Process each SPED file
            records: List[SpedRecord] = []
            for path in self.sped_files:
                self._set_status(f"Processando {os.path.basename(path)}...")
                try:
                    rec = parse_sped_file(path, xml_map, tipi_map)
                    records.append(rec)
                except Exception as exc:
                    if _messagebox:
                        _messagebox.showerror(
                            "Erro de Processamento",
                            f"Erro ao processar {os.path.basename(path)}: {exc}"
                        )
                    return
            # Aggregate records
            self._set_status("Agrupando resultados...")
            sheets = aggregate_records(records)
            # Write Excel
            self._set_status("Salvando Excel...")
            write_excel(sheets, self.output_file)
            self._set_status("Concluído! Abrindo arquivo...")
            # Open the file automatically
            self._open_file(self.output_file)
            self._set_status("Relatório gerado com sucesso.")
        except Exception as exc:
            if _messagebox:
                _messagebox.showerror("Erro inesperado", str(exc))
            self._set_status("Erro durante a execução.")

    def _set_status(self, msg: str) -> None:
        """Update status label in a thread-safe manner."""
        def _update():
            self.status_label.config(text=msg)
        # schedule update on the Tkinter main thread
        self.status_label.after(0, _update)

    def _open_file(self, path: str) -> None:
        """Attempt to open the given file with the default system application."""
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore
            elif sys.platform == "darwin":
                import subprocess
                subprocess.run(["open", path], check=False)
            else:
                import subprocess
                subprocess.run(["xdg-open", path], check=False)
        except Exception as exc:
            if _messagebox:
                _messagebox.showwarning("Falha ao Abrir", f"O relatório foi salvo em:\n{path}\n\nPorém, não foi possível abri-lo automaticamente: {exc}")

# ---------------------------------------------------------------------------
# Streamlit web application
# ---------------------------------------------------------------------------
def run_streamlit_app() -> None:
    """Execute the Streamlit web interface.

    This function builds an interactive web application using Streamlit.  It
    allows users to upload one or more SPED text files, optionally provide
    a TIPI table (CSV or XLSX) and XML files (either individually or as
    ZIP archives), and then runs the audit.  Upon completion the user
    may download the resulting Excel report and optionally view a
    summary of fiscal indicators directly in the browser.
    """
    # Import streamlit lazily so that command‑line use does not require it
    import streamlit as st
    import tempfile
    import zipfile
    import io

    st.set_page_config(page_title="SPED Analyzer ICMS e IPI", layout="centered")
    st.title("SPED Analyzer ICMS e IPI")
    st.write("Auditoria de arquivos SPED ICMS/IPI")

    # File uploads
    sped_files = st.file_uploader(
        "Selecione arquivos SPED (.txt)",
        type=["txt"],
        accept_multiple_files=True
    )
    tipi_file = st.file_uploader(
        "Selecione arquivo TIPI (CSV ou XLSX) (opcional)",
        type=["csv", "xlsx"],
        accept_multiple_files=False
    )
    xml_files = st.file_uploader(
        "Selecione arquivos XML (NF-e/CT-e) ou ZIP contendo XMLs (opcional)",
        type=["xml", "zip"],
        accept_multiple_files=True
    )

    run_button = st.button("Executar Auditoria")
    if run_button:
        if not sped_files:
            st.error("Selecione pelo menos um arquivo SPED.")
        else:
            with st.spinner("Processando arquivos... isto pode levar alguns minutos."):
                # Create a temporary working directory
                with tempfile.TemporaryDirectory() as tmpdir:
                    # Write SPED files to disk
                    sped_paths: List[str] = []
                    for uploaded in sped_files:
                        path = os.path.join(tmpdir, uploaded.name)
                        with open(path, "wb") as f:
                            f.write(uploaded.getbuffer())
                        sped_paths.append(path)
                    # Handle TIPI file
                    tipi_map: Dict[str, float] = {}
                    if tipi_file is not None:
                        tipi_path = os.path.join(tmpdir, tipi_file.name)
                        with open(tipi_path, "wb") as f:
                            f.write(tipi_file.getbuffer())
                        try:
                            tipi_map = load_tipi_table(tipi_path)
                        except Exception as exc:
                            st.warning(f"Falha ao carregar TIPI ({exc}). A conformidade IPI não será verificada.")
                            tipi_map = {}
                    # Handle XML files
                    xml_map: Dict[str, Dict[str, any]] = {}
                    if xml_files:
                        for uploaded in xml_files:
                            fname = uploaded.name.lower()
                            if fname.endswith(".zip"):
                                # Unzip and parse each XML inside
                                try:
                                    with zipfile.ZipFile(io.BytesIO(uploaded.getbuffer())) as zf:
                                        for name in zf.namelist():
                                            if not name.lower().endswith(".xml"):
                                                continue
                                            try:
                                                data_bytes = zf.read(name)
                                                # Write to temp file for parsing via existing functions
                                                with tempfile.NamedTemporaryFile(dir=tmpdir, delete=False, suffix=".xml") as tfile:
                                                    tfile.write(data_bytes)
                                                    tfile.flush()
                                                    # Try NFe then CT-e parser
                                                    d = parse_xml_nfe(tfile.name)
                                                    if not d or 'Chave' not in d:
                                                        d = parse_xml_cte(tfile.name)
                                                    if d and 'Chave' in d:
                                                        xml_map[d['Chave']] = d
                                            except Exception:
                                                # Ignore individual parse errors
                                                pass
                                except Exception:
                                    # Ignore zip extraction/parsing errors
                                    pass
                            elif fname.endswith(".xml"):
                                try:
                                    with tempfile.NamedTemporaryFile(dir=tmpdir, delete=False, suffix=".xml") as tfile:
                                        tfile.write(uploaded.getbuffer())
                                        tfile.flush()
                                        d = parse_xml_nfe(tfile.name)
                                        if not d or 'Chave' not in d:
                                            d = parse_xml_cte(tfile.name)
                                        if d and 'Chave' in d:
                                            xml_map[d['Chave']] = d
                                except Exception:
                                    pass
                    # Process each SPED file
                    records: List[SpedRecord] = []
                    for path in sped_paths:
                        try:
                            rec = parse_sped_file(path, xml_map, tipi_map)
                            records.append(rec)
                        except Exception as exc:
                            st.error(f"Erro ao processar {os.path.basename(path)}: {exc}")
                    if not records:
                        st.error("Nenhum arquivo SPED processado.")
                    else:
                        sheets = aggregate_records(records)
                        # Write the Excel report into memory
                        with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp_excel:
                            write_excel(sheets, tmp_excel.name)
                            tmp_excel.seek(0)
                            excel_data = tmp_excel.read()
                        st.success("Relatório gerado com sucesso!")
                        # Provide download button
                        st.download_button(
                            label="Download do relatório Excel",
                            data=excel_data,
                            file_name="auditoria_sped.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        # Optionally show fiscal indicators summary
                        if 'Indicadores Fiscais' in sheets:
                            if st.checkbox("Mostrar resumo de indicadores fiscais (DRE)"):
                                st.dataframe(sheets['Indicadores Fiscais'])

# ---------------------------------------------------------------------------
# Entry point selection
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    """
    Determine how to launch the application based on how the script is
    invoked.  When command line arguments are provided (e.g. when
    running via ``python script.py --sped ...``) the command line
    interface is used.  Otherwise the function will attempt to run
    the Streamlit application; if that fails and Tkinter is available
    a desktop GUI will be shown as a fallback.  This makes the
    module versatile: it can be run as a CLI tool, launched with
    ``streamlit run``, or executed directly to open a desktop
    interface.
    """
    import sys as _sys
    # When any argument starts with '--sped' assume CLI invocation
    if any(arg.startswith('--sped') for arg in _sys.argv[1:]):
        _sys.exit(main())
    try:
        # Attempt to detect if running under streamlit.  When using
        # ``streamlit run``, no command line flags are passed to the
        # script and the Streamlit framework will be available for
        # import.  If the import succeeds run the web app.
        import streamlit  # type: ignore
        run_streamlit_app()
    except Exception:
        # Fallback to Tkinter GUI if available, otherwise CLI
        if _tk:
            app = AuditorGUI()
            app.mainloop()
        else:
            _sys.exit(main())