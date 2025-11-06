# -*- coding: utf-8 -*-
"""
Auditor Unificado do SPED ICMS/IPI
==================================

Este módulo consolida a lógica de auditoria de múltiplos scripts do SPED em um
único aplicativo. Ele lê um ou mais arquivos texto do **SPED Fiscal (EFD ICMS/IPI)**
e produz uma planilha Excel com diversos relatórios de auditoria.

Principais recursos:

* Extração de dados mestres (razão social, CNPJ, UF, etc.) dos registros
  |0000|, |0002|, |0005|, |0015| e |0100|.
* Mapeamento de produtos (|0200|) para NCM e descrição.
* Itens de **entradas** (ind_oper = 0) com base no |C170|, incluindo checagem
  opcional contra tabela TIPI (alíquota de IPI), quando fornecida.
* Resumo de entradas por **CFOP** e por **NCM/CFOP**.
* Notas de **saídas** (ind_oper = 1) por |C190|, com consolidação de valores
  e alíquotas efetivas de ICMS.
* Resumo de saídas por **CFOP** e **CST**.
* Identificação de notas de saída **sem** o respectivo |C190|.
* Extração de dados de **CT-e** via |D100| e |D190|.
* Cruzamento opcional com **XML** (NF-e e CT-e) para validar valores.
* Cruzamento opcional de IPI x **TIPI** (CSV/XLSX).
* Cálculo de saldos **ICMS/IPI** (crédito x débito) por empresa e competência.
* Sinalização de CFOPs típicos de **imobilizado/uso e consumo**
  (1556, 1407, 1551, 1406, 2551, 2556, 2406, 2407) para apontar possíveis
  créditos indevidos.
* Coleta de ajustes (|C197|, |E111|, |E115|, |E116|) e resumos de blocos
  (E200, E300, E500) para apoiar auditorias de ST/DIFAL/IPI.
* Geração de um resumo tipo **DRE Fiscal** (Receita, Outras Saídas, Custos,
  Despesas) e **Indicadores Fiscais**.

Entradas opcionais:
- Pasta com XMLs (NF-e/CT-e) para cruzamento.
- Arquivo TIPI (CSV/XLSX) para verificação de alíquota de IPI por NCM.

Saída:
- Um arquivo **.xlsx** com múltiplas abas de relatórios.

Modo de uso (linha de comando):
    python sped_analyzer.py --sped /caminho/arquivo1.txt /caminho/arquivo2.txt \
        --tipi /caminho/tipi.xlsx --xml_dir /caminho/xmls \
        --output auditoria_resultado.xlsx

Se TIPI ou XML não forem informados, os respectivos cruzamentos são ignorados.
A codificação do arquivo é detectada automaticamente (usa latin-1 como padrão).

Também é possível executar como **web** via Streamlit ou **desktop** via Tkinter,
dependendo do ambiente.
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

# Importações opcionais para GUI desktop (Tkinter)
try:
    import tkinter as _tk
    from tkinter import filedialog as _filedialog, messagebox as _messagebox
    import threading as _threading
except Exception:
    _tk = None
    _filedialog = None
    _messagebox = None
    _threading = None

# O Streamlit é importado apenas quando o app web é executado.

# ---------------------------------------------------------------------------
# Detecção de codificação
# ---------------------------------------------------------------------------

def detect_encoding(file_path: str) -> str:
    """Detecta a codificação de caracteres do arquivo informado.

    Tenta usar o pacote opcional ``chardet`` em uma amostra do arquivo.
    Em caso de falha ou baixa confiança, retorna 'latin-1'.
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
        if confidence < 0.7 or enc.lower() in {'ascii'}:
            return 'latin-1'
        return enc
    except Exception:
        return 'latin-1'


# ---------------------------------------------------------------------------
# Utilidades para normalização de texto e parsing numérico BR
# ---------------------------------------------------------------------------

def norm_text(s: str) -> str:
    """Normaliza texto (minúsculo, sem acento/pontuação) para comparações."""
    if s is None:
        return ''
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ascii', 'ignore').decode('utf-8')
    s = s.lower()
    s = re.sub(r'[\s\./\-_,]+', ' ', s).strip()
    return s


def parse_float_br(value: str) -> float:
    """Converte número em formato brasileiro para float.

    Ex.: '1.234,56' -> 1234.56. Em falha, retorna 0.0.
    """
    if not value:
        return 0.0
    value = value.strip()
    if not value:
        return 0.0
    value = value.replace('.', '').replace(',', '.')
    try:
        return float(value)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Carregamento da TIPI
# ---------------------------------------------------------------------------

def load_tipi_table(path: str) -> Dict[str, float]:
    """Carrega uma tabela TIPI (CSV ou XLSX) e devolve {NCM: aliquota}."""
    if not path:
        return {}
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Arquivo TIPI não encontrado: {path}")
    if path.lower().endswith('.xlsx'):
        df = pd.read_excel(path)
    elif path.lower().endswith('.csv'):
        df = pd.read_csv(path, sep=';', decimal=',')
    else:
        raise ValueError("Formato TIPI não suportado; use .csv ou .xlsx")
    # Normaliza nomes de colunas
    cols = {}
    for col in df.columns:
        nc = unicodedata.normalize('NFKD', col)
        nc = nc.encode('ascii', 'ignore').decode('utf-8')
        nc = nc.upper().strip()
        nc = re.sub(r'[^A-Z0-9]', '', nc)
        cols[col] = nc
    df = df.rename(columns=cols)
    if 'NCM' not in df.columns or 'ALIQUOTA' not in df.columns:
        raise KeyError("TIPI deve conter colunas 'NCM' e 'ALIQUOTA'")
    tipi_map: Dict[str, float] = {}
    for _, row in df.iterrows():
        ncm = str(row['NCM']).strip()
        if not ncm:
            continue
        try:
            aliquot = float(str(row['ALIQUOTA']).replace(',', '.'))
        except Exception:
            continue
        tipi_map[ncm] = aliquot
    return tipi_map


# ---------------------------------------------------------------------------
# Leitura de XML (NF-e e CT-e)
# ---------------------------------------------------------------------------

def parse_xml_nfe(path: str) -> Optional[Dict[str, any]]:
    """Extrai totais e participantes principais de um XML de NF-e."""
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        data: Dict[str, any] = {}
        # Chave de acesso
        inf = root.find('.//nfe:infNFe', ns)
        if inf is not None:
            key = inf.get('Id')
            if key and key.startswith('NFe'):
                key = key[3:]
            data['Chave'] = key
        # Totais
        tot = root.find('.//nfe:ICMSTot', ns)
        if tot is not None:
            vICMS = tot.find('nfe:vICMS', ns)
            data['Valor ICMS XML'] = float(vICMS.text) if vICMS is not None and vICMS.text else 0.0
            vIPI = tot.find('nfe:vIPI', ns)
            data['Valor IPI XML'] = float(vIPI.text) if vIPI is not None and vIPI.text else 0.0
            vProd = tot.find('nfe:vProd', ns)
            data['Valor Produtos XML'] = float(vProd.text) if vProd is not None and vProd.text else 0.0
        # Participantes
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
    """Extrai totais e participantes principais de um XML de CT-e."""
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
        # ICMS (variações)
        icms = root.find('.//cte:ICMS/cte:ICMSOutraUF', ns)
        if icms is not None:
            data['BC ICMS XML'] = float(icms.find('cte:vBCOutraUF', ns).text) if icms.find('cte:vBCOutraUF', ns) is not None and icms.find('cte:vBCOutraUF', ns).text else 0.0
            data['Valor ICMS XML'] = float(icms.find('cte:vICMSOutraUF', ns).text) if icms.find('cte:vICMSOutraUF', ns) is not None and icms.find('cte:vICMSOutraUF', ns).text else 0.0
            data['Alíquota ICMS XML'] = float(icms.find('cte:pICMSOutraUF', ns).text) if icms.find('cte:pICMSOutraUF', ns) is not None and icms.find('cte:pICMSOutraUF', ns).text else 0.0
            cst = icms.find('cte:CST', ns)
            data['CST XML'] = cst.text if cst is not None else 'N/A'
        else:
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
        # Tomador (toma3)
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
        # Participantes
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
    """Percorre uma pasta e monta um mapa {Chave: dados} de XMLs NFe/CTe."""
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
# Leitura do SPED
# ---------------------------------------------------------------------------

class SpedRecord:
    """Container dos dados processados de um arquivo SPED."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.entries: List[dict] = []      # Itens detalhados de entradas (C170)
        self.outputs: List[dict] = []      # Resumo de saídas (C190)
        self.imob_uso: List[dict] = []     # Entradas com CFOPs de imobilizado/uso/consumo
        self.cte: List[dict] = []          # CT-e (D190)
        self.adjustments: List[dict] = []  # Ajustes (C197/E111/E115/E116)
        self.st_blocks: List[dict] = []    # E200/E210
        self.difal_blocks: List[dict] = [] # E300/E310/E316
        self.ipi_blocks: List[dict] = []   # E500/E510
        self.master_data: dict = {}        # Registros mestres
        self.block_flags: dict = {}        # Presença de blocos/indícios
        self.missing_c190: List[dict] = [] # Notas de saída sem C190
        self.parsing_warnings: List[str] = []
        # Itens (entradas + saídas) para sumarizações amplas por NCM/CFOP
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
        self.items.extend(other.items)


def parse_sped_file(file_path: str, xml_map: Dict[str, Dict[str, any]], tipi: Dict[str, float]) -> SpedRecord:
    """Lê um arquivo SPED Fiscal e devolve um SpedRecord com os dados extraídos."""
    record = SpedRecord(file_path)
    encoding = detect_encoding(file_path)
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
            # Tabelas temporárias e estado corrente
            ncm_map: Dict[str, str] = {}     # cod_item -> NCM
            desc_map: Dict[str, str] = {}    # cod_item -> descrição
            current_note: Optional[dict] = None
            current_note_key: Optional[str] = None
            current_note_is_entry: bool = False
            current_note_has_c190 = False
            current_cte: Optional[dict] = None

            # Contextos de blocos (E200/E210, E300/E310/E316, E500/E510)
            current_e200 = None
            current_e300 = None
            current_e310 = None
            current_e500 = None

            # Dados mestres
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

            # Flags de presença
            block_flags = {
                'has_c100_saida': False,
                'has_st_cfop': False,
                'has_st_cfop_divergence': False,
                'has_block_e200': False,
                'has_difal_cfop': False,
                'has_block_e300': False,
                'has_block_g110': False
            }

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

                # --- Dados mestres ---
                if rec == '0000':
                    # |0000|...|DT_INI|DT_FIN|NOME|CNPJ|...|UF|IE|COD_MUN|IM|...|IND_PERFIL|IND_ATIV|
                    if len(parts) > 8:
                        dt_ini = parts[3] if len(parts) > 3 else ''
                        dt_fin = parts[4] if len(parts) > 4 else ''
                        date_source = ''
                        if len(dt_ini) == 8 and dt_ini.isdigit():
                            date_source = dt_ini
                        elif len(dt_fin) == 8 and dt_fin.isdigit():
                            date_source = dt_fin
                        if date_source:
                            mes, ano = date_source[2:4], date_source[4:8]
                            master['competence'] = f"{mes}/{ano}"
                        master['company_name'] = parts[6].strip() if len(parts) > 6 else ''
                        master['company_cnpj'] = parts[7].strip() if len(parts) > 7 else ''
                        master['company_ie'] = parts[9].strip() if len(parts) > 9 else ''
                        master['company_cod_mun'] = parts[10].strip() if len(parts) > 10 else ''
                        master['company_im'] = parts[11].strip() if len(parts) > 11 else ''
                        master['company_profile'] = parts[14].strip() if len(parts) > 14 else ''
                        master['company_status'] = parts[15].strip() if len(parts) > 15 else ''
                elif rec == '0002':
                    if len(parts) > 2:
                        master['company_activity_type'] = parts[2].strip()
                elif rec == '0005':
                    if len(parts) > 2:
                        master['company_trade_name'] = parts[2].strip()
                    if len(parts) > 3:
                        master['company_phone'] = parts[3].strip()
                    if len(parts) > 4:
                        master['company_email'] = parts[10].strip() if len(parts) > 10 else master.get('company_email', '')
                        master['company_address'] = parts[4].strip()
                    if len(parts) > 5:
                        master['company_number'] = parts[5].strip()
                    if len(parts) > 6:
                        master['company_complement'] = parts[6].strip()
                    if len(parts) > 7:
                        master['company_district'] = parts[7].strip()
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

                # --- Produtos (0200) ---
                if rec == '0200':
                    cod_item = parts[2].strip() if len(parts) > 2 else ''
                    descr_item = parts[3].strip() if len(parts) > 3 else ''
                    ncm = parts[8].strip() if len(parts) > 8 else ''
                    if cod_item:
                        if ncm:
                            ncm_map[cod_item] = ncm
                        if descr_item:
                            desc_map[cod_item] = descr_item

                # --- Cabeçalho de documento (C100) ---
                if rec == 'C100':
                    # Se a nota anterior era de saída e não teve C190, registra como faltante
                    if current_note is not None and not current_note_is_entry and not current_note_has_c190:
                        record.missing_c190.append(current_note.copy())
                    current_note = None
                    current_note_key = None
                    current_note_is_entry = False
                    current_note_has_c190 = False
                    if len(parts) > 2:
                        ind_oper = parts[2].strip()
                        if ind_oper in ('0', '1'):
                            current_note_is_entry = (ind_oper == '0')
                            try:
                                serie = parts[7].strip() if len(parts) > 7 else ''
                                numero = parts[8].strip() if len(parts) > 8 else ''
                                chave = parts[9].strip() if len(parts) > 9 else ''
                                # Valor total
                                vl_doc = 0.0
                                if len(parts) > 12 and parts[12].strip():
                                    vl_doc = parse_float_br(parts[12])
                                elif len(parts) > 11 and parts[11].strip():
                                    vl_doc = parse_float_br(parts[11])
                                # BC ICMS
                                bc_icms = 0.0
                                if len(parts) > 21 and parts[21].strip():
                                    bc_icms = parse_float_br(parts[21])
                                elif len(parts) > 20 and parts[20].strip():
                                    bc_icms = parse_float_br(parts[20])
                                # Valor ICMS
                                vl_icms = 0.0
                                if len(parts) > 22 and parts[22].strip():
                                    vl_icms = parse_float_br(parts[22])
                                elif len(parts) > 21 and parts[21].strip():
                                    vl_icms = parse_float_br(parts[21])
                                # Valor IPI
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

                # --- Itens (C170) ---
                if rec == 'C170' and current_note is not None:
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
                        eff_aliq = (vl_icms_item / val_item) * 100.0 if val_item > 0 else 0.0
                        ncm = ncm_map.get(cod_item, '')
                        descr = desc_map.get(cod_item, '')
                        # Conformidade IPI x TIPI
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
                        record.items.append(item_rec)
                        if current_note_is_entry:
                            record.entries.append(item_rec)
                            if cfop.replace('.', '') in {'1556', '1407', '1551', '1406', '2551', '2556', '2406', '2407'}:
                                uso_rec = item_rec.copy()
                                if vl_icms_item > 0.001 or vl_ipi_item > 0.001:
                                    uso_rec['Situação Crédito'] = '❌ Crédito indevido sobre Uso/Consumo'
                                else:
                                    uso_rec['Situação Crédito'] = '✅ Sem crédito indevido sobre Uso/Consumo'
                                record.imob_uso.append(uso_rec)
                    except Exception:
                        pass

                # --- Resumo de saídas (C190) ---
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
                        eff_aliq = (vl_icms / bc_icms) * 100.0 if bc_icms > 0 else 0.0
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
                        # Indícios de ST/DIFAL via CFOP
                        if cfop.replace('.', '') in {'5401', '5403', '5405', '6401', '6403'}:
                            block_flags['has_st_cfop'] = True
                        if cfop.replace('.', '') in {'5401', '5403', '6403'}:
                            block_flags['has_st_cfop_divergence'] = True
                        if cfop.replace('.', '') in {'6107', '6108'}:
                            block_flags['has_difal_cfop'] = True
                    except Exception:
                        pass

                # --- CT-e (D100 / D190) ---
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
                        eff_aliq = (vl_icms / vl_opr) * 100.0 if vl_opr > 0 else 0.0
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

                # --- Observações e ajustes por documento (C195/C197) ---
                if rec == 'C195' and current_note is not None:
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
                    code = parts[2].strip() if len(parts) > 2 else ''
                    descr = parts[3].strip() if len(parts) > 3 else ''
                    adj_value = 0.0
                    for item in parts[4:]:
                        v = parse_float_br(item)
                        if v > 0:
                            adj_value = v
                    add_adjustment('C197', code, descr, adj_value, current_note_key)

                # --- Ajustes por período (E111/E115/E116) ---
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
                    cod_or = parts[2].strip() if len(parts) > 2 else ''
                    value = parse_float_br(parts[3]) if len(parts) > 3 else 0.0
                    cod_rec = parts[5].strip() if len(parts) > 5 else ''
                    txt = parts[9].strip() if len(parts) > 9 else ''
                    descr = f"{cod_or} {cod_rec} {txt}".strip()
                    add_adjustment('E116', cod_rec or cod_or, descr, value)

                # --- Rastreadores de presença de blocos ---
                if rec.startswith('E2'):
                    block_flags['has_block_e200'] = True
                if rec.startswith('E3'):
                    block_flags['has_block_e300'] = True
                if rec == 'G110':
                    block_flags['has_block_g110'] = True

                # --- E200/E210 (ICMS ST por UF/período) ---
                if rec == 'E200':
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
                    current_e200['Ind Mov'] = parts[2].strip() if len(parts) > 2 else ''
                    record.st_blocks.append(current_e200.copy())

                # --- E300/E310/E316 (DIFAL) ---
                if rec == 'E300':
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
                    current_e300['Ind Mov'] = parts[2].strip() if len(parts) > 2 else ''
                    vl_apur = parse_float_br(parts[9]) if len(parts) > 9 else 0.0
                    current_e310 = current_e300.copy()
                    current_e310['Saldo Apurado'] = vl_apur
                if rec == 'E316' and current_e310 is not None:
                    cod_rec_e316 = parts[2].strip() if len(parts) > 2 else ''
                    vl_recol = parse_float_br(parts[3]) if len(parts) > 3 else 0.0
                    dt_recol = parts[4].strip() if len(parts) > 4 else ''
                    current_e310['Código Receita'] = cod_rec_e316
                    current_e310['Valor Recolhimento'] = vl_recol
                    current_e310['Data Recolhimento'] = dt_recol
                    record.difal_blocks.append(current_e310.copy())

                # --- E500/E510 (IPI) ---
                if rec == 'E500':
                    current_e500 = {
                        'Arquivo': os.path.basename(file_path),
                        'Competência': master['competence'],
                        'Ind Apur': parts[2].strip() if len(parts) > 2 else '',
                        'Data Início': parts[3].strip() if len(parts) > 3 else '',
                        'Data Fim': parts[4].strip() if len(parts) > 4 else ''
                    }
                if rec == 'E510' and current_e500 is not None:
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

        # Após ler o arquivo, se a última nota de saída não teve C190, registra
        if current_note is not None and not current_note_is_entry and not current_note_has_c190:
            record.missing_c190.append(current_note.copy())

        # Salva dados mestres e flags
        record.master_data = master.copy()
        record.block_flags = block_flags.copy()

        # Anexa valores do XML quando disponível
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
# Agregações e relatórios (DataFrames)
# ---------------------------------------------------------------------------

def aggregate_records(records: List[SpedRecord]):
    """Agrega múltiplos SpedRecord em DataFrames prontos para exportação."""
    df_entries = pd.DataFrame([row for rec in records for row in rec.entries])
    df_outputs = pd.DataFrame([row for rec in records for row in rec.outputs])
    df_items = pd.DataFrame([row for rec in records for row in getattr(rec, 'items', [])])
    df_imob = pd.DataFrame([row for rec in records for row in rec.imob_uso])
    df_cte = pd.DataFrame([row for rec in records for row in rec.cte])
    df_adjustments = pd.DataFrame([row for rec in records for row in rec.adjustments])
    df_st_blocks = pd.DataFrame([row for rec in records for row in rec.st_blocks])
    df_difal_blocks = pd.DataFrame([row for rec in records for row in rec.difal_blocks])
    df_ipi_blocks = pd.DataFrame([row for rec in records for row in rec.ipi_blocks])
    df_missing_c190 = pd.DataFrame([row for rec in records for row in rec.missing_c190])
    df_master = pd.DataFrame([rec.master_data for rec in records])
    df_flags = pd.DataFrame([rec.block_flags for rec in records])

    sheets: Dict[str, pd.DataFrame] = {}

    # Itens detalhados + resumos por NCM/CFOP e CFOP-NCM-CST
    if not df_items.empty:
        num_cols_items = ['Valor Total Item', 'BC ICMS Item', 'Valor ICMS Item', 'Valor IPI Item']
        for col in num_cols_items:
            if col in df_items.columns:
                df_items[col] = pd.to_numeric(df_items[col], errors='coerce').fillna(0.0)
        sheets['Detalhes Itens'] = df_items

        grp_cols = [c for c in ['Tipo Nota', 'Competência', 'CNPJ', 'Razão Social', 'NCM Item', 'CFOP'] if c in df_items.columns]
        if grp_cols:
            agg_cols = {c: 'sum' for c in num_cols_items if c in df_items.columns}
            df_items_sum = df_items.groupby(grp_cols).agg(agg_cols).reset_index()
            rename_map_items = {
                'Valor Total Item': 'Valor Contábil',
                'BC ICMS Item': 'BC ICMS',
                'Valor ICMS Item': 'ICMS',
                'Valor IPI Item': 'IPI'
            }
            df_items_sum = df_items_sum.rename(columns={k: v for k, v in rename_map_items.items() if k in df_items_sum.columns})
            sheets['Resumo Itens por NCM-CFOP'] = df_items_sum

        grp2 = [c for c in ['Tipo Nota', 'Competência', 'CNPJ', 'Razão Social', 'CFOP', 'NCM Item', 'CST ICMS'] if c in df_items.columns]
        if grp2:
            agg2 = {c: 'sum' for c in num_cols_items if c in df_items.columns}
            df_cfop_ncm_cst = df_items.groupby(grp2).agg(agg2).reset_index()
            df_cfop_ncm_cst = df_cfop_ncm_cst.rename(columns={
                'Valor Total Item': 'Valor Contábil',
                'BC ICMS Item': 'BC ICMS',
                'Valor ICMS Item': 'ICMS',
                'Valor IPI Item': 'IPI'
            })
            sheets['Resumo CFOP-NCM-CST'] = df_cfop_ncm_cst

        # Ranking de produtos (Descrição/NCM/CFOP)
        rank_cols = [c for c in ['Competência', 'CNPJ', 'Razão Social', 'Descrição do Produto', 'NCM Item', 'CFOP'] if c in df_items.columns]
        if rank_cols:
            agg_rank = {c: 'sum' for c in num_cols_items if c in df_items.columns}
            df_rank = df_items.groupby(rank_cols).agg(agg_rank).reset_index()
            df_rank = df_rank.rename(columns={
                'Valor Total Item': 'Valor Contábil',
                'BC ICMS Item': 'BC ICMS',
                'Valor ICMS Item': 'ICMS',
                'Valor IPI Item': 'IPI'
            })
            if 'Valor Contábil' in df_rank.columns:
                df_rank = df_rank.sort_values(by='Valor Contábil', ascending=False)
            sheets['Ranking Produtos'] = df_rank

        # Resumo por UF e CFOP (somente se existirem as colunas)
        uf_cfop_cols = [c for c in ['Competência', 'CNPJ', 'Razão Social', 'UF', 'CFOP'] if c in df_items.columns]
        if uf_cfop_cols:
            agg_cols_uf = {c: 'sum' for c in num_cols_items if c in df_items.columns}
            df_uf_cfop = df_items.groupby(uf_cfop_cols).agg(agg_cols_uf).reset_index()
            df_uf_cfop = df_uf_cfop.rename(columns={
                'Valor Total Item': 'Valor Contábil',
                'BC ICMS Item': 'BC ICMS',
                '
