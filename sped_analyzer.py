# Create a streamlined, Streamlit-only SPED Analyzer script that avoids writing to fixed paths.
# The app only writes to temporary files and exposes a download button.
code = r'''# -*- coding: utf-8 -*-
"""
SPED Analyzer ICMS e IPI (apenas Streamlit)
-------------------------------------------
Aplicativo web para auditar arquivos SPED EFD ICMS/IPI (.txt), com suporte opcional a TIPI (CSV/XLSX)
e XMLs (NF-e/CT-e, arquivos soltos ou ZIP). Não grava nada em caminhos fixos; gera o Excel em memória
e entrega via botão de download.

Como executar localmente:
    streamlit run sped_analyzer.py
"""

from __future__ import annotations

import io
import os
import re
import unicodedata
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Iterable

import pandas as pd
import streamlit as st

# -------------------------
# Utilidades e normalização
# -------------------------

def norm_text(s: str) -> str:
    if s is None:
        return ''
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ascii', 'ignore').decode('utf-8')
    s = s.lower()
    s = re.sub(r'[\s\./\-_,]+', ' ', s).strip()
    return s

def parse_float_br(value: str) -> float:
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

def detect_encoding_from_bytes(b: bytes) -> str:
    try:
        import chardet  # type: ignore
    except Exception:
        return 'latin-1'
    try:
        result = chardet.detect(b[:20000])
        enc = (result.get('encoding') or 'latin-1').lower()
        conf = float(result.get('confidence') or 0.0)
        if conf < 0.7 or enc in {'ascii'}:
            return 'latin-1'
        return enc
    except Exception:
        return 'latin-1'

# -------------------------
# TIPI
# -------------------------

def load_tipi_table(path: str) -> Dict[str, float]:
    if not path:
        return {}
    if path.lower().endswith('.xlsx'):
        df = pd.read_excel(path)
    elif path.lower().endswith('.csv'):
        # TIPI CSV geralmente ; e , como decimal
        df = pd.read_csv(path, sep=';', decimal=',')
    else:
        raise ValueError("TIPI precisa ser .csv ou .xlsx")
    # normaliza colunas
    cols = {}
    for col in df.columns:
        nc = unicodedata.normalize('NFKD', col)
        nc = nc.encode('ascii', 'ignore').decode('utf-8')
        nc = nc.upper().strip()
        nc = re.sub(r'[^A-Z0-9]', '', nc)
        cols[col] = nc
    df = df.rename(columns=cols)
    if 'NCM' not in df.columns or 'ALIQUOTA' not in df.columns:
        raise KeyError("TIPI precisa conter colunas 'NCM' e 'ALIQUOTA'")
    m: Dict[str, float] = {}
    for _, row in df.iterrows():
        ncm = str(row['NCM']).strip()
        if not ncm:
            continue
        try:
            aliq = float(str(row['ALIQUOTA']).replace(',', '.'))
        except Exception:
            continue
        m[ncm] = aliq
    return m

# -------------------------
# XML (NF-e e CT-e)
# -------------------------

def parse_xml_nfe_bytes(b: bytes) -> Optional[Dict[str, any]]:
    try:
        root = ET.fromstring(b)
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        data: Dict[str, any] = {}
        inf = root.find('.//nfe:infNFe', ns)
        if inf is not None:
            key = inf.get('Id')
            if key and key.startswith('NFe'):
                key = key[3:]
            data['Chave'] = key
        tot = root.find('.//nfe:ICMSTot', ns)
        if tot is not None:
            def f(tag):
                el = tot.find(f'nfe:{tag}', ns)
                return float(el.text) if el is not None and el.text else 0.0
            data['Valor ICMS XML'] = f('vICMS')
            data['Valor IPI XML'] = f('vIPI')
            data['Valor Produtos XML'] = f('vProd')
        emit = root.find('.//nfe:emit', ns)
        if emit is not None:
            xNome = emit.find('nfe:xNome', ns)
            cnpj  = emit.find('nfe:CNPJ', ns)
            data['Emitente XML'] = xNome.text if xNome is not None else 'N/A'
            data['CNPJ Emitente XML'] = cnpj.text if cnpj is not None else 'N/A'
        dest = root.find('.//nfe:dest', ns)
        if dest is not None:
            xNome = dest.find('nfe:xNome', ns)
            cnpj  = dest.find('nfe:CNPJ', ns)
            data['Destinatário XML'] = xNome.text if xNome is not None else 'N/A'
            data['CNPJ Destinatário XML'] = cnpj.text if cnpj is not None else 'N/A'
        return data if 'Chave' in data else None
    except Exception:
        return None

def parse_xml_cte_bytes(b: bytes) -> Optional[Dict[str, any]]:
    try:
        root = ET.fromstring(b)
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
        icms = root.find('.//cte:ICMS/cte:ICMSOutraUF', ns)
        if icms is not None:
            def f(tag):
                el = icms.find(f'cte:{tag}', ns)
                return float(el.text) if el is not None and el.text else 0.0
            data['BC ICMS XML'] = f('vBCOutraUF')
            data['Valor ICMS XML'] = f('vICMSOutraUF')
            data['Alíquota ICMS XML'] = f('pICMSOutraUF')
            cst = icms.find('cte:CST', ns)
            data['CST XML'] = cst.text if cst is not None else 'N/A'
        else:
            anytag = None
            for t in ['ICMS00','ICMS20','ICMS90','ICMS40','ICMS51','ICMS60','ICMS70','ICMSPart','ICMSST','ICMSCons','ICMSUFDest']:
                anytag = root.find(f'.//cte:ICMS/cte:{t}', ns)
                if anytag is not None:
                    break
            if anytag is not None:
                def f(tag):
                    el = anytag.find(f'cte:{tag}', ns)
                    return float(el.text) if el is not None and el.text else 0.0
                data['BC ICMS XML'] = f('vBC')
                data['Valor ICMS XML'] = f('vICMS')
                data['Alíquota ICMS XML'] = f('pICMS')
                cst = anytag.find('cte:CST', ns)
                data['CST XML'] = cst.text if cst is not None else 'N/A'
            else:
                data['BC ICMS XML'] = 0.0
                data['Valor ICMS XML'] = 0.0
                data['Alíquota ICMS XML'] = 0.0
                data['CST XML'] = 'N/A'
        toma3 = root.find('.//cte:toma3/cte:toma', ns)
        toma_value = toma3.text if toma3 is not None else ''
        tipo = 'Não Identificado'
        if toma_value == '0': tipo = 'Remetente'
        elif toma_value == '1': tipo = 'Expedidor'
        elif toma_value == '2': tipo = 'Recebedor'
        elif toma_value == '3': tipo = 'Destinatário'
        data['Tipo Tomador XML'] = tipo
        emit = root.find('.//cte:emit', ns)
        if emit is not None:
            xNome = emit.find('cte:xNome', ns); cnpj = emit.find('cte:CNPJ', ns)
            data['Emitente XML'] = xNome.text if xNome is not None else 'N/A'
            data['CNPJ Emitente XML'] = cnpj.text if cnpj is not None else 'N/A'
        dest = root.find('.//cte:dest', ns)
        if dest is not None:
            xNome = dest.find('cte:xNome', ns); cnpj = dest.find('cte:CNPJ', ns)
            data['Destinatário XML'] = xNome.text if xNome is not None else 'N/A'
            data['CNPJ Destinatário XML'] = cnpj.text if cnpj is not None else 'N/A'
        return data if 'Chave' in data else None
    except Exception:
        return None

# -------------------------
# Estruturas
# -------------------------

class SpedRecord:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.entries: List[dict] = []
        self.outputs: List[dict] = []
        self.items: List[dict] = []
        self.imob_uso: List[dict] = []
        self.cte: List[dict] = []
        self.adjustments: List[dict] = []
        self.st_blocks: List[dict] = []
        self.difal_blocks: List[dict] = []
        self.ipi_blocks: List[dict] = []
        self.missing_c190: List[dict] = []
        self.master_data: dict = {}
        self.block_flags: dict = {}
        self.parsing_warnings: List[str] = []

    def extend(self, other: 'SpedRecord'):
        self.entries += other.entries
        self.outputs += other.outputs
        self.items += other.items
        self.imob_uso += other.imob_uso
        self.cte += other.cte
        self.adjustments += other.adjustments
        self.st_blocks += other.st_blocks
        self.difal_blocks += other.difal_blocks
        self.ipi_blocks += other.ipi_blocks
        self.missing_c190 += other.missing_c190
        self.parsing_warnings += other.parsing_warnings

# -------------------------
# Parser SPED (leitura em memória)
# -------------------------

def parse_sped_bytes(file_name: str, data: bytes, xml_map: Dict[str, Dict[str, any]], tipi: Dict[str, float]) -> SpedRecord:
    rec = SpedRecord(file_name)
    enc = detect_encoding_from_bytes(data)
    text = data.decode(enc, errors='ignore')
    ncm_map: Dict[str, str] = {}
    desc_map: Dict[str, str] = {}
    current_note: Optional[dict] = None
    current_note_key: Optional[str] = None
    current_note_is_entry = False
    current_note_has_c190 = False
    current_cte: Optional[dict] = None
    master = {
        'competence':'','company_name':'','company_cnpj':'','company_ie':'','company_cod_mun':'',
        'company_im':'','company_profile':'','company_status':'','company_activity_type':'',
        'company_trade_name':'','company_phone':'','company_address':'','company_number':'',
        'company_complement':'','company_district':'','company_email':'','ie_substituted':[],
        'accountant_name':'','accountant_cpf':'','accountant_crc':'','accountant_phone':'','accountant_email':''
    }
    flags = {'has_c100_saida':False,'has_st_cfop':False,'has_st_cfop_divergence':False,'has_block_e200':False,'has_difal_cfop':False,'has_block_e300':False,'has_block_g110':False}

    def add_adjustment(reg_type: str, code: str, descr: str, value: float, note_id: Optional[str]=None):
        rec.adjustments.append({
            'Arquivo': file_name, 'Competência': master['competence'],
            'Tipo Registro': reg_type, 'Código Ajuste': code, 'Descrição Ajuste': descr,
            'Valor Ajuste': value, 'Nota': note_id or ''
        })

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or '|' not in line: 
            continue
        parts = line.split('|')
        reg = parts[1] if len(parts)>1 else ''
        # ---- Mestres
        if reg == '0000':
            if len(parts) > 8:
                dt_ini = parts[3] if len(parts)>3 else ''
                dt_fin = parts[4] if len(parts)>4 else ''
                d = dt_ini if (len(dt_ini)==8 and dt_ini.isdigit()) else (dt_fin if (len(dt_fin)==8 and dt_fin.isdigit()) else '')
                if d:
                    master['competence'] = f"{d[2:4]}/{d[4:8]}"
                master['company_name'] = parts[6].strip() if len(parts)>6 else ''
                master['company_cnpj'] = parts[7].strip() if len(parts)>7 else ''
                master['company_ie'] = parts[9].strip() if len(parts)>9 else ''
                master['company_cod_mun'] = parts[10].strip() if len(parts)>10 else ''
                master['company_im'] = parts[11].strip() if len(parts)>11 else ''
                master['company_profile'] = parts[14].strip() if len(parts)>14 else ''
                master['company_status'] = parts[15].strip() if len(parts)>15 else ''
        elif reg == '0002':
            master['company_activity_type'] = parts[2].strip() if len(parts)>2 else ''
        elif reg == '0005':
            if len(parts)>2: master['company_trade_name'] = parts[2].strip()
            if len(parts)>3: master['company_phone'] = parts[3].strip()
            if len(parts)>4: master['company_address'] = parts[4].strip()
            if len(parts)>5: master['company_number'] = parts[5].strip()
            if len(parts)>6: master['company_complement'] = parts[6].strip()
            if len(parts)>7: master['company_district'] = parts[7].strip()
            if len(parts)>10: master['company_email'] = parts[10].strip()
        elif reg == '0015':
            if len(parts)>2 and parts[2].strip():
                master['ie_substituted'].append(parts[2].strip())
        elif reg == '0100':
            if len(parts)>2: master['accountant_name'] = parts[2].strip()
            if len(parts)>3: master['accountant_cpf'] = parts[3].strip()
            if len(parts)>4: master['accountant_crc'] = parts[4].strip()
            if len(parts)>11: master['accountant_phone'] = parts[11].strip()
            if len(parts)>13: master['accountant_email'] = parts[13].strip()

        # ---- 0200
        if reg == '0200':
            cod_item = parts[2].strip() if len(parts)>2 else ''
            descr_item = parts[3].strip() if len(parts)>3 else ''
            ncm = parts[8].strip() if len(parts)>8 else ''
            if cod_item:
                if ncm: ncm_map[cod_item] = ncm
                if descr_item: desc_map[cod_item] = descr_item

        # ---- C100
        if reg == 'C100':
            if current_note is not None and (not current_note_is_entry) and (not current_note_has_c190):
                rec.missing_c190.append(current_note.copy())
            current_note = None; current_note_key = None; current_note_is_entry = False; current_note_has_c190 = False
            if len(parts)>2 and parts[2].strip() in {'0','1'}:
                current_note_is_entry = (parts[2].strip()=='0')
                try:
                    serie = parts[7].strip() if len(parts)>7 else ''
                    numero = parts[8].strip() if len(parts)>8 else ''
                    chave = parts[9].strip() if len(parts)>9 else ''
                    vl_doc = parse_float_br(parts[12]) if len(parts)>12 and parts[12].strip() else (parse_float_br(parts[11]) if len(parts)>11 and parts[11].strip() else 0.0)
                    bc_icms = parse_float_br(parts[21]) if len(parts)>21 and parts[21].strip() else (parse_float_br(parts[20]) if len(parts)>20 and parts[20].strip() else 0.0)
                    vl_icms = parse_float_br(parts[22]) if len(parts)>22 and parts[22].strip() else (parse_float_br(parts[21]) if len(parts)>21 and parts[21].strip() else 0.0)
                    vl_ipi  = parse_float_br(parts[25]) if len(parts)>25 and parts[25].strip() else (parse_float_br(parts[24]) if len(parts)>24 and parts[24].strip() else 0.0)
                    current_note = {
                        'Arquivo': file_name, 'Competência': master['competence'],
                        'CNPJ': master['company_cnpj'], 'Razão Social': master['company_name'],
                        'UF': master['company_cod_mun'], 'Série da nota': serie, 'Número da nota': numero,
                        'Chave': chave, 'Data de emissão': parts[10].strip() if len(parts)>10 else '',
                        'Valor Total Nota': vl_doc, 'BC ICMS Nota': bc_icms, 'Valor ICMS Nota': vl_icms,
                        'Valor IPI Nota': vl_ipi, 'Tipo Nota': ('Entrada' if current_note_is_entry else 'Saída')
                    }
                    current_note_key = chave
                    if not current_note_is_entry: flags['has_c100_saida'] = True
                except Exception:
                    current_note = None; current_note_key=None; current_note_is_entry=False; current_note_has_c190=False

        # ---- C170 (itens)
        if reg == 'C170' and current_note is not None:
            if len(parts) < 25: 
                continue
            try:
                num_item = parts[2].strip(); cod_item = parts[3].strip()
                cfop = parts[11].strip() if len(parts)>11 else ''
                cst_icms = parts[10].strip() if len(parts)>10 else ''
                cst_ipi  = parts[20].strip() if len(parts)>20 else ''
                val_item = parse_float_br(parts[7]) if len(parts)>7 else 0.0
                bc_icms_item = parse_float_br(parts[13]) if len(parts)>13 else 0.0
                aliq_icms_item = parse_float_br(parts[14]) if len(parts)>14 else 0.0
                vl_icms_item = parse_float_br(parts[15]) if len(parts)>15 else 0.0
                aliq_ipi_item = parse_float_br(parts[23]) if len(parts)>23 else 0.0
                vl_ipi_item   = parse_float_br(parts[24]) if len(parts)>24 else 0.0
                eff_aliq = (vl_icms_item/val_item*100.0) if val_item>0 else 0.0
                ncm = ncm_map.get(cod_item, '')
                descr = desc_map.get(cod_item, '')
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
                    exp = tipi[ncm]
                    ipi_status = 'Conforme' if abs(aliq_ipi_item-exp) < 0.001 else f'Divergente (TIPI: {exp:.2f}%)'
                item_rec = current_note.copy()
                item_rec.update({
                    'Num. Item': num_item, 'Cód. Item': cod_item, 'Descrição do Produto': descr,
                    'CFOP': cfop, 'CST ICMS': cst_icms, 'CST IPI': cst_ipi,
                    'Valor Total Item': val_item, 'BC ICMS Item': bc_icms_item,
                    'Alíquota ICMS Item (%)': aliq_icms_item, 'Valor ICMS Item': vl_icms_item,
                    'Alíq. Efetiva (%)': eff_aliq, 'Alíquota IPI Item (%)': aliq_ipi_item,
                    'Valor IPI Item': vl_ipi_item, 'NCM Item': ncm, 'Conformidade IPI x TIPI': ipi_status
                })
                rec.items.append(item_rec)
                if current_note_is_entry:
                    rec.entries.append(item_rec)
                    if cfop.replace('.','') in {'1556','1407','1551','1406','2551','2556','2406','2407'}:
                        uso = item_rec.copy()
                        uso['Situação Crédito'] = ('❌ Crédito indevido sobre Uso/Consumo' if (vl_icms_item>0.001 or vl_ipi_item>0.001) else '✅ Sem crédito indevido')
                        rec.imob_uso.append(uso)
            except Exception:
                pass

        # ---- C190 (saídas)
        if reg == 'C190' and current_note is not None and (not current_note_is_entry):
            current_note_has_c190 = True
            try:
                cst_icms = parts[2].strip() if len(parts)>2 else ''
                cfop = parts[3].strip() if len(parts)>3 else ''
                aliq = parse_float_br(parts[4]) if len(parts)>4 else 0.0
                vl_opr = parse_float_br(parts[5]) if len(parts)>5 else 0.0
                bc_icms = parse_float_br(parts[6]) if len(parts)>6 else 0.0
                vl_icms = parse_float_br(parts[7]) if len(parts)>7 else 0.0
                vl_ipi  = parse_float_br(parts[11]) if len(parts)>11 else 0.0
                eff = (vl_icms/bc_icms*100.0) if bc_icms>0 else 0.0
                out = current_note.copy()
                out.update({'CST ICMS':cst_icms,'CFOP':cfop,'Alíquota ICMS':aliq,'Valor Operação':vl_opr,
                            'BC ICMS':bc_icms,'Valor ICMS':vl_icms,'Alíq. Efetiva (%)':eff,'Valor IPI Nota':vl_ipi})
                rec.outputs.append(out)
                if cfop.replace('.','') in {'5401','5403','5405','6401','6403'}:
                    flags['has_st_cfop'] = True
                if cfop.replace('.','') in {'5401','5403','6403'}:
                    flags['has_st_cfop_divergence'] = True
                if cfop.replace('.','') in {'6107','6108'}:
                    flags['has_difal_cfop'] = True
            except Exception:
                pass

        # ---- D100/D190 (CT-e)
        if reg == 'D100':
            current_cte = None
            try:
                serie = parts[7].strip() if len(parts)>7 else ''
                numero = parts[9].strip() if len(parts)>9 else ''
                chave  = parts[10].strip() if len(parts)>10 else ''
                vl_total = parse_float_br(parts[15]) if len(parts)>15 else 0.0
                bc_icms_cte = parse_float_br(parts[18]) if len(parts)>18 else 0.0
                vl_icms_cte = parse_float_br(parts[20]) if len(parts)>20 else 0.0
                current_cte = {
                    'Arquivo': file_name, 'Competência': master['competence'],
                    'Chave CT-e': chave, 'Série CT-e': serie, 'Número CT-e': numero,
                    'Data de emissão': parts[11].strip() if len(parts)>11 else '',
                    'Valor Total CT-e': vl_total, 'BC ICMS CT-e': bc_icms_cte, 'Valor ICMS CT-e': vl_icms_cte
                }
            except Exception:
                current_cte = None
        if reg == 'D190' and current_cte is not None:
            try:
                cst = parts[2].strip() if len(parts)>2 else ''
                cfop = parts[3].strip() if len(parts)>3 else ''
                aliq = parse_float_br(parts[4]) if len(parts)>4 else 0.0
                vl_opr = parse_float_br(parts[5]) if len(parts)>5 else 0.0
                bc_icms = parse_float_br(parts[6]) if len(parts)>6 else 0.0
                vl_icms = parse_float_br(parts[7]) if len(parts)>7 else 0.0
                eff = (vl_icms/vl_opr*100.0) if vl_opr>0 else 0.0
                row = current_cte.copy()
                row.update({'CST CT-e':cst,'CFOP CT-e':cfop,'Alíquota ICMS CT-e':aliq,
                            'Valor Operação CT-e':vl_opr,'BC ICMS CT-e (D190)':bc_icms,
                            'Valor ICMS CT-e (D190)':vl_icms,'Alíq. Efetiva CT-e (%)':eff,
                            'Valor IPI CT-e':0.0,'NCM Item':'Não se Aplica','Descrição do Produto':'Serviço de Transporte'})
                rec.cte.append(row)
            except Exception:
                pass

        # ---- Ajustes por documento/periodo
        if reg == 'C195' and current_note is not None:
            txt = parts[3].strip() if len(parts)>3 else ''
            if txt:
                rec.adjustments.append({
                    'Arquivo': file_name, 'Competência': master['competence'],
                    'Tipo Registro':'C195','Código Ajuste':'','Descrição Ajuste':txt,
                    'Valor Ajuste':0.0,'Nota': current_note_key or ''
                })
        if reg == 'C197' and current_note is not None:
            code = parts[2].strip() if len(parts)>2 else ''
            descr = parts[3].strip() if len(parts)>3 else ''
            adj_value = 0.0
            for it in parts[4:]:
                v = parse_float_br(it)
                if v>0: adj_value = v
            add_adjustment('C197', code, descr, adj_value, current_note_key)
        if reg == 'E111':
            code = parts[2].strip() if len(parts)>2 else ''
            descr = parts[3].strip() if len(parts)>3 else ''
            value = parse_float_br(parts[4]) if len(parts)>4 else 0.0
            add_adjustment('E111', code, descr, value)
        if reg == 'E115':
            code = parts[2].strip() if len(parts)>2 else ''
            value = parse_float_br(parts[3]) if len(parts)>3 else 0.0
            descr = parts[4].strip() if len(parts)>4 else ''
            add_adjustment('E115', code, descr, value)
        if reg == 'E116':
            cod_or = parts[2].strip() if len(parts)>2 else ''
            value = parse_float_br(parts[3]) if len(parts)>3 else 0.0
            cod_rec = parts[5].strip() if len(parts)>5 else ''
            txt = parts[9].strip() if len(parts)>9 else ''
            descr = f"{cod_or} {cod_rec} {txt}".strip()
            add_adjustment('E116', cod_rec or cod_or, descr, value)

        # ---- Flags de blocos
        if reg.startswith('E2'): flags['has_block_e200'] = True
        if reg.startswith('E3'): flags['has_block_e300'] = True
        if reg == 'G110': flags['has_block_g110'] = True

        # ---- E200/E210
        # Apenas salvamos presença e UF por simplicidade
        if reg == 'E200' and len(parts)>4:
            rec.st_blocks.append({
                'Arquivo': file_name, 'Competência': master['competence'],
                'UF': parts[2].strip(), 'Data Início': parts[3].strip(), 'Data Fim': parts[4].strip()
            })
        # ---- E300/E310/E316 (resumo simplificado)
        if reg == 'E300' and len(parts)>4:
            base = {'Arquivo': file_name, 'Competência': master['competence'],
                    'UF': parts[2].strip(), 'Data Início': parts[3].strip(), 'Data Fim': parts[4].strip()}
        if reg == 'E316' and 'base' in locals():
            row = dict(base)
            row['Código Receita'] = parts[2].strip() if len(parts)>2 else ''
            row['Valor Recolhimento'] = parse_float_br(parts[3]) if len(parts)>3 else 0.0
            row['Data Recolhimento'] = parts[4].strip() if len(parts)>4 else ''
            rec.difal_blocks.append(row)

        # fim loop linhas

    if current_note is not None and (not current_note_is_entry) and (not current_note_has_c190):
        rec.missing_c190.append(current_note.copy())

    rec.master_data = master.copy()
    rec.block_flags = flags.copy()

    # Anexa cruzamento com mapa XML (se houver)
    if xml_map:
        for item in rec.entries:
            key = item.get('Chave')
            if key and key in xml_map:
                x = xml_map[key]
                item['Valor ICMS XML'] = x.get('Valor ICMS XML', 0.0)
                item['Valor IPI XML'] = x.get('Valor IPI XML', 0.0)
                item['Valor Produtos XML'] = x.get('Valor Produtos XML', 0.0)
        for out in rec.outputs:
            key = out.get('Chave')
            if key and key in xml_map:
                x = xml_map[key]
                out['Valor ICMS XML'] = x.get('Valor ICMS XML', 0.0)
                out['Valor IPI XML'] = x.get('Valor IPI XML', 0.0)
                out['Valor Produtos XML'] = x.get('Valor Produtos XML', 0.0)

    return rec

# -------------------------
# Agregações
# -------------------------

def aggregate_records(records: List[SpedRecord]) -> Dict[str, pd.DataFrame]:
    df_entries = pd.DataFrame([r for rec in records for r in rec.entries])
    df_outputs = pd.DataFrame([r for rec in records for r in rec.outputs])
    df_items   = pd.DataFrame([r for rec in records for r in rec.items])
    df_imob    = pd.DataFrame([r for rec in records for r in rec.imob_uso])
    df_cte     = pd.DataFrame([r for rec in records for r in rec.cte])
    df_adj     = pd.DataFrame([r for rec in records for r in rec.adjustments])
    df_st      = pd.DataFrame([r for rec in records for r in rec.st_blocks])
    df_difal   = pd.DataFrame([r for rec in records for r in rec.difal_blocks])
    df_ipi     = pd.DataFrame([r for rec in records for r in rec.ipi_blocks])
    df_missing = pd.DataFrame([r for rec in records for r in rec.missing_c190])
    df_master  = pd.DataFrame([rec.master_data for rec in records])
    df_flags   = pd.DataFrame([rec.block_flags for rec in records])

    sheets: Dict[str, pd.DataFrame] = {}

    if not df_items.empty:
        for c in ['Valor Total Item','BC ICMS Item','Valor ICMS Item','Valor IPI Item']:
            if c in df_items.columns:
                df_items[c] = pd.to_numeric(df_items[c], errors='coerce').fillna(0.0)
        sheets['Detalhes Itens'] = df_items

        # Resumo Itens por NCM-CFOP
        grp = [c for c in ['Tipo Nota','Competência','CNPJ','Razão Social','NCM Item','CFOP'] if c in df_items.columns]
        if grp:
            agg_cols = {c:'sum' for c in ['Valor Total Item','BC ICMS Item','Valor ICMS Item','Valor IPI Item'] if c in df_items.columns}
            df_sum = df_items.groupby(grp).agg(agg_cols).reset_index().rename(columns={
                'Valor Total Item':'Valor Contábil','BC ICMS Item':'BC ICMS','Valor ICMS Item':'ICMS','Valor IPI Item':'IPI'
            })
            sheets['Resumo Itens por NCM-CFOP'] = df_sum

    if not df_entries.empty:
        sheets['Detalhes Entradas'] = df_entries
        for c in ['Valor Total Item','BC ICMS Item','Valor ICMS Item','Valor IPI Item']:
            if c in df_entries.columns:
                df_entries[c] = pd.to_numeric(df_entries[c], errors='coerce').fillna(0.0)
        grp = [c for c in ['Competência','CNPJ','Razão Social','CFOP'] if c in df_entries.columns]
        if grp:
            df_cfop = df_entries.groupby(grp).agg({
                'Valor Total Item':'sum','BC ICMS Item':'sum','Valor ICMS Item':'sum','Valor IPI Item':'sum'
            }).reset_index().rename(columns={
                'Valor Total Item':'Valor Contábil','BC ICMS Item':'BC ICMS','Valor ICMS Item':'ICMS','Valor IPI Item':'IPI'
            })
            sheets['Resumo Entradas por CFOP'] = df_cfop

    if not df_outputs.empty:
        sheets['Detalhes Saídas'] = df_outputs
        for c in ['Valor Total Nota','BC ICMS','Valor ICMS','Valor IPI Nota']:
            if c in df_outputs.columns:
                df_outputs[c] = pd.to_numeric(df_outputs[c], errors='coerce').fillna(0.0)
        grp = [c for c in ['Competência','CNPJ','Razão Social','CFOP','CST ICMS'] if c in df_outputs.columns]
        if grp:
            df_out = df_outputs.groupby(grp).agg({
                'Valor Total Nota':'sum','BC ICMS':'sum','Valor ICMS':'sum','Valor IPI Nota':'sum'
            }).reset_index().rename(columns={
                'Valor Total Nota':'Valor Contábil','BC ICMS':'BC ICMS','Valor ICMS':'ICMS','Valor IPI Nota':'IPI'
            })
            sheets['Resumo Saídas por CFOP-CST'] = df_out

    if not df_imob.empty:
        sheets['Entradas Imob_UsoConsumo'] = df_imob
    if not df_cte.empty:
        sheets['Detalhes CT-e'] = df_cte
    if not df_adj.empty:
        sheets['Ajustes'] = df_adj
    if not df_st.empty:
        sheets['Resumo E200_ICMS_ST'] = df_st
    if not df_difal.empty:
        sheets['Resumo E300_DIFAL'] = df_difal
    if not df_ipi.empty:
        sheets['Resumo E500_IPI'] = df_ipi
    if not df_missing.empty:
        sheets['Notas Saída sem C190'] = df_missing
    if not df_master.empty:
        sheets['Dados Mestres'] = df_master
    if not df_flags.empty:
        sheets['Presença Blocos'] = df_flags

    # DRE Fiscal (simplificada)
    dre_list: List[pd.DataFrame] = []
    def _clean_cfop(x: str) -> str:
        return str(x or '').replace('.','')
    revenue_cfops = {'5101','5102','5403','5405','6101','6102','6403'}
    other_explicit = {'5949','6949','6910','5910'}
    other_prefix = ('59','69')
    cost_cfops = {'2102','2101','2403','2405','1102','1101','1403','1405'}
    expense_cfops = {'2551','1551','1933'}

    def _build_category(df: pd.DataFrame, name: str, test_fn) -> Optional[pd.DataFrame]:
        if df.empty: return None
        m = df['CFOP'].apply(lambda c: test_fn(_clean_cfop(c)))
        sub = df.loc[m].copy()
        if sub.empty: return None
        for c in ['Valor Contábil','ICMS','IPI']:
            if c in sub.columns: sub[c] = pd.to_numeric(sub[c], errors='coerce').fillna(0.0)
        gcols = [c for c in ['Competência','CNPJ','Razão Social'] if c in sub.columns]
        if not gcols: gcols = ['Competência']
        grouped = sub.groupby(gcols).agg({'Valor Contábil':'sum','ICMS':'sum','IPI':'sum'}).reset_index()
        grouped['Categoria'] = name
        grouped['Total Impostos'] = grouped['ICMS'] + grouped['IPI']
        return grouped

    if not df_outputs.empty:
        df_out = df_outputs.copy()
        if 'Valor Contábil' not in df_out.columns:
            df_out['Valor Contábil'] = pd.to_numeric(df_out.get('Valor Total Nota', 0), errors='coerce').fillna(0.0)
        if 'ICMS' not in df_out.columns:
            df_out['ICMS'] = pd.to_numeric(df_out.get('Valor ICMS', 0), errors='coerce').fillna(0.0)
        if 'IPI' not in df_out.columns:
            df_out['IPI'] = pd.to_numeric(df_out.get('Valor IPI Nota', 0), errors='coerce').fillna(0.0)
        dre_rev = _build_category(df_out, 'Receita', lambda c: c in revenue_cfops)
        if dre_rev is not None: dre_list.append(dre_rev)
        dre_out = _build_category(df_out, 'Outras Saídas', lambda c: (c in other_explicit) or any(c.startswith(p) for p in other_prefix))
        if dre_out is not None: dre_list.append(dre_out)

    if not df_entries.empty:
        df_in = df_entries.copy()
        if 'Valor Contábil' not in df_in.columns:
            df_in['Valor Contábil'] = pd.to_numeric(df_in.get('Valor Total Item', 0), errors='coerce').fillna(0.0)
        if 'ICMS' not in df_in.columns:
            df_in['ICMS'] = pd.to_numeric(df_in.get('Valor ICMS Item', 0), errors='coerce').fillna(0.0)
        if 'IPI' not in df_in.columns:
            df_in['IPI'] = pd.to_numeric(df_in.get('Valor IPI Item', 0), errors='coerce').fillna(0.0)
        dre_cost = _build_category(df_in, 'Custos', lambda c: c in cost_cfops)
        if dre_cost is not None: dre_list.append(dre_cost)
        dre_exp = _build_category(df_in, 'Despesas', lambda c: c in expense_cfops)
        if dre_exp is not None: dre_list.append(dre_exp)

    if dre_list:
        df_dre = pd.concat(dre_list, ignore_index=True)
        order = ['Receita','Outras Saídas','Custos','Despesas']
        df_dre['Categoria'] = pd.Categorical(df_dre['Categoria'], categories=order, ordered=True)
        sort_cols = [c for c in ['Competência','CNPJ','Razão Social','Categoria'] if c in df_dre.columns]
        df_dre = df_dre.sort_values(sort_cols).reset_index(drop=True)
        sheets['DRE Fiscal'] = df_dre

        # KPIs
        kpi_df = df_dre.copy()
        kpi_df['Competência'] = kpi_df['Competência'].fillna('Sem competência').astype(str)
        rows = []
        gcols = [c for c in ['Competência','CNPJ','Razão Social'] if c in kpi_df.columns]
        if not gcols: gcols = ['Competência']
        for keys, grp in kpi_df.groupby(gcols):
            if not isinstance(keys, tuple): keys = (keys,)
            key_map = {gcols[i]: keys[i] for i in range(len(gcols))}
            rev = grp.loc[grp['Categoria']=='Receita','Valor Contábil'].sum()
            cost = grp.loc[grp['Categoria']=='Custos','Valor Contábil'].sum()
            taxes = grp.loc[grp['Categoria']=='Receita','Total Impostos'].sum()
            margem = rev - cost
            carga = (taxes/rev*100.0) if rev>0 else 0.0
            row = {'Receita':rev,'Custos':cost,'Margem Bruta':margem,'Total Impostos':taxes,'Carga Tributária Efetiva (%)':carga}
            row.update(key_map)
            rows.append(row)
        df_kpi = pd.DataFrame(rows)
        cols = [c for c in ['Competência','CNPJ','Razão Social'] if c in df_kpi.columns] + ['Receita','Custos','Margem Bruta','Total Impostos','Carga Tributária Efetiva (%)']
        df_kpi = df_kpi[cols]
        sheets['Indicadores Fiscais'] = df_kpi

    return sheets

# -------------------------
# Excel em memória
# -------------------------

def build_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine='xlsxwriter') as writer:
        for name, df in sheets.items():
            wsname = name[:31]
            df.to_excel(writer, sheet_name=wsname, index=False)
            ws = writer.sheets[wsname]
            ws.freeze_panes(1,0); ws.set_zoom(90)
            for i, col in enumerate(df.columns):
                max_len = max((len(str(x)) for x in [col] + df[col].astype(str).tolist()), default=0)
                ws.set_column(i, i, min(max(max_len+2, 12), 60))
    bio.seek(0)
    return bio.read()

# -------------------------
# App Streamlit
# -------------------------

st.set_page_config(page_title="SPED Analyzer ICMS e IPI", layout="centered")
st.title("SPED Analyzer ICMS e IPI")
st.write("Auditoria de arquivos SPED ICMS/IPI (sem gravação em disco).")

sped_files = st.file_uploader("Selecione arquivos SPED (.txt)", type=["txt"], accept_multiple_files=True)
tipi_file  = st.file_uploader("TIPI (CSV/XLSX) (opcional)", type=["csv","xlsx"])
xml_files  = st.file_uploader("XMLs NF-e/CT-e ou ZIP (opcional)", type=["xml","zip"], accept_multiple_files=True)

if st.button("Executar Auditoria"):
    if not sped_files:
        st.error("Selecione pelo menos um arquivo SPED (.txt).")
    else:
        with st.spinner("Processando..."):
            # TIPI
            tipi_map: Dict[str, float] = {}
            if tipi_file is not None:
                with tempfile.NamedTemporaryFile(suffix=os.path.splitext(tipi_file.name)[1]) as tf:
                    tf.write(tipi_file.getbuffer()); tf.flush()
                    try:
                        tipi_map = load_tipi_table(tf.name)
                    except Exception as exc:
                        st.warning(f"Falha ao carregar TIPI: {exc}")
                        tipi_map = {}

            # XMLs
            xml_map: Dict[str, Dict[str, any]] = {}
            for up in (xml_files or []):
                name = up.name.lower()
                if name.endswith('.xml'):
                    try:
                        d = parse_xml_nfe_bytes(up.getbuffer())
                        if not d or 'Chave' not in d:
                            d = parse_xml_cte_bytes(up.getbuffer())
                        if d and 'Chave' in d:
                            xml_map[d['Chave']] = d
                    except Exception:
                        pass
                elif name.endswith('.zip'):
                    try:
                        with zipfile.ZipFile(io.BytesIO(up.getbuffer())) as zf:
                            for nm in zf.namelist():
                                if nm.lower().endswith('.xml'):
                                    try:
                                        b = zf.read(nm)
                                        d = parse_xml_nfe_bytes(b)
                                        if not d or 'Chave' not in d:
                                            d = parse_xml_cte_bytes(b)
                                        if d and 'Chave' in d:
                                            xml_map[d['Chave']] = d
                                    except Exception:
                                        pass
                    except Exception:
                        pass

            # SPEDs
            records: List[SpedRecord] = []
            for up in sped_files:
                try:
                    rec = parse_sped_bytes(up.name, up.getbuffer(), xml_map, tipi_map)
                    records.append(rec)
                except Exception as exc:
                    st.error(f"Erro ao processar {up.name}: {exc}")
            if not records:
                st.error("Nenhum arquivo SPED processado.")
            else:
                sheets = aggregate_records(records)
                excel_bytes = build_excel_bytes(sheets)
                st.success("Relatório gerado com sucesso!")
                st.download_button(
                    "Baixar Excel",
                    data=excel_bytes,
                    file_name="auditoria_sped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                if 'Indicadores Fiscais' in sheets:
                    if st.checkbox("Mostrar Indicadores Fiscais (DRE)"):
                        st.dataframe(sheets['Indicadores Fiscais'])
'''
open('/mnt/data/sped_analyzer.py','w',encoding='utf-8').write(code)
print("Saved to /mnt/data/sped_analyzer.py")
