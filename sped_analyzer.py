# -*- coding: utf-8 -*-
"""
Gerar arquivo Python atualizado conforme solicitado pelo usuário:
- Remover textos em inglês e manter orientações em português
- Garantir que não haja erros de string/aspas
- Unificar CLI + Tkinter + Streamlit (opcional)
O arquivo será salvo em /mnt/data/sped_analyzer.py
"""
from textwrap import dedent

code = dedent(r'''
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
SPED Analyzer ICMS e IPI
========================

Aplicação unificada para auditoria de arquivos SPED Fiscal (EFD ICMS/IPI).
- Leitura de 1 ou mais arquivos .txt do SPED
- (Opcional) Cruzamento com XMLs de NF-e e CT-e (pasta)
- (Opcional) Conferência de TIPI para alíquotas de IPI (CSV/XLSX)
- Geração de planilha Excel com abas detalhadas e resumos
- Modo CLI, GUI (Tkinter) e Web (Streamlit)

Uso (linha de comando):

    python sped_analyzer.py --sped caminho/arquivo1.txt caminho/arquivo2.txt \
        --tipi caminho/tipi.xlsx --xml_dir caminho/pasta_xmls \
        --output resultado.xlsx
"""

import argparse
import os
import re
import sys
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Iterable

import pandas as pd

# Importações opcionais para GUI (Tkinter). Mantidas isoladas.
try:
    import tkinter as _tk
    from tkinter import filedialog as _filedialog, messagebox as _messagebox
    import threading as _threading
except Exception:
    _tk = None
    _filedialog = None
    _messagebox = None
    _threading = None

# ---------------------------------------------------------------------------
# Detecção de codificação
# ---------------------------------------------------------------------------

def detectar_codificacao(caminho_arquivo: str) -> str:
    """
    Detecta a codificação do arquivo. Se não houver chardet, usa latin-1.
    """
    try:
        import chardet  # type: ignore
    except ImportError:
        return "latin-1"

    try:
        with open(caminho_arquivo, "rb") as f:
            bruto = f.read(20000)
        result = chardet.detect(bruto)
        enc = (result.get("encoding") or "latin-1").lower()
        conf = result.get("confidence", 0.0)
        if conf < 0.7 or enc in {"ascii"}:
            return "latin-1"
        return enc
    except Exception:
        return "latin-1"


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def normalizar_texto(s: str) -> str:
    """Normaliza texto para comparações heurísticas."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("utf-8")
    s = s.lower()
    s = re.sub(r"[\\s\\./\\-_,]+", " ", s).strip()
    return s


def parse_float_br(valor: str) -> float:
    """
    Converte número em formato brasileiro (ponto milhar, vírgula decimal) para float.
    Retorna 0.0 em caso de falha.
    """
    if not valor:
        return 0.0
    valor = valor.strip()
    if not valor:
        return 0.0
    valor = valor.replace(".", "").replace(",", ".")
    try:
        return float(valor)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Leitura TIPI
# ---------------------------------------------------------------------------

def carregar_tipi(caminho: str) -> Dict[str, float]:
    """
    Carrega tabela TIPI (CSV/XLSX) contendo colunas 'NCM' e 'ALIQUOTA'.
    Retorna dict {NCM_8_digitos: aliquota_float}.
    """
    if not caminho:
        return {}
    if not os.path.isfile(caminho):
        raise FileNotFoundError(f"Arquivo TIPI não encontrado: {caminho}")
    if caminho.lower().endswith(".xlsx"):
        df = pd.read_excel(caminho)
    elif caminho.lower().endswith(".csv"):
        df = pd.read_csv(caminho, sep=";", decimal=",")
    else:
        raise ValueError("Formato TIPI não suportado; use .csv ou .xlsx")

    # Normaliza nomes das colunas
    ren = {}
    for col in df.columns:
        nc = unicodedata.normalize("NFKD", col).encode("ascii", "ignore").decode("utf-8")
        nc = re.sub(r"[^A-Za-z0-9]", "", nc).upper()
        ren[col] = nc
    df = df.rename(columns=ren)
    if "NCM" not in df.columns or "ALIQUOTA" not in df.columns:
        raise KeyError("TIPI deve conter colunas 'NCM' e 'ALIQUOTA'.")

    mapa: Dict[str, float] = {}
    for _, row in df.iterrows():
        ncm = str(row["NCM"]).strip()
        if not ncm:
            continue
        try:
            aliq = float(str(row["ALIQUOTA"]).replace(",", "."))
        except Exception:
            continue
        mapa[ncm] = aliq
    return mapa


# ---------------------------------------------------------------------------
# XML NF-e / CT-e
# ---------------------------------------------------------------------------

def parse_xml_nfe(caminho: str) -> Optional[Dict[str, object]]:
    """Extrai totais e partes de um XML de NF-e."""
    try:
        tree = ET.parse(caminho)
        root = tree.getroot()
        ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
        dados: Dict[str, object] = {}

        inf = root.find(".//nfe:infNFe", ns)
        if inf is not None:
            chave = inf.get("Id")
            if chave and chave.startswith("NFe"):
                chave = chave[3:]
            dados["Chave"] = chave

        tot = root.find(".//nfe:ICMSTot", ns)
        if tot is not None:
            vICMS = tot.find("nfe:vICMS", ns)
            dados["Valor ICMS XML"] = float(vICMS.text) if vICMS is not None and vICMS.text else 0.0
            vIPI = tot.find("nfe:vIPI", ns)
            dados["Valor IPI XML"] = float(vIPI.text) if vIPI is not None and vIPI.text else 0.0
            vProd = tot.find("nfe:vProd", ns)
            dados["Valor Produtos XML"] = float(vProd.text) if vProd is not None and vProd.text else 0.0

        emit = root.find(".//nfe:emit", ns)
        if emit is not None:
            xNome = emit.find("nfe:xNome", ns)
            CNPJ = emit.find("nfe:CNPJ", ns)
            dados["Emitente XML"] = xNome.text if xNome is not None else "N/A"
            dados["CNPJ Emitente XML"] = CNPJ.text if CNPJ is not None else "N/A"

        dest = root.find(".//nfe:dest", ns)
        if dest is not None:
            xNome = dest.find("nfe:xNome", ns)
            CNPJ = dest.find("nfe:CNPJ", ns)
            dados["Destinatário XML"] = xNome.text if xNome is not None else "N/A"
            dados["CNPJ Destinatário XML"] = CNPJ.text if CNPJ is not None else "N/A"

        return dados if "Chave" in dados else None
    except Exception:
        return None


def parse_xml_cte(caminho: str) -> Optional[Dict[str, object]]:
    """Extrai totais e partes de um XML de CT-e."""
    try:
        tree = ET.parse(caminho)
        root = tree.getroot()
        ns = {"cte": "http://www.portalfiscal.inf.br/cte"}
        dados: Dict[str, object] = {}

        inf = root.find(".//cte:infCte", ns)
        if inf is not None:
            chave = inf.get("Id")
            if chave and chave.startswith("CTe"):
                chave = chave[3:]
            dados["Chave"] = chave

        vPrest = root.find(".//cte:vPrest", ns)
        if vPrest is not None:
            v = vPrest.find("cte:vTPrest", ns)
            dados["Valor Total Prestação XML"] = float(v.text) if v is not None and v.text else 0.0

        icms = root.find(".//cte:ICMS/cte:ICMSOutraUF", ns)
        if icms is not None:
            vbc = icms.find("cte:vBCOutraUF", ns)
            vicms = icms.find("cte:vICMSOutraUF", ns)
            picms = icms.find("cte:pICMSOutraUF", ns)
            cst = icms.find("cte:CST", ns)
            dados["BC ICMS XML"] = float(vbc.text) if vbc is not None and vbc.text else 0.0
            dados["Valor ICMS XML"] = float(vicms.text) if vicms is not None and vicms.text else 0.0
            dados["Alíquota ICMS XML"] = float(picms.text) if picms is not None and picms.text else 0.0
            dados["CST XML"] = cst.text if cst is not None else "N/A"
        else:
            tipo = None
            for t in ["ICMS00", "ICMS20", "ICMS90", "ICMS40", "ICMS51", "ICMS60", "ICMS70", "ICMSPart", "ICMSST", "ICMSCons", "ICMSUFDest"]:
                tipo = root.find(f".//cte:ICMS/cte:{t}", ns)
                if tipo is not None:
                    break
            if tipo is not None:
                vbc = tipo.find("cte:vBC", ns)
                vicms = tipo.find("cte:vICMS", ns)
                picms = tipo.find("cte:pICMS", ns)
                cst = tipo.find("cte:CST", ns)
                dados["BC ICMS XML"] = float(vbc.text) if vbc is not None and vbc.text else 0.0
                dados["Valor ICMS XML"] = float(vicms.text) if vicms is not None and vicms.text else 0.0
                dados["Alíquota ICMS XML"] = float(picms.text) if picms is not None and picms.text else 0.0
                dados["CST XML"] = cst.text if cst is not None else "N/A"
            else:
                dados["BC ICMS XML"] = 0.0
                dados["Valor ICMS XML"] = 0.0
                dados["Alíquota ICMS XML"] = 0.0
                dados["CST XML"] = "N/A"

        toma3 = root.find(".//cte:toma3/cte:toma", ns)
        toma_val = toma3.text if toma3 is not None else ""
        tipo_tom = "Não Identificado"
        nome_tom = "N/A"
        if toma_val == "0":
            tipo_tom = "Remetente"
            rem = root.find(".//cte:rem", ns)
            if rem is not None:
                x = rem.find("cte:xNome", ns)
                nome_tom = x.text if x is not None else "N/A"
        elif toma_val == "1":
            tipo_tom = "Expedidor"
            no = root.find(".//cte:exped", ns)
            if no is not None:
                x = no.find("cte:xNome", ns)
                nome_tom = x.text if x is not None else "N/A"
        elif toma_val == "2":
            tipo_tom = "Recebedor"
            no = root.find(".//cte:receb", ns)
            if no is not None:
                x = no.find("cte:xNome", ns)
                nome_tom = x.text if x is not None else "N/A"
        elif toma_val == "3":
            tipo_tom = "Destinatário"
            no = root.find(".//cte:dest", ns)
            if no is not None:
                x = no.find("cte:xNome", ns)
                nome_tom = x.text if x is not None else "N/A"
        dados["Tipo Tomador XML"] = tipo_tom
        dados["Nome Tomador XML"] = nome_tom

        emit = root.find(".//cte:emit", ns)
        if emit is not None:
            x = emit.find("cte:xNome", ns)
            c = emit.find("cte:CNPJ", ns)
            dados["Emitente XML"] = x.text if x is not None else "N/A"
            dados["CNPJ Emitente XML"] = c.text if c is not None else "N/A"

        dest = root.find(".//cte:dest", ns)
        if dest is not None:
            x = dest.find("cte:xNome", ns)
            c = dest.find("cte:CNPJ", ns)
            dados["Destinatário XML"] = x.text if x is not None else "N/A"
            dados["CNPJ Destinatário XML"] = c.text if c is not None else "N/A"

        return dados if "Chave" in dados else None
    except Exception:
        return None


def parse_xml_diretorio(pasta: str) -> Dict[str, Dict[str, object]]:
    """Lê todos XMLs de uma pasta e monta mapa por chave (NF-e/CT-e)."""
    res: Dict[str, Dict[str, object]] = {}
    if not pasta or not os.path.isdir(pasta):
        return res
    for raiz, _, arquivos in os.walk(pasta):
        for nome in arquivos:
            if not nome.lower().endswith(".xml"):
                continue
            caminho = os.path.join(raiz, nome)
            d = parse_xml_nfe(caminho)
            if d and "Chave" in d:
                res[d["Chave"]] = d
                continue
            d = parse_xml_cte(caminho)
            if d and "Chave" in d:
                res[d["Chave"]] = d
    return res


# ---------------------------------------------------------------------------
# Parser SPED
# ---------------------------------------------------------------------------

class SpedRecord:
    """Estrutura de dados para um arquivo SPED."""
    def __init__(self, caminho: str):
        self.caminho = caminho
        self.entradas: List[dict] = []
        self.saidas: List[dict] = []
        self.imob_uso: List[dict] = []
        self.cte: List[dict] = []
        self.ajustes: List[dict] = []
        self.bloco_st: List[dict] = []
        self.bloco_difal: List[dict] = []
        self.bloco_ipi: List[dict] = []
        self.mestres: dict = {}
        self.flags: dict = {}
        self.saida_sem_c190: List[dict] = []
        self.itens: List[dict] = []
        self.alertas: List[str] = []

    def merge(self, outro: "SpedRecord") -> None:
        self.entradas += outro.entradas
        self.saidas += outro.saidas
        self.imob_uso += outro.imob_uso
        self.cte += outro.cte
        self.ajustes += outro.ajustes
        self.bloco_st += outro.bloco_st
        self.bloco_difal += outro.bloco_difal
        self.bloco_ipi += outro.bloco_ipi
        self.saida_sem_c190 += outro.saida_sem_c190
        self.itens += outro.itens
        self.alertas += outro.alertas


def parse_sped(caminho: str, mapa_xml: Dict[str, Dict[str, object]], tipi: Dict[str, float]) -> SpedRecord:
    """Processa um arquivo SPED e retorna SpedRecord com dados capturados."""
    rec = SpedRecord(caminho)
    enc = detectar_codificacao(caminho)
    try:
        with open(caminho, "r", encoding=enc, errors="ignore") as f:
            ncm_map: Dict[str, str] = {}
            desc_map: Dict[str, str] = {}
            nota_atual: Optional[dict] = None
            chave_atual: Optional[str] = None
            nota_e_entrada: bool = False
            nota_tem_c190: bool = False
            cte_atual: Optional[dict] = None
            e200_atual = None
            e300_atual = None
            e310_atual = None
            e500_atual = None
            mestres = {
                "competencia": "",
                "razao": "",
                "cnpj": "",
                "ie": "",
                "cod_mun": "",
                "im": "",
                "perfil": "",
                "status": "",
                "tp_ativ": "",
                "fantasia": "",
                "fone": "",
                "endereco": "",
                "numero": "",
                "compl": "",
                "bairro": "",
                "email": "",
                "ie_subs": [],
                "cont_nome": "",
                "cont_cpf": "",
                "cont_crc": "",
                "cont_fone": "",
                "cont_email": "",
            }
            flags = {
                "tem_c100_saida": False,
                "tem_st_cfop": False,
                "tem_st_div": False,
                "tem_bloco_e200": False,
                "tem_difal_cfop": False,
                "tem_bloco_e300": False,
                "tem_bloco_g110": False,
            }

            def add_ajuste(tp: str, cod: str, descr: str, valor: float, nota: str | None = None):
                rec.ajustes.append({
                    "Arquivo": os.path.basename(caminho),
                    "Competência": mestres["competencia"],
                    "Tipo Registro": tp,
                    "Código Ajuste": cod,
                    "Descrição Ajuste": descr,
                    "Valor Ajuste": valor,
                    "Nota": nota or "",
                })

            for bruta in f:
                linha = bruta.strip()
                if not linha or "|" not in linha:
                    continue
                partes = linha.split("|")
                reg = partes[1] if len(partes) > 1 else ""

                # 0000
                if reg == "0000" and len(partes) > 8:
                    dt_ini = partes[3] if len(partes) > 3 else ""
                    dt_fin = partes[4] if len(partes) > 4 else ""
                    fonte = ""
                    if len(dt_ini) == 8 and dt_ini.isdigit():
                        fonte = dt_ini
                    elif len(dt_fin) == 8 and dt_fin.isdigit():
                        fonte = dt_fin
                    if fonte:
                        mes, ano = fonte[2:4], fonte[4:8]
                        mestres["competencia"] = f"{mes}/{ano}"
                    mestres["razao"] = partes[6].strip() if len(partes) > 6 else ""
                    mestres["cnpj"] = partes[7].strip() if len(partes) > 7 else ""
                    mestres["ie"] = partes[9].strip() if len(partes) > 9 else ""
                    mestres["cod_mun"] = partes[10].strip() if len(partes) > 10 else ""
                    mestres["im"] = partes[11].strip() if len(partes) > 11 else ""
                    mestres["perfil"] = partes[14].strip() if len(partes) > 14 else ""
                    mestres["status"] = partes[15].strip() if len(partes) > 15 else ""

                elif reg == "0002" and len(partes) > 2:
                    mestres["tp_ativ"] = partes[2].strip()

                elif reg == "0005":
                    if len(partes) > 2:
                        mestres["fantasia"] = partes[2].strip()
                    if len(partes) > 3:
                        mestres["fone"] = partes[3].strip()
                    if len(partes) > 4:
                        mestres["endereco"] = partes[4].strip()
                    if len(partes) > 5:
                        mestres["numero"] = partes[5].strip()
                    if len(partes) > 6:
                        mestres["compl"] = partes[6].strip()
                    if len(partes) > 7:
                        mestres["bairro"] = partes[7].strip()
                    if len(partes) > 10:
                        mestres["email"] = partes[10].strip()

                elif reg == "0015" and len(partes) > 2:
                    ie_sub = partes[2].strip()
                    if ie_sub:
                        mestres["ie_subs"].append(ie_sub)

                elif reg == "0100" and len(partes) > 4:
                    mestres["cont_nome"] = partes[2].strip()
                    mestres["cont_cpf"] = partes[3].strip() if len(partes) > 3 else ""
                    mestres["cont_crc"] = partes[4].strip() if len(partes) > 4 else ""
                    mestres["cont_fone"] = partes[11].strip() if len(partes) > 11 else ""
                    mestres["cont_email"] = partes[13].strip() if len(partes) > 13 else ""

                # 0200
                if reg == "0200":
                    cod_item = partes[2].strip() if len(partes) > 2 else ""
                    descr_item = partes[3].strip() if len(partes) > 3 else ""
                    ncm = partes[8].strip() if len(partes) > 8 else ""
                    if cod_item:
                        if ncm:
                            ncm_map[cod_item] = ncm
                        if descr_item:
                            desc_map[cod_item] = descr_item

                # C100
                if reg == "C100":
                    if nota_atual is not None and (not nota_e_entrada) and (not nota_tem_c190):
                        rec.saida_sem_c190.append(nota_atual.copy())
                    nota_atual = None
                    chave_atual = None
                    nota_e_entrada = False
                    nota_tem_c190 = False

                    if len(partes) > 2:
                        ind_oper = partes[2].strip()
                        if ind_oper in {"0", "1"}:
                            nota_e_entrada = (ind_oper == "0")
                            try:
                                serie = partes[7].strip() if len(partes) > 7 else ""
                                numero = partes[8].strip() if len(partes) > 8 else ""
                                chave = partes[9].strip() if len(partes) > 9 else ""
                                vl_doc = 0.0
                                if len(partes) > 12 and partes[12].strip():
                                    vl_doc = parse_float_br(partes[12])
                                elif len(partes) > 11 and partes[11].strip():
                                    vl_doc = parse_float_br(partes[11])

                                bc_icms = 0.0
                                if len(partes) > 21 and partes[21].strip():
                                    bc_icms = parse_float_br(partes[21])
                                elif len(partes) > 20 and partes[20].strip():
                                    bc_icms = parse_float_br(partes[20])

                                vl_icms = 0.0
                                if len(partes) > 22 and partes[22].strip():
                                    vl_icms = parse_float_br(partes[22])
                                elif len(partes) > 21 and partes[21].strip():
                                    vl_icms = parse_float_br(partes[21])

                                vl_ipi = 0.0
                                if len(partes) > 25 and partes[25].strip():
                                    vl_ipi = parse_float_br(partes[25])
                                elif len(partes) > 24 and partes[24].strip():
                                    vl_ipi = parse_float_br(partes[24])

                                nota_atual = {
                                    "Arquivo": os.path.basename(caminho),
                                    "Competência": mestres["competencia"],
                                    "CNPJ": mestres["cnpj"],
                                    "Razão Social": mestres["razao"],
                                    "UF": mestres["cod_mun"],
                                    "Série da nota": serie,
                                    "Número da nota": numero,
                                    "Chave": chave,
                                    "Data de emissão": partes[10].strip() if len(partes) > 10 else "",
                                    "Valor Total Nota": vl_doc,
                                    "BC ICMS Nota": bc_icms,
                                    "Valor ICMS Nota": vl_icms,
                                    "Valor IPI Nota": vl_ipi,
                                    "Tipo Nota": "Entrada" if nota_e_entrada else "Saída",
                                }
                                chave_atual = chave
                                if not nota_e_entrada:
                                    flags["tem_c100_saida"] = True
                            except Exception:
                                nota_atual = None
                                chave_atual = None
                                nota_e_entrada = False
                                nota_tem_c190 = False

                # C170 (itens)
                if reg == "C170" and nota_atual is not None:
                    if len(partes) < 25:
                        continue
                    try:
                        num_item = partes[2].strip()
                        cod_item = partes[3].strip()
                        cfop = partes[11].strip() if len(partes) > 11 else ""
                        cst_icms = partes[10].strip() if len(partes) > 10 else ""
                        cst_ipi = partes[20].strip() if len(partes) > 20 else ""
                        val_item = parse_float_br(partes[7]) if len(partes) > 7 else 0.0
                        bc_icms_item = parse_float_br(partes[13]) if len(partes) > 13 else 0.0
                        aliq_icms_item = parse_float_br(partes[14]) if len(partes) > 14 else 0.0
                        vl_icms_item = parse_float_br(partes[15]) if len(partes) > 15 else 0.0
                        aliq_ipi_item = parse_float_br(partes[23]) if len(partes) > 23 else 0.0
                        vl_ipi_item = parse_float_br(partes[24]) if len(partes) > 24 else 0.0
                        eff_aliq = (vl_icms_item / val_item) * 100.0 if val_item > 0 else 0.0

                        ncm = ncm_map.get(cod_item, "")
                        descr = desc_map.get(cod_item, "")

                        status_ipi = ""
                        if aliq_ipi_item == 0.0:
                            status_ipi = "Conforme"
                        elif not tipi:
                            status_ipi = "TIPI não carregada"
                        elif not ncm:
                            status_ipi = "NCM não encontrado"
                        elif ncm not in tipi:
                            status_ipi = "NCM não encontrado na TIPI"
                        else:
                            esperado = tipi[ncm]
                            status_ipi = "Conforme" if abs(aliq_ipi_item - esperado) < 0.001 else f"Divergente (TIPI: {esperado:.2f}%)"

                        item_reg = nota_atual.copy()
                        item_reg.update({
                            "Num. Item": num_item,
                            "Cód. Item": cod_item,
                            "Descrição do Produto": descr,
                            "CFOP": cfop,
                            "CST ICMS": cst_icms,
                            "CST IPI": cst_ipi,
                            "Valor Total Item": val_item,
                            "BC ICMS Item": bc_icms_item,
                            "Alíquota ICMS Item (%)": aliq_icms_item,
                            "Valor ICMS Item": vl_icms_item,
                            "Alíq. Efetiva (%)": eff_aliq,
                            "Alíquota IPI Item (%)": aliq_ipi_item,
                            "Valor IPI Item": vl_ipi_item,
                            "NCM Item": ncm,
                            "Conformidade IPI x TIPI": status_ipi,
                        })
                        rec.itens.append(item_reg)

                        if nota_e_entrada:
                            rec.entradas.append(item_reg)
                            if cfop.replace(".", "") in {"1556", "1407", "1551", "1406", "2551", "2556", "2406", "2407"}:
                                uso = item_reg.copy()
                                if vl_icms_item > 0.001 or vl_ipi_item > 0.001:
                                    uso["Situação Crédito"] = "❌ Crédito indevido (Uso e Consumo)"
                                else:
                                    uso["Situação Crédito"] = "✅ Sem crédito indevido (Uso e Consumo)"
                                rec.imob_uso.append(uso)
                    except Exception:
                        pass

                # C190 (resumo saídas)
                if reg == "C190" and nota_atual is not None and (not nota_e_entrada):
                    nota_tem_c190 = True
                    try:
                        cst_icms = partes[2].strip() if len(partes) > 2 else ""
                        cfop = partes[3].strip() if len(partes) > 3 else ""
                        aliq = parse_float_br(partes[4]) if len(partes) > 4 else 0.0
                        vl_opr = parse_float_br(partes[5]) if len(partes) > 5 else 0.0
                        bc_icms = parse_float_br(partes[6]) if len(partes) > 6 else 0.0
                        vl_icms = parse_float_br(partes[7]) if len(partes) > 7 else 0.0
                        vl_ipi = parse_float_br(partes[11]) if len(partes) > 11 else 0.0
                        eff = (vl_icms / bc_icms) * 100.0 if bc_icms > 0 else 0.0
                        out_reg = nota_atual.copy()
                        out_reg.update({
                            "CST ICMS": cst_icms,
                            "CFOP": cfop,
                            "Alíquota ICMS": aliq,
                            "Valor Operação": vl_opr,
                            "BC ICMS": bc_icms,
                            "Valor ICMS": vl_icms,
                            "Alíq. Efetiva (%)": eff,
                            "Valor IPI Nota": vl_ipi,
                        })
                        rec.saidas.append(out_reg)

                        if cfop.replace(".", "") in {"5401", "5403", "5405", "6401", "6403"}:
                            flags["tem_st_cfop"] = True
                        if cfop.replace(".", "") in {"5401", "5403", "6403"}:
                            flags["tem_st_div"] = True
                        if cfop.replace(".", "") in {"6107", "6108"}:
                            flags["tem_difal_cfop"] = True
                    except Exception:
                        pass

                # D100 / D190 (CT-e)
                if reg == "D100":
                    cte_atual = None
                    try:
                        serie = partes[7].strip() if len(partes) > 7 else ""
                        numero = partes[9].strip() if len(partes) > 9 else ""
                        chave = partes[10].strip() if len(partes) > 10 else ""
                        vl_tot = parse_float_br(partes[15]) if len(partes) > 15 else 0.0
                        bc_icms_cte = parse_float_br(partes[18]) if len(partes) > 18 else 0.0
                        vl_icms_cte = parse_float_br(partes[20]) if len(partes) > 20 else 0.0
                        cte_atual = {
                            "Arquivo": os.path.basename(caminho),
                            "Competência": mestres["competencia"],
                            "Chave CT-e": chave,
                            "Série CT-e": serie,
                            "Número CT-e": numero,
                            "Data de emissão": partes[11].strip() if len(partes) > 11 else "",
                            "Valor Total CT-e": vl_tot,
                            "BC ICMS CT-e": bc_icms_cte,
                            "Valor ICMS CT-e": vl_icms_cte,
                        }
                    except Exception:
                        cte_atual = None

                if reg == "D190" and cte_atual is not None:
                    try:
                        cst = partes[2].strip() if len(partes) > 2 else ""
                        cfop = partes[3].strip() if len(partes) > 3 else ""
                        aliq = parse_float_br(partes[4]) if len(partes) > 4 else 0.0
                        vl_opr = parse_float_br(partes[5]) if len(partes) > 5 else 0.0
                        bc_icms = parse_float_br(partes[6]) if len(partes) > 6 else 0.0
                        vl_icms = parse_float_br(partes[7]) if len(partes) > 7 else 0.0
                        eff = (vl_icms / vl_opr) * 100.0 if vl_opr > 0 else 0.0
                        cte_reg = cte_atual.copy()
                        cte_reg.update({
                            "CST CT-e": cst,
                            "CFOP CT-e": cfop,
                            "Alíquota ICMS CT-e": aliq,
                            "Valor Operação CT-e": vl_opr,
                            "BC ICMS CT-e (D190)": bc_icms,
                            "Valor ICMS CT-e (D190)": vl_icms,
                            "Alíq. Efetiva CT-e (%)": eff,
                            "Valor IPI CT-e": 0.0,
                            "NCM Item": "Não se Aplica",
                            "Descrição do Produto": "Serviço de Transporte",
                        })
                        rec.cte.append(cte_reg)
                    except Exception:
                        pass

                # C195 / C197
                if reg == "C195" and nota_atual is not None:
                    txt = partes[3].strip() if len(partes) > 3 else ""
                    if txt:
                        rec.ajustes.append({
                            "Arquivo": os.path.basename(caminho),
                            "Competência": mestres["competencia"],
                            "Tipo Registro": "C195",
                            "Código Ajuste": "",
                            "Descrição Ajuste": txt,
                            "Valor Ajuste": 0.0,
                            "Nota": chave_atual or "",
                        })

                if reg == "C197" and nota_atual is not None:
                    cod = partes[2].strip() if len(partes) > 2 else ""
                    descr = partes[3].strip() if len(partes) > 3 else ""
                    val = 0.0
                    for it in partes[4:]:
                        v = parse_float_br(it)
                        if v > 0:
                            val = v
                    add_ajuste("C197", cod, descr, val, chave_atual)

                # E111 / E115 / E116
                if reg == "E111":
                    cod = partes[2].strip() if len(partes) > 2 else ""
                    descr = partes[3].strip() if len(partes) > 3 else ""
                    val = parse_float_br(partes[4]) if len(partes) > 4 else 0.0
                    add_ajuste("E111", cod, descr, val)

                if reg == "E115":
                    cod = partes[2].strip() if len(partes) > 2 else ""
                    val = parse_float_br(partes[3]) if len(partes) > 3 else 0.0
                    descr = partes[4].strip() if len(partes) > 4 else ""
                    add_ajuste("E115", cod, descr, val)

                if reg == "E116":
                    cod_or = partes[2].strip() if len(partes) > 2 else ""
                    val = parse_float_br(partes[3]) if len(partes) > 3 else 0.0
                    cod_rec = partes[5].strip() if len(partes) > 5 else ""
                    txt = partes[9].strip() if len(partes) > 9 else ""
                    descr = f"{cod_or} {cod_rec} {txt}".strip()
                    add_ajuste("E116", cod_rec or cod_or, descr, val)

                # Flags
                if reg.startswith("E2"):
                    flags["tem_bloco_e200"] = True
                if reg.startswith("E3"):
                    flags["tem_bloco_e300"] = True
                if reg == "G110":
                    flags["tem_bloco_g110"] = True

                # E200 / E210
                if reg == "E200" and len(partes) > 4:
                    e200_atual = {
                        "Arquivo": os.path.basename(caminho),
                        "Competência": mestres["competencia"],
                        "UF": partes[2].strip() if len(partes) > 2 else "",
                        "Data Início": partes[3].strip() if len(partes) > 3 else "",
                        "Data Fim": partes[4].strip() if len(partes) > 4 else "",
                        "Ind Mov": "",
                    }
                if reg == "E210" and e200_atual is not None:
                    e200_atual["Ind Mov"] = partes[2].strip() if len(partes) > 2 else ""
                    rec.bloco_st.append(e200_atual.copy())

                # E300 / E310 / E316
                if reg == "E300" and len(partes) > 4:
                    e300_atual = {
                        "Arquivo": os.path.basename(caminho),
                        "Competência": mestres["competencia"],
                        "UF": partes[2].strip() if len(partes) > 2 else "",
                        "Data Início": partes[3].strip() if len(partes) > 3 else "",
                        "Data Fim": partes[4].strip() if len(partes) > 4 else "",
                        "Ind Mov": "",
                    }
                    e310_atual = None

                if reg == "E310" and e300_atual is not None:
                    e300_atual["Ind Mov"] = partes[2].strip() if len(partes) > 2 else ""
                    vl_apurado = parse_float_br(partes[9]) if len(partes) > 9 else 0.0
                    e310_atual = e300_atual.copy()
                    e310_atual["Saldo Apurado"] = vl_apurado

                if reg == "E316" and e310_atual is not None:
                    cod_rec = partes[2].strip() if len(partes) > 2 else ""
                    vl_rec = parse_float_br(partes[3]) if len(partes) > 3 else 0.0
                    dt_rec = partes[4].strip() if len(partes) > 4 else ""
                    e310_atual["Código Receita"] = cod_rec
                    e310_atual["Valor Recolhimento"] = vl_rec
                    e310_atual["Data Recolhimento"] = dt_rec
                    rec.bloco_difal.append(e310_atual.copy())

                # E500 / E510
                if reg == "E500":
                    e500_atual = {
                        "Arquivo": os.path.basename(caminho),
                        "Competência": mestres["competencia"],
                        "Ind Apur": partes[2].strip() if len(partes) > 2 else "",
                        "Data Início": partes[3].strip() if len(partes) > 3 else "",
                        "Data Fim": partes[4].strip() if len(partes) > 4 else "",
                    }
                if reg == "E510" and e500_atual is not None:
                    cfop = partes[2].strip() if len(partes) > 2 else ""
                    cst = partes[3].strip() if len(partes) > 3 else ""
                    vl_cont = parse_float_br(partes[4]) if len(partes) > 4 else 0.0
                    vl_bc = parse_float_br(partes[5]) if len(partes) > 5 else 0.0
                    vl_ipi = parse_float_br(partes[6]) if len(partes) > 6 else 0.0
                    e5 = e500_atual.copy()
                    e5.update({
                        "CFOP": cfop,
                        "CST IPI": cst,
                        "Valor Contábil IPI": vl_cont,
                        "Base IPI": vl_bc,
                        "Valor IPI": vl_ipi,
                    })
                    rec.bloco_ipi.append(e5)

            # Flush nota saída sem C190
            if nota_atual is not None and (not nota_e_entrada) and (not nota_tem_c190):
                rec.saida_sem_c190.append(nota_atual.copy())

        rec.mestres = mestres.copy()
        rec.flags = flags.copy()

        # Anexa cruzamento XML
        if mapa_xml:
            for ent in rec.entradas:
                chave = ent.get("Chave")
                if chave and chave in mapa_xml:
                    x = mapa_xml[chave]
                    ent["Valor ICMS XML"] = x.get("Valor ICMS XML", 0.0)
                    ent["Valor IPI XML"] = x.get("Valor IPI XML", 0.0)
                    ent["Valor Produtos XML"] = x.get("Valor Produtos XML", 0.0)
            for sai in rec.saidas:
                chave = sai.get("Chave")
                if chave and chave in mapa_xml:
                    x = mapa_xml[chave]
                    sai["Valor ICMS XML"] = x.get("Valor ICMS XML", 0.0)
                    sai["Valor IPI XML"] = x.get("Valor IPI XML", 0.0)
                    sai["Valor Produtos XML"] = x.get("Valor Produtos XML", 0.0)

    except Exception as exc:
        rec.alertas.append(f"Erro ao processar {os.path.basename(caminho)}: {exc}")
    return rec


# ---------------------------------------------------------------------------
# Agregações e Relatórios (DataFrames)
# ---------------------------------------------------------------------------

def agregar_registros(registros: List[SpedRecord]) -> Dict[str, pd.DataFrame]:
    """Agrega múltiplos SpedRecord em dicionário de DataFrames por aba."""
    df_ent = pd.DataFrame([r for rec in registros for r in rec.entradas])
    df_sai = pd.DataFrame([r for rec in registros for r in rec.saidas])
    df_itens = pd.DataFrame([r for rec in registros for r in rec.itens])
    df_imob = pd.DataFrame([r for rec in registros for r in rec.imob_uso])
    df_cte = pd.DataFrame([r for rec in registros for r in rec.cte])
    df_aj = pd.DataFrame([r for rec in registros for r in rec.ajustes])
    df_st = pd.DataFrame([r for rec in registros for r in rec.bloco_st])
    df_difal = pd.DataFrame([r for rec in registros for r in rec.bloco_difal])
    df_ipi = pd.DataFrame([r for rec in registros for r in rec.bloco_ipi])
    df_sem_c190 = pd.DataFrame([r for rec in registros for r in rec.saida_sem_c190])
    df_mestres = pd.DataFrame([rec.mestres for rec in registros])
    df_flags = pd.DataFrame([rec.flags for rec in registros])

    abas: Dict[str, pd.DataFrame] = {}

    # Itens (entradas + saídas)
    if not df_itens.empty:
        for col in ["Valor Total Item", "BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"]:
            if col in df_itens.columns:
                df_itens[col] = pd.to_numeric(df_itens[col], errors="coerce").fillna(0.0)
        abas["Detalhes Itens"] = df_itens

        # Resumo por Nota/NCM/CFOP (com CNPJ/Razão/Competência)
        grp_cols = [c for c in ["Tipo Nota", "Competência", "CNPJ", "Razão Social", "NCM Item", "CFOP"] if c in df_itens.columns]
        if grp_cols:
            agg_cols = {c: "sum" for c in ["Valor Total Item", "BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"] if c in df_itens.columns}
            res = df_itens.groupby(grp_cols).agg(agg_cols).reset_index()
            res = res.rename(columns={
                "Valor Total Item": "Valor Contábil",
                "BC ICMS Item": "BC ICMS",
                "Valor ICMS Item": "ICMS",
                "Valor IPI Item": "IPI",
            })
            abas["Resumo Itens por NCM-CFOP"] = res

        grp2_cols = [c for c in ["Tipo Nota", "Competência", "CNPJ", "Razão Social", "CFOP", "NCM Item", "CST ICMS"] if c in df_itens.columns]
        if grp2_cols:
            agg_cols = {c: "sum" for c in ["Valor Total Item", "BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"] if c in df_itens.columns}
            res2 = df_itens.groupby(grp2_cols).agg(agg_cols).reset_index()
            res2 = res2.rename(columns={
                "Valor Total Item": "Valor Contábil",
                "BC ICMS Item": "BC ICMS",
                "Valor ICMS Item": "ICMS",
                "Valor IPI Item": "IPI",
            })
            abas["Resumo CFOP-NCM-CST"] = res2

        grp_uf_cols = [c for c in ["Competência", "CNPJ", "Razão Social", "UF", "CFOP"] if c in df_itens.columns]
        if grp_uf_cols:
            agg_cols = {c: "sum" for c in ["Valor Total Item", "BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"] if c in df_itens.columns}
            res_uf = df_itens.groupby(grp_uf_cols).agg(agg_cols).reset_index()
            res_uf = res_uf.rename(columns={
                "Valor Total Item": "Valor Contábil",
                "BC ICMS Item": "BC ICMS",
                "Valor ICMS Item": "ICMS",
                "Valor IPI Item": "IPI",
            })
            abas["Resumo UF-CFOP"] = res_uf

        grp_ncm_cols = [c for c in ["Competência", "CNPJ", "Razão Social", "NCM Item"] if c in df_itens.columns]
        if grp_ncm_cols:
            agg_cols = {c: "sum" for c in ["Valor Total Item", "BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"] if c in df_itens.columns}
            res_ncm = df_itens.groupby(grp_ncm_cols).agg(agg_cols).reset_index()
            res_ncm = res_ncm.rename(columns={
                "Valor Total Item": "Valor Contábil",
                "BC ICMS Item": "BC ICMS",
                "Valor ICMS Item": "ICMS",
                "Valor IPI Item": "IPI",
            })
            abas["Resumo NCM"] = res_ncm

    # Entradas
    if not df_ent.empty:
        abas["Detalhes Entradas"] = df_ent
        for c in ["Valor Total Item", "BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"]:
            if c in df_ent.columns:
                df_ent[c] = pd.to_numeric(df_ent[c], errors="coerce").fillna(0.0)
        grp = [c for c in ["Competência", "CNPJ", "Razão Social", "CFOP"] if c in df_ent.columns]
        if grp:
            df_cfop = df_ent.groupby(grp).agg({
                "Valor Total Item": "sum",
                "BC ICMS Item": "sum",
                "Valor ICMS Item": "sum",
                "Valor IPI Item": "sum",
            }).reset_index()
        else:
            df_cfop = df_ent.groupby(["Competência", "CFOP"]).agg({
                "Valor Total Item": "sum",
                "BC ICMS Item": "sum",
                "Valor ICMS Item": "sum",
                "Valor IPI Item": "sum",
            }).reset_index()
        df_cfop = df_cfop.rename(columns={
            "Valor Total Item": "Valor Contábil",
            "BC ICMS Item": "BC ICMS",
            "Valor ICMS Item": "ICMS",
            "Valor IPI Item": "IPI",
        })
        abas["Resumo Entradas por CFOP"] = df_cfop

        if "NCM Item" in df_ent.columns:
            grp2 = [c for c in ["Competência", "CNPJ", "Razão Social", "NCM Item", "CFOP"] if c in df_ent.columns]
            if grp2:
                df_ncm = df_ent.groupby(grp2).agg({
                    "Valor Total Item": "sum",
                    "BC ICMS Item": "sum",
                    "Valor ICMS Item": "sum",
                    "Valor IPI Item": "sum",
                }).reset_index()
            else:
                df_ncm = df_ent.groupby(["Competência", "NCM Item", "CFOP"]).agg({
                    "Valor Total Item": "sum",
                    "BC ICMS Item": "sum",
                    "Valor ICMS Item": "sum",
                    "Valor IPI Item": "sum",
                }).reset_index()
            df_ncm = df_ncm.rename(columns={
                "Valor Total Item": "Valor Contábil",
                "BC ICMS Item": "BC ICMS",
                "Valor ICMS Item": "ICMS",
                "Valor IPI Item": "IPI",
            })
            abas["Resumo Entradas por NCM-CFOP"] = df_ncm

    # Saídas
    if not df_sai.empty:
        abas["Detalhes Saídas"] = df_sai
        for c in ["Valor Total Nota", "BC ICMS", "Valor ICMS", "Valor IPI Nota"]:
            if c in df_sai.columns:
                df_sai[c] = pd.to_numeric(df_sai[c], errors="coerce").fillna(0.0)
        grp_s = [c for c in ["Competência", "CNPJ", "Razão Social", "CFOP", "CST ICMS"] if c in df_sai.columns]
        if grp_s:
            df_s = df_sai.groupby(grp_s).agg({
                "Valor Total Nota": "sum",
                "BC ICMS": "sum",
                "Valor ICMS": "sum",
                "Valor IPI Nota": "sum",
            }).reset_index()
        else:
            df_s = df_sai.groupby(["Competência", "CFOP", "CST ICMS"]).agg({
                "Valor Total Nota": "sum",
                "BC ICMS": "sum",
                "Valor ICMS": "sum",
                "Valor IPI Nota": "sum",
            }).reset_index()
        df_s = df_s.rename(columns={
            "Valor Total Nota": "Valor Contábil",
            "BC ICMS": "BC ICMS",
            "Valor ICMS": "ICMS",
            "Valor IPI Nota": "IPI",
        })
        abas["Resumo Saídas por CFOP-CST"] = df_s

    # Imobilizado / Uso e Consumo
    if not df_imob.empty:
        abas["Entradas Imob_UsoConsumo"] = df_imob

    # CT-e
    if not df_cte.empty:
        abas["Detalhes CT-e"] = df_cte
        for c in ["Valor Operação CT-e", "BC ICMS CT-e (D190)", "Valor ICMS CT-e (D190)"]:
            if c in df_cte.columns:
                df_cte[c] = pd.to_numeric(df_cte[c], errors="coerce").fillna(0.0)
        grp_c = [c for c in ["Competência", "CNPJ", "Razão Social", "CFOP CT-e", "CST CT-e"] if c in df_cte.columns]
        if grp_c:
            df_cs = df_cte.groupby(grp_c).agg({
                "Valor Operação CT-e": "sum",
                "BC ICMS CT-e (D190)": "sum",
                "Valor ICMS CT-e (D190)": "sum",
            }).reset_index()
        else:
            df_cs = df_cte.groupby(["Competência", "CFOP CT-e", "CST CT-e"]).agg({
                "Valor Operação CT-e": "sum",
                "BC ICMS CT-e (D190)": "sum",
                "Valor ICMS CT-e (D190)": "sum",
            }).reset_index()
        df_cs = df_cs.rename(columns={
            "Valor Operação CT-e": "Valor Contábil",
            "BC ICMS CT-e (D190)": "BC ICMS",
            "Valor ICMS CT-e (D190)": "ICMS",
        })
        abas["Resumo CT-e por CFOP-CST"] = df_cs

    # Ajustes e blocos
    if not df_aj.empty:
        abas["Ajustes"] = df_aj
    if not df_st.empty:
        abas["Resumo E200_ICMS_ST"] = df_st
    if not df_difal.empty:
        abas["Resumo E300_DIFAL"] = df_difal
    if not df_ipi.empty:
        abas["Resumo E500_IPI"] = df_ipi
    if not df_sem_c190.empty:
        abas["Notas Saída sem C190"] = df_sem_c190
    if not df_mestres.empty:
        abas["Dados Mestres"] = df_mestres
    if not df_flags.empty:
        abas["Presença Blocos"] = df_flags

    # DRE Fiscal (Receita, Outras Saídas, Custos, Despesas)
    def _limpa_cfop(c: str) -> str:
        return str(c or "").replace(".", "")

    dre_list: List[pd.DataFrame] = []

    cfop_receita = {"5101", "5102", "5403", "5405", "6101", "6102", "6403"}
    cfop_outros_exp = {"5949", "6949", "6910", "5910"}
    pref_outros = ("59", "69")
    cfop_custos = {"2102", "2101", "2403", "2405", "1102", "1101", "1403", "1405"}
    cfop_despesas = {"2551", "1551", "1933"}

    def _cat(df: pd.DataFrame, nome: str, teste_cfop) -> Optional[pd.DataFrame]:
        if df.empty:
            return None
        if "CFOP" not in df.columns:
            return None
        sub = df[df["CFOP"].apply(lambda x: teste_cfop(_limpa_cfop(x)))].copy()
        if sub.empty:
            return None
        for c in ["Valor Contábil", "ICMS", "IPI"]:
            if c in sub.columns:
                sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0.0)
        grp = [c for c in ["Competência", "CNPJ", "Razão Social"] if c in sub.columns]
        if not grp:
            grp = ["Competência"]
        g = sub.groupby(grp).agg({
            "Valor Contábil": "sum",
            "ICMS": "sum",
            "IPI": "sum",
        }).reset_index()
        g["Categoria"] = nome
        g["Total Impostos"] = g["ICMS"] + g["IPI"]
        return g

    if not df_sai.empty:
        df_out = df_sai.copy()
        if "Valor Contábil" not in df_out.columns:
            if "Valor Total Nota" in df_out.columns:
                df_out["Valor Contábil"] = pd.to_numeric(df_out["Valor Total Nota"], errors="coerce").fillna(0.0)
            elif "Valor Operação" in df_out.columns:
                df_out["Valor Contábil"] = pd.to_numeric(df_out["Valor Operação"], errors="coerce").fillna(0.0)
            else:
                df_out["Valor Contábil"] = 0.0
        if "ICMS" not in df_out.columns:
            if "Valor ICMS" in df_out.columns:
                df_out["ICMS"] = pd.to_numeric(df_out["Valor ICMS"], errors="coerce").fillna(0.0)
            elif "Valor ICMS Nota" in df_out.columns:
                df_out["ICMS"] = pd.to_numeric(df_out["Valor ICMS Nota"], errors="coerce").fillna(0.0)
            else:
                df_out["ICMS"] = 0.0
        if "IPI" not in df_out.columns:
            if "Valor IPI Nota" in df_out.columns:
                df_out["IPI"] = pd.to_numeric(df_out["Valor IPI Nota"], errors="coerce").fillna(0.0)
            elif "Valor IPI (C190)" in df_out.columns:
                df_out["IPI"] = pd.to_numeric(df_out["Valor IPI (C190)"], errors="coerce").fillna(0.0)
            else:
                df_out["IPI"] = 0.0

        def teste_receita(cf: str) -> bool:
            return cf in cfop_receita
        g_rec = _cat(df_out, "Receita", teste_receita)
        if g_rec is not None:
            dre_list.append(g_rec)

        def teste_outros(cf: str) -> bool:
            return (cf in cfop_outros_exp) or cf.startswith(pref_outros)
        g_out = _cat(df_out, "Outras Saídas", teste_outros)
        if g_out is not None:
            dre_list.append(g_out)

    if not df_ent.empty:
        df_in = df_ent.copy()
        if "Valor Contábil" not in df_in.columns:
            if "Valor Total Item" in df_in.columns:
                df_in["Valor Contábil"] = pd.to_numeric(df_in["Valor Total Item"], errors="coerce").fillna(0.0)
            else:
                df_in["Valor Contábil"] = 0.0
        if "ICMS" not in df_in.columns:
            if "Valor ICMS Item" in df_in.columns:
                df_in["ICMS"] = pd.to_numeric(df_in["Valor ICMS Item"], errors="coerce").fillna(0.0)
            elif "Valor ICMS Nota" in df_in.columns:
                df_in["ICMS"] = pd.to_numeric(df_in["Valor ICMS Nota"], errors="coerce").fillna(0.0)
            else:
                df_in["ICMS"] = 0.0
        if "IPI" not in df_in.columns:
            if "Valor IPI Item" in df_in.columns:
                df_in["IPI"] = pd.to_numeric(df_in["Valor IPI Item"], errors="coerce").fillna(0.0)
            elif "Valor IPI Nota" in df_in.columns:
                df_in["IPI"] = pd.to_numeric(df_in["Valor IPI Nota"], errors="coerce").fillna(0.0)
            else:
                df_in["IPI"] = 0.0

        def teste_custos(cf: str) -> bool:
            return cf in cfop_custos
        g_cus = _cat(df_in, "Custos", teste_custos)
        if g_cus is not None:
            dre_list.append(g_cus)

        def teste_desp(cf: str) -> bool:
            return cf in cfop_despesas
        g_des = _cat(df_in, "Despesas", teste_desp)
        if g_des is not None:
            dre_list.append(g_des)

    if dre_list:
        df_dre = pd.concat(dre_list, ignore_index=True)
        ordem = [c for c in ["Competência", "CNPJ", "Razão Social", "Categoria", "Valor Contábil", "ICMS", "IPI", "Total Impostos"] if c in df_dre.columns]
        df_dre = df_dre[ordem]
        df_dre["Categoria"] = pd.Categorical(df_dre["Categoria"], categories=["Receita", "Outras Saídas", "Custos", "Despesas"], ordered=True)
        sort_cols = [c for c in ["Competência", "CNPJ", "Razão Social", "Categoria"] if c in df_dre.columns]
        df_dre = df_dre.sort_values(by=sort_cols).reset_index(drop=True)
        abas["DRE Fiscal"] = df_dre

        # Indicadores (KPI) e Carga Efetiva
        kpi = df_dre.copy()
        kpi["Competência"] = kpi["Competência"].fillna("Sem competência")
        for c in ["CNPJ", "Razão Social"]:
            if c in kpi.columns:
                kpi[c] = kpi[c].fillna("").astype(str)

        linhas = []
        grp_fields = [c for c in ["Competência", "CNPJ", "Razão Social"] if c in kpi.columns] or ["Competência"]
        for keys, comp in kpi.groupby(grp_fields):
            if isinstance(keys, tuple):
                chave = {grp_fields[i]: keys[i] for i in range(len(grp_fields))}
            else:
                chave = {grp_fields[0]: keys}
            receita = comp.loc[comp["Categoria"] == "Receita", "Valor Contábil"].sum()
            custos = comp.loc[comp["Categoria"] == "Custos", "Valor Contábil"].sum()
            impostos = comp.loc[comp["Categoria"] == "Receita", "Total Impostos"].sum()
            margem = receita - custos
            carga = (impostos / receita * 100.0) if receita > 0 else 0.0
            linha = {"Receita": receita, "Custos": custos, "Margem Bruta": margem, "Total Impostos": impostos, "Carga Tributária Efetiva (%)": carga}
            for k, v in chave.items():
                linha[k] = v
            linhas.append(linha)
        df_kpi = pd.DataFrame(linhas)
        ordem = [c for c in ["Competência", "CNPJ", "Razão Social"] if c in df_kpi.columns] + ["Receita", "Custos", "Margem Bruta", "Total Impostos", "Carga Tributária Efetiva (%)"]
        df_kpi = df_kpi[ordem]
        df_kpi["Competência"] = df_kpi["Competência"].astype(str)
        abas["Indicadores Fiscais"] = df_kpi

        # Integração do saldo de impostos para métrica Imposto/Faturamento (%)
        # (Mantido simples: usa apenas ICMS/IPI de entradas/saídas já agregados acima)
        # Se necessário, esta parte pode ser expandida para cruzar com "Saldo Impostos".

    # Saldos impostos (simplificado)
    linhas_bal = []
    if not df_ent.empty or not df_sai.empty:
        if not df_ent.empty:
            dfe = df_ent.copy()
            for c in ["BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"]:
                if c in dfe.columns:
                    dfe[c] = pd.to_numeric(dfe[c], errors="coerce").fillna(0.0)
            creditos = dfe.groupby(["CNPJ", "Competência"]).agg({
                "BC ICMS Item": "sum",
                "Valor ICMS Item": "sum",
                "Valor IPI Item": "sum",
            }).reset_index()
        else:
            creditos = pd.DataFrame(columns=["CNPJ", "Competência", "BC ICMS Item", "Valor ICMS Item", "Valor IPI Item"])

        if not df_sai.empty:
            dfs = df_sai.copy()
            for c in ["BC ICMS", "Valor ICMS", "Valor IPI Nota"]:
                if c in dfs.columns:
                    dfs[c] = pd.to_numeric(dfs[c], errors="coerce").fillna(0.0)
            debitos = dfs.groupby(["CNPJ", "Competência"]).agg({
                "BC ICMS": "sum",
                "Valor ICMS": "sum",
                "Valor IPI Nota": "sum",
            }).reset_index()
        else:
            debitos = pd.DataFrame(columns=["CNPJ", "Competência", "BC ICMS", "Valor ICMS", "Valor IPI Nota"])

        bal = pd.merge(creditos, debitos, on=["CNPJ", "Competência"], how="outer", suffixes=("_Cred", "_Deb")).fillna(0.0)
        for _, r in bal.iterrows():
            icms_cred = r.get("Valor ICMS Item", 0.0)
            icms_deb = r.get("Valor ICMS", 0.0)
            ipi_cred = r.get("Valor IPI Item", 0.0)
            ipi_deb = r.get("Valor IPI Nota", 0.0)
            linhas_bal.append({
                "CNPJ": r["CNPJ"],
                "Competência": r["Competência"],
                "ICMS a Recuperar (Entradas)": icms_cred,
                "ICMS a Pagar (Saídas)": icms_deb,
                "Saldo ICMS (Cred - Deb)": icms_cred - icms_deb,
                "IPI a Recuperar (Entradas)": ipi_cred,
                "IPI a Pagar (Saídas)": ipi_deb,
                "Saldo IPI (Cred - Deb)": ipi_cred - ipi_deb,
            })
        if linhas_bal:
            abas["Saldo Impostos"] = pd.DataFrame(linhas_bal)

    return abas


# ---------------------------------------------------------------------------
# Escrita Excel
# ---------------------------------------------------------------------------

def escrever_excel(abas: Dict[str, pd.DataFrame], destino: str) -> None:
    """Gera Excel com abas e formatação básica."""
    with pd.ExcelWriter(destino, engine="xlsxwriter") as writer:
        for nome, df in abas.items():
            nome_31 = nome[:31]
            df.to_excel(writer, sheet_name=nome_31, index=False)
            ws = writer.sheets[nome_31]
            ws.freeze_panes(1, 0)
            ws.set_zoom(90)
            for i, col in enumerate(df.columns):
                max_len = max((len(str(x)) for x in [col] + df[col].astype(str).tolist()), default=0)
                largura = min(max(max_len + 2, 12), 60)
                ws.set_column(i, i, largura)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def cli(argv: Optional[Iterable[str]] = None) -> int:
    """Interface de linha de comando."""
    p = argparse.ArgumentParser(description="SPED Analyzer ICMS e IPI (CLI)")
    p.add_argument("--sped", nargs="+", required=True, help="Um ou mais arquivos SPED (.txt)")
    p.add_argument("--xml_dir", default="", help="Pasta com XMLs (NF-e/CT-e) para cruzamento (opcional)")
    p.add_argument("--tipi", default="", help="Arquivo TIPI (.csv/.xlsx) para conferência de IPI (opcional)")
    p.add_argument("--output", required=True, help="Caminho do Excel de saída (.xlsx)")
    args = p.parse_args(argv)

    mapa_tipi = {}
    if args.tipi:
        try:
            mapa_tipi = carregar_tipi(args.tipi)
        except Exception as exc:
            print(f"Aviso: falha ao carregar TIPI ({exc}).")

    mapa_xml = {}
    if args.xml_dir:
        try:
            mapa_xml = parse_xml_diretorio(args.xml_dir)
        except Exception as exc:
            print(f"Aviso: falha ao ler XMLs ({exc}).")

    registros: List[SpedRecord] = []
    for caminho in args.sped:
        if not os.path.isfile(caminho):
            print(f"Arquivo SPED não encontrado: {caminho}")
            continue
        registros.append(parse_sped(caminho, mapa_xml, mapa_tipi))

    if not registros:
        print("Nenhum arquivo SPED processado.")
        return 1

    abas = agregar_registros(registros)
    escrever_excel(abas, args.output)
    print(f"Relatório salvo em: {args.output}")
    return 0


# ---------------------------------------------------------------------------
# GUI (Tkinter)
# ---------------------------------------------------------------------------

class AppGUI(_tk.Tk if _tk else object):
    """Interface simples (Tkinter)."""
    def __init__(self) -> None:
        if not _tk:
            raise RuntimeError("Tkinter indisponível.")
        super().__init__()
        self.title("SPED Analyzer ICMS e IPI")
        self.geometry("640x360")

        self.speds: List[str] = []
        self.tipi: str = ""
        self.xml_dir: str = ""
        self.saida: str = ""

        self._montar_ui()

    def _montar_ui(self) -> None:
        frm_sped = _tk.LabelFrame(self, text="SPED (.txt)")
        frm_sped.pack(fill="x", padx=10, pady=5)
        _tk.Button(frm_sped, text="Selecionar", command=self._sel_sped).pack(side="left", padx=5, pady=5)
        self.lbl_sped = _tk.Label(frm_sped, text="Nenhum arquivo selecionado")
        self.lbl_sped.pack(side="left", padx=5)

        frm_tipi = _tk.LabelFrame(self, text="TIPI (opcional)")
        frm_tipi.pack(fill="x", padx=10, pady=5)
        _tk.Button(frm_tipi, text="Selecionar", command=self._sel_tipi).pack(side="left", padx=5, pady=5)
        self.lbl_tipi = _tk.Label(frm_tipi, text="Nenhum arquivo TIPI selecionado")
        self.lbl_tipi.pack(side="left", padx=5)

        frm_xml = _tk.LabelFrame(self, text="Pasta de XMLs (opcional)")
        frm_xml.pack(fill="x", padx=10, pady=5)
        _tk.Button(frm_xml, text="Selecionar", command=self._sel_xml).pack(side="left", padx=5, pady=5)
        self.lbl_xml = _tk.Label(frm_xml, text="Nenhuma pasta selecionada")
        self.lbl_xml.pack(side="left", padx=5)

        frm_out = _tk.LabelFrame(self, text="Salvar Excel")
        frm_out.pack(fill="x", padx=10, pady=5)
        _tk.Button(frm_out, text="Escolher", command=self._sel_saida).pack(side="left", padx=5, pady=5)
        self.lbl_out = _tk.Label(frm_out, text="Nenhum arquivo de saída")
        self.lbl_out.pack(side="left", padx=5)

        _tk.Button(self, text="Executar Auditoria", command=self._executar).pack(pady=10)
        self.lbl_status = _tk.Label(self, text="Pronto", anchor="w")
        self.lbl_status.pack(fill="x", padx=10, pady=5)

    def _sel_sped(self):
        if not _filedialog:
            return
        files = _filedialog.askopenfilenames(title="Selecione .txt do SPED", filetypes=[("Text Files", "*.txt")])
        if files:
            self.speds = list(files)
            self.lbl_sped.config(text=f"{len(self.speds)} arquivo(s) selecionado(s)" if len(self.speds) > 1 else os.path.basename(self.speds[0]))

    def _sel_tipi(self):
        if not _filedialog:
            return
        f = _filedialog.askopenfilename(title="Selecione TIPI", filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")])
        if f:
            self.tipi = f
            self.lbl_tipi.config(text=os.path.basename(f))

    def _sel_xml(self):
        if not _filedialog:
            return
        d = _filedialog.askdirectory(title="Selecione pasta de XMLs")
        if d:
            self.xml_dir = d
            self.lbl_xml.config(text=os.path.basename(d))

    def _sel_saida(self):
        if not _filedialog:
            return
        f = _filedialog.asksaveasfilename(title="Salvar Excel", defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx")])
        if f:
            if not f.lower().endswith(".xlsx"):
                f += ".xlsx"
            self.saida = f
            self.lbl_out.config(text=os.path.basename(f))

    def _executar(self):
        if not self.speds:
            if _messagebox:
                _messagebox.showwarning("SPED", "Selecione pelo menos um arquivo .txt do SPED.")
            return
        if not self.saida:
            if _messagebox:
                _messagebox.showwarning("Saída", "Defina o caminho do Excel de saída.")
            return
        self._set_status("Processando...")
        if _threading:
            _threading.Thread(target=self._worker, daemon=True).start()
        else:
            self._worker()

    def _worker(self):
        try:
            mapa_tipi: Dict[str, float] = {}
            if self.tipi:
                self._set_status("Carregando TIPI...")
                try:
                    mapa_tipi = carregar_tipi(self.tipi)
                except Exception as exc:
                    if _messagebox:
                        _messagebox.showwarning("TIPI", f"Falha ao carregar TIPI ({exc}).")
                    mapa_tipi = {}

            mapa_xml: Dict[str, Dict[str, object]] = {}
            if self.xml_dir:
                self._set_status("Lendo XMLs...")
                try:
                    mapa_xml = parse_xml_diretorio(self.xml_dir)
                except Exception as exc:
                    if _messagebox:
                        _messagebox.showwarning("XML", f"Falha ao ler XMLs ({exc}).")
                    mapa_xml = {}

            registros: List[SpedRecord] = []
            for p in self.speds:
                self._set_status(f"Processando {os.path.basename(p)}...")
                try:
                    registros.append(parse_sped(p, mapa_xml, mapa_tipi))
                except Exception as exc:
                    if _messagebox:
                        _messagebox.showerror("Erro", f"Erro ao processar {os.path.basename(p)}: {exc}")
                    return

            self._set_status("Agregando...")
            abas = agregar_registros(registros)
            self._set_status("Salvando Excel...")
            escrever_excel(abas, self.saida)
            self._set_status("Concluído! Abrindo arquivo...")
            self._abrir(self.saida)
            self._set_status("Relatório gerado com sucesso.")
        except Exception as exc:
            if _messagebox:
                _messagebox.showerror("Erro", str(exc))
            self._set_status("Erro durante a execução.")

    def _set_status(self, msg: str):
        def _u():
            self.lbl_status.config(text=msg)
        self.lbl_status.after(0, _u)

    def _abrir(self, caminho: str):
        try:
            if sys.platform.startswith("win"):
                os.startfile(caminho)  # type: ignore
            elif sys.platform == "darwin":
                import subprocess
                subprocess.run(["open", caminho], check=False)
            else:
                import subprocess
                subprocess.run(["xdg-open", caminho], check=False)
        except Exception as exc:
            if _messagebox:
                _messagebox.showwarning("Abrir", f"Arquivo salvo em:\n{caminho}\nNão foi possível abrir automaticamente: {exc}")


# ---------------------------------------------------------------------------
# Streamlit (Web)
# ---------------------------------------------------------------------------

def executar_streamlit():
    """
    Interface Web (Streamlit) para subir arquivos e gerar o Excel.
    """
    import streamlit as st
    import tempfile
    import zipfile
    import io

    st.set_page_config(page_title="SPED Analyzer ICMS e IPI", layout="centered")
    st.title("SPED Analyzer ICMS e IPI")
    st.write("Auditoria de arquivos SPED ICMS/IPI")

    sped_files = st.file_uploader("Selecione arquivos SPED (.txt)", type=["txt"], accept_multiple_files=True)
    tipi_file = st.file_uploader("Selecione TIPI (CSV/XLSX) (opcional)", type=["csv", "xlsx"])
    xml_files = st.file_uploader("Selecione XMLs ou ZIP com XMLs (opcional)", type=["xml", "zip"], accept_multiple_files=True)

    if st.button("Executar Auditoria"):
        if not sped_files:
            st.error("Selecione pelo menos um arquivo SPED.")
            return
        with st.spinner("Processando arquivos..."):
            with tempfile.TemporaryDirectory() as tmp:
                caminhos_sped: List[str] = []
                for up in sped_files:
                    pth = os.path.join(tmp, up.name)
                    with open(pth, "wb") as f:
                        f.write(up.getbuffer())
                    caminhos_sped.append(pth)

                mapa_tipi: Dict[str, float] = {}
                if tipi_file:
                    p_tipi = os.path.join(tmp, tipi_file.name)
                    with open(p_tipi, "wb") as f:
                        f.write(tipi_file.getbuffer())
                    try:
                        mapa_tipi = carregar_tipi(p_tipi)
                    except Exception as exc:
                        st.warning(f"Falha ao carregar TIPI ({exc}).")
                        mapa_tipi = {}

                mapa_xml: Dict[str, Dict[str, object]] = {}
                if xml_files:
                    for up in xml_files:
                        nome = up.name.lower()
                        if nome.endswith(".zip"):
                            try:
                                with zipfile.ZipFile(io.BytesIO(up.getbuffer())) as zf:
                                    for nm in zf.namelist():
                                        if not nm.lower().endswith(".xml"):
                                            continue
                                        try:
                                            dados = zf.read(nm)
                                            with tempfile.NamedTemporaryFile(dir=tmp, delete=False, suffix=".xml") as tfile:
                                                tfile.write(dados)
                                                tfile.flush()
                                                d = parse_xml_nfe(tfile.name) or parse_xml_cte(tfile.name)
                                                if d and "Chave" in d:
                                                    mapa_xml[d["Chave"]] = d
                                        except Exception:
                                            pass
                            except Exception:
                                pass
                        elif nome.endswith(".xml"):
                            try:
                                with tempfile.NamedTemporaryFile(dir=tmp, delete=False, suffix=".xml") as tfile:
                                    tfile.write(up.getbuffer())
                                    tfile.flush()
                                    d = parse_xml_nfe(tfile.name) or parse_xml_cte(tfile.name)
                                    if d and "Chave" in d:
                                        mapa_xml[d["Chave"]] = d
                            except Exception:
                                pass

                registros: List[SpedRecord] = []
                for pth in caminhos_sped:
                    try:
                        registros.append(parse_sped(pth, mapa_xml, mapa_tipi))
                    except Exception as exc:
                        st.error(f"Erro ao processar {os.path.basename(pth)}: {exc}")

                if not registros:
                    st.error("Nenhum arquivo SPED processado.")
                    return

                abas = agregar_registros(registros)
                with tempfile.NamedTemporaryFile(suffix=".xlsx") as tfile:
                    escrever_excel(abas, tfile.name)
                    tfile.seek(0)
                    binario = tfile.read()

                st.success("Relatório gerado com sucesso!")
                st.download_button(
                    label="Download do Excel",
                    data=binario,
                    file_name="auditoria_sped.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                if "Indicadores Fiscais" in abas:
                    if st.checkbox("Mostrar Indicadores Fiscais"):
                        st.dataframe(abas["Indicadores Fiscais"])


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Se recebeu argumentos --sped, executa CLI.
    if any(arg.startswith("--sped") for arg in sys.argv[1:]):
        sys.exit(cli())

    # Caso contrário, tenta Streamlit; se falhar, abre GUI; por fim, CLI simples.
    try:
        import streamlit  # type: ignore
        executar_streamlit()
    except Exception:
        if _tk:
            app = AppGUI()
            app.mainloop()
        else:
            print("Executando em modo CLI simples.")
            print("Exemplo: python sped_analyzer.py --sped arquivo.txt --output relatorio.xlsx")
''')

# Salva o arquivo
out_path = "/mnt/data/sped_analyzer.py"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(code)

out_path
