import re
from datetime import datetime

def _to_float_brl(s: str) -> float:
    s = s.replace('.', '').replace(',', '.')
    return float(re.search(r'\d+(?:[\.,]\d+)?', s).group())

def parse_date_range_pt(body: str):
    # Ex.: "Confirmamos disponibilidade para (01 a 04/01/2026)"
    m = re.search(r'\((\d{1,2})\s*a\s*(\d{1,2})/(\d{1,2})/(\d{4})\)', body)
    if not m: 
        return None, None
    d1, d2, mm, yyyy = map(int, m.groups())
    checkin  = datetime(yyyy, mm, d1).strftime('%Y-%m-%d')
    checkout = datetime(yyyy, mm, d2).strftime('%Y-%m-%d')
    return checkin, checkout

def extract_tabular_quotes(body: str):
    """
    Procura blocos tipo:
      Execut... (Cama twin ou casal)
      SGL/DBL R$ 975,00 + 5% ISS
    retorna lista de dicts com Tipo de quarto e Preço (num)
    """
    lines = [re.sub(r'\s+', ' ', ln.strip('*•· \t')) for ln in body.splitlines() if ln.strip()]
    rows, last_room = [], None

    # taxa ISS (se existir)
    iss = None
    for ln in lines:
        m_iss = re.search(r'(\d{1,2}(?:[\.,]\d{1,2})?)\s*%.*ISS', ln, re.I)
        if m_iss:
            iss = m_iss.group(1) + '% ISS'; break

    for ln in lines:
        # captura nome de categoria/quarto
        if re.search(r'(executivo|luxo|standard|superior|frente mar|vista lateral|twin|duplo|triplo|sgl/dbl)', ln, re.I):
            last_room = ln
        # captura preço linha seguinte
        m_price = re.search(r'SGL/DBL\s*R\$\s*([\d\.\,]+)', ln, re.I)
        if m_price and last_room:
            price_num = _to_float_brl(m_price.group(1))
            rows.append({
                "Tipo de quarto": last_room,
                "Preço (num)": f"{price_num:.2f}",
                "Taxa? Ex.: 5% de ISS": iss or ""
            })
            last_room = None

    return rows
