import re

def strip_forwarding_noise(s: str) -> str:
    """Limpa ruídos comuns de encaminhamento sem remover o miolo (preços/políticas)."""
    if not isinstance(s, str):
        return ""
    t = s.replace("\r\n", "\n").replace("\r", "\n")
    # blocos de encaminhamento
    t = re.sub(r"(?m)^[- ]{5,}\s*Forwarded message\s*[- ]{5,}\n.*?(?=\n\n|\Z)", "", t, flags=re.I|re.S)
    # cabeçalhos repetidos
    t = re.sub(r"(?m)^(From|De|To|Para|Subject|Assunto|Date|Data):.*$", "", t)
    # links/assinaturas
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"(?mi)^--\s*$.*?(?=\n\S|\Z)", "", t, flags=re.S)
    # normalização
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()
