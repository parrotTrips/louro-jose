import os
import re
import json

def force_json_object(text: str) -> dict:
    """Aceita resposta com ```json ... ``` ou texto solto; devolve o 1º objeto JSON válido."""
    text = (text or "").strip()
    m = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    if m:
        text = m.group(1).strip()
    b0, b1 = text.find("{"), text.rfind("}")
    if b0 != -1 and b1 != -1 and b1 > b0:
        text = text[b0:b1+1]
    return json.loads(text)

def blank_row(fields: list[str]) -> dict:
    return {k: "" for k in fields}

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
