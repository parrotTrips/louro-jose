import os
import json

SYSTEM_INSTRUCTIONS = (
    "Você é um extrator de dados de cotações hoteleiras. "
    "Receberá metadados (timestamp, assunto, remetente e destinatário) e o corpo do e-mail limpo. "
    "Retorne ESTRITAMENTE um JSON contendo:\n"
    "- Um OBJETO com as chaves exatas, quando houver apenas um tipo de quarto;\n"
    "- Ou um ARRAY de OBJETOS (um por tipo de quarto) quando houver múltiplos tipos.\n"
    "NUNCA inclua nada fora do JSON/ARRAY JSON. NUNCA inclua chaves extras.\n"
    "Se um campo não existir, use a string vazia \"\".\n"
    "O campo 'Fornecedor' deve ser preenchido com o REMETENTE (nome e/ou e-mail de quem enviou)."
)

EXAMPLE = [
    {
        "Timestamp": "2025-11-08 09:30",
        "Fornecedor": "Hotel Aconchego <reservas@exemplo.com>",
        "Assunto": "Paraty | Hotel Aconchego | Novembro",
        "Nome do hotel": "Hotel Aconchego",
        "Cidade": "Paraty",
        "Check-in": "2025-11-24",
        "Check-out": "2025-11-26",
        "Número de quartos": "7",  # apenas números
        "Tipo de quarto": "Duplo luxo",
        "Tipo de quarto (normalizado)": "duplo luxo",
        "Preço (num)": "508.20",  # sem R$, ponto decimal, 2 casas
        "Qual configuração do quarto (twin, double)": "",
        "Tarifa NET ou comissionada?": "NET",
        "Taxa? Ex.: 5% de ISS": "5% ISS",
        "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.": "Café da manhã (07:00–10:00)",
        "Política de pagamento": "25% até 30 dias após bloqueio; saldo em 3 transferências; quitar até o check-in",
        "Política de cancelamento": ""
    },
    {
        "Timestamp": "2025-11-08 09:30",
        "Fornecedor": "Hotel Aconchego <reservas@exemplo.com>",
        "Assunto": "Paraty | Hotel Aconchego | Novembro",
        "Nome do hotel": "Hotel Aconchego",
        "Cidade": "Paraty",
        "Check-in": "2025-11-24",
        "Check-out": "2025-11-26",
        "Número de quartos": "6",
        "Tipo de quarto": "Duplo standard",
        "Tipo de quarto (normalizado)": "duplo standard",
        "Preço (num)": "379.80",
        "Qual configuração do quarto (twin, double)": "",
        "Tarifa NET ou comissionada?": "NET",
        "Taxa? Ex.: 5% de ISS": "5% ISS",
        "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.": "Café da manhã (07:00–10:00)",
        "Política de pagamento": "25% até 30 dias após bloqueio; saldo em 3 transferências; quitar até o check-in",
        "Política de cancelamento": ""
    }
]

def build_user_prompt(fields: list[str], meta: dict, body_clean: str) -> str:
    """
    Monta o prompt para a LLM com:
      - lista de campos do cabeçalho (fields)
      - metadados do e-mail (meta)
      - corpo já limpo (body_clean)
    Retorna uma string pronta para 'generate_content'.
    """
    fields_lines = os.linesep.join(f"- {k}" for k in fields)

    return (
        "Contexto:\n"
        "O texto abaixo é um e-mail (em português) possivelmente com histórico. "
        "O conteúdo já foi minimamente limpo. "
        "Você deve focar nas partes onde o hotel/pousada informa disponibilidade, preços e políticas.\n\n"
        "Esquema de saída (CHAVES EXATAS, nesta ordem lógica):\n"
        f"{fields_lines}\n\n"
        "FORMATO DE RESPOSTA:\n"
        "- Se houver APENAS UM tipo de quarto, retorne APENAS UM OBJETO JSON com essas chaves.\n"
        "- Se houver MÚLTIPLOS tipos de quarto, retorne UM ARRAY JSON; cada item do array deve ser um OBJETO com as MESMAS chaves acima, "
        "representando UMA linha/uma cotação para aquele tipo de quarto.\n"
        "NÃO inclua nenhum texto fora do JSON/ARRAY JSON.\n\n"
        "Exemplo (didático; valores meramente ilustrativos):\n"
        f"{json.dumps(EXAMPLE, ensure_ascii=False)}\n\n"
        "Metadados:\n"
        f"- Timestamp: {meta.get('timestamp','')}\n"
        f"- Assunto: {meta.get('subject','')}\n"
        f"- Remetente (quem enviou): {meta.get('from','')}\n"
        f"- Destinatário (quem recebeu): {meta.get('to','')}\n\n"
        "Corpo:\n"
        "---\n"
        f"{body_clean}\n"
        "---\n"
        "Regras de preenchimento (IMPORTANTES):\n"
        "- \"Fornecedor\": SEMPRE preencher com o REMETENTE (nome e/ou e-mail de quem enviou), não use o destinatário.\n"
        "- \"Check-in\" e \"Check-out\": quando houver período (ex.: '24/11/2025 a 26/11/2025'), converter cada data para ISO 'AAAA-MM-DD'. "
        "Se houver apenas uma data, usar em 'Check-in' e deixar 'Check-out' como \"\".\n"
        "- \"Número de quartos\": usar apenas dígitos (ex.: '7'). Se o e-mail listar quantidade por categoria, use a quantidade correspondente ao "
        "objeto/\"Tipo de quarto\" daquele item. Se não for claro, retornar \"\".\n"
        "- \"Tipo de quarto\": copiar a denominação comercial como aparece (ex.: 'Duplo luxo').\n"
        "- \"Tipo de quarto (normalizado)\": versão normalizada do tipo de quarto em minúsculas, removendo termos como 'apto', 'apartamento', 'ap.', "
        "'quarto', 'quartos', pontuação redundante e espaçamentos extras; ex.: 'duplo luxo'.\n"
        "- \"Preço (num)\": retornar SOMENTE o número, em notação decimal com ponto e duas casas (ex.: '508.20'); sem 'R$', sem milhares, sem texto.\n"
        "- Demais campos: preencher a partir do e-mail; se não existir, use \"\".\n"
        "- NÃO invente valores; se tiver dúvida real, use \"\".\n"
        "- NÃO inclua campos além dos listados.\n"
    )
