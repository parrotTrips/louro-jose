import os
import json

# Instruções de sistema (passadas em system_instruction do modelo)
SYSTEM_INSTRUCTIONS = (
    "Você é um extrator de dados de cotações hoteleiras. "
    "Receberá metadados (timestamp, destinatário, assunto, remetente) e o corpo do e-mail limpo. "
    "Retorne ESTRITAMENTE um objeto JSON com as chaves exatas fornecidas. "
    "Se um campo não existir, use a string vazia \"\". "
    "Não inclua nada fora do JSON."
)

# Exemplo curto para ancorar o formato (valores meramente ilustrativos)
EXAMPLE = {
    "Timestamp": "2025-11-08 09:30",
    "Destinatário": "cotacoes@exemplo.com",
    "Assunto": "Paraty | Hotel Aconchego | Novembro",
    "Nome do hotel": "Hotel Aconchego",
    "Cidade": "Paraty",
    "Número de quartos disponível": "07 duplo luxo; 06 duplo standard; 04 suíte luxo; 01 triplo luxo; 02 triplo standard; 11 quádruplos; 01 quíntuplo",
    "Qual configuração do quarto (twin, double)": "",
    "Qual tipo de quarto (standard, luxo, superior…)": "",
    "Preço por tipo de quarto": "Duplo luxo: R$ 508,20; Duplo standard: R$ 379,80; Suíte luxo: R$ 654,00; Triplo luxo: R$ 636,00; Triplo standard: R$ 489,60; Quádruplo: R$ 602,40; Quíntuplo: R$ 712,80",
    "Tarifa NET ou comissionada?": "NET",
    "Taxa? Ex.: 5% de ISS": "5% ISS",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.": "Café da manhã (07:00–10:00)",
    "Data da hospedagem": "2025-11-24 a 2025-11-26",
    "Política de pagamento": "25% até 30 dias após bloqueio; saldo em 3 transferências; quitar até o check-in",
    "Política de cancelamento": ""
}

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
        "Retorne APENAS um JSON com as chaves EXATAS a seguir (sem texto extra):\n"
        f"{fields_lines}\n\n"
        "Exemplo de formato (apenas ilustrativo; os valores devem vir do e-mail real):\n"
        f"{json.dumps(EXAMPLE, ensure_ascii=False)}\n\n"
        "Metadados:\n"
        f"- Timestamp: {meta.get('timestamp','')}\n"
        f"- Destinatário: {meta.get('to','')}\n"
        f"- Assunto: {meta.get('subject','')}\n"
        f"- Remetente: {meta.get('from','')}\n\n"
        "Corpo:\n"
        "---\n"
        f"{body_clean}\n"
        "---\n"
        "Regras:\n"
        "- Se tiver múltiplos tipos de quarto/preços, concatenar numa única string separados por '; '.\n"
        "- Se não souber, use \"\".\n"
        "- Campos devem estar em português e legíveis (ex.: \"Duplo luxo: R$ 508,20; Suíte: R$ 654,00\")."
    )
