# app/core/llm_client.py

import json
import requests
from app.core.config import settings


class LLMClient:
    def __init__(self):
        self.api_key = settings.OPENROUTER_API_KEY
        self.base_url = settings.OPENROUTER_BASE_URL
        self.model = settings.OPENROUTER_MODEL

    # ------------------------------------------------------------------
    # 1) Classificador simples: VIAGEM / NAO-VIAGEM  (já existia)
    # ------------------------------------------------------------------
    def classify_email(self, text: str) -> str:
        """
        Envia o texto do email para o LLM e retorna 'VIAGEM' ou 'NAO-VIAGEM'.
        """
        prompt = f"""
Você é um classificador de emails.

Responda APENAS com uma das opções:

- VIAGEM
- NAO-VIAGEM

Essa classificação deve identificar emails que são:
- cotações de hotel
- mensagens de fornecedores de hospedagem
- respostas sobre disponibilidade, tarifas, política de cancelamento
- temas relacionados a viagens

Email:
{text}
"""

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }

        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers=headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        response_json = response.json()
        raw = response_json["choices"][0]["message"]["content"].strip().upper()

        # Sanitizar resposta
        if "VIAGEM" in raw:
            return "VIAGEM"
        return "NAO-VIAGEM"

    # ------------------------------------------------------------------
    # 2) Novo: extrair cotações em formato JSON (lista de objetos)
    # ------------------------------------------------------------------
    def extract_quotes(self, system_prompt: str, user_prompt: str):
        """
        Usa o LLM para extrair cotações em formato JSON (array de objetos).

        Retorna uma lista de dicionários Python.
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
        }

        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers=headers,
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        response_json = response.json()
        content = response_json["choices"][0]["message"]["content"]

        # Remover possíveis crases ``` e rótulo ```json
        text = content.strip()
        if text.startswith("```"):
            # remove blocos tipo ```json ... ```
            text = text.strip("`").strip()
            if text.lower().startswith("json"):
                text = text[4:].strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            # Em produção, ideal logar esse erro e talvez salvar o raw.
            raise RuntimeError(f"Falha ao parsear JSON do LLM: {e}\nConteúdo: {text[:500]}")

        if not isinstance(data, list):
            raise RuntimeError("A resposta do LLM não é um array JSON.")

        return data


llm_client = LLMClient()
