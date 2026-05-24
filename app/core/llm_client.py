import json
import re
import logging
from typing import Any, Dict, List

import requests

from app.core.config import settings

logger = logging.getLogger(__name__)


def _strip_markdown_fences(text: str) -> str:
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return text.strip()


class LLMClient:
    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def extract_quotes(self, system_prompt: str, user_prompt: str) -> List[Dict[str, Any]]:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
        }
        resp = requests.post(
            f"{self.base_url}/chat/completions",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]

        text = _strip_markdown_fences(content)

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Falha ao parsear JSON do LLM: {e}\nConteúdo: {text[:500]}"
            )

        if not isinstance(data, list):
            raise RuntimeError("A resposta do LLM não é um array JSON.")

        return data


def make_llm_client() -> LLMClient:
    return LLMClient(
        api_key=settings.OPENAI_API_KEY,
        base_url=settings.OPENAI_BASE_URL,
        model=settings.OPENAI_MODEL,
    )
