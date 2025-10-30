from __future__ import annotations

import sys
from functools import lru_cache
from typing import Optional
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigError(RuntimeError):
    """Erro de configuração amigável para o operador do pipeline."""


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    gcp_project_id: str = Field(..., alias="GCP_PROJECT_ID")
    gcs_bucket: str = Field(..., alias="GCS_BUCKET")
    gmail_label: str = Field(..., alias="GMAIL_LABEL")
    sheet_id: str = Field(..., alias="SHEET_ID")

    @property
    def is_valid(self) -> bool:
        return all(
            [
                bool(self.gcp_project_id.strip()),
                bool(self.gcs_bucket.strip()),
                bool(self.gmail_label.strip()),
                bool(self.sheet_id.strip()),
            ]
        )


def load_config() -> AppConfig:
    """Carrega a configuração diretamente do ambiente/.env (sem cache)."""
    try:
        cfg = AppConfig()  # lê de env + .env
    except ValidationError as ve:
        # Campos ausentes/invalidos -> mensagem amigável
        missing = []
        for err in ve.errors():
            loc = ".".join(str(x) for x in err.get("loc", []))
            if "field required" in err.get("msg", ""):
                missing.append(loc)
        msg = (
            "Configuração incompleta. Verifique seu .env (ou variáveis de ambiente).\n"
            f"Campos obrigatórios ausentes: {', '.join(missing) if missing else 'desconhecido'}\n"
            "Exemplo em .env.template"
        )
        raise ConfigError(msg) from ve

    # Regras adicionais (ex.: nomes simples, sem espaços, etc.)
    if "/" in cfg.gcs_bucket or cfg.gcs_bucket.strip() == "":
        raise ConfigError("GCS_BUCKET inválido. Use apenas o nome do bucket (sem gs:// e sem barras).")

    if cfg.gmail_label.strip() == "":
        raise ConfigError("GMAIL_LABEL não pode estar vazio.")

    if not cfg.is_valid:
        raise ConfigError("Config inválida. Revise seu .env conforme .env.template.")

    return cfg


@lru_cache
def get_settings() -> AppConfig:
    """Versão com cache (idempotente) para ser usada pelo resto do projeto."""
    return load_config()


# Compat: permite `from app.core.config import settings`
settings: AppConfig = get_settings()

__all__ = ["AppConfig", "ConfigError", "load_config", "get_settings", "settings"]


def _print_ok(cfg: AppConfig) -> None:
    print("✅ Config carregada com sucesso:")
    print(f"- GCP_PROJECT_ID = {cfg.gcp_project_id}")
    print(f"- GCS_BUCKET     = {cfg.gcs_bucket}")
    print(f"- GMAIL_LABEL    = {cfg.gmail_label}")
    print(f"- SHEET_ID       = {cfg.sheet_id}")


if __name__ == "__main__":
    # Smoke test: `python -m app.core.config` quando estiver no PYTHONPATH correto
    try:
        cfg = get_settings()  # usa a versão cacheada
        _print_ok(cfg)
        sys.exit(0)
    except ConfigError as e:
        print(f"❌ Erro de configuração:\n{e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"❌ Erro inesperado ao carregar config: {e!r}", file=sys.stderr)
        sys.exit(3)
