# src/app/main.py
from __future__ import annotations
import argparse, json, sys
from datetime import date, timedelta

from app.core.config import get_settings
from app.gmail.labeler_llm import run as labeler_run          # etapa opcional p/ rotular QUOTES
from app.gmail.incremental import run as gmail_run            # baixa RAW em GCS (raw/)
from app.parser.minimal import run as parser_run              # gera parsed/ (pode skipar se já existir)
from app.parser.normalize_parsed_email import run as normalize_run  # gera parsed_fixed/
from app.sheets.sync_quotes_raw import run as sheets_run      # lê parsed_fixed/ e sobe p/ Sheets

# Agora o pipeline inclui normalização antes do Sheets
STEPS = ("labeler", "gmail", "parser", "normalize", "sheets")


def _resolve_range(range_expr: str | None) -> tuple[str | None, str | None]:
    if not range_expr:
        return None, None
    if range_expr.startswith("newer_than:") and range_expr.endswith("d"):
        days = int(range_expr.split(":")[1][:-1])
        before = date.today().isoformat()
        after = (date.today() - timedelta(days=days)).isoformat()
        return after, before
    return None, None


def _build_labeler_query(after: str | None, before: str | None, fallback_days: int = 30) -> str:
    """
    Monta uma query compatível com o Gmail Search para o LABELER.
    - Usa 'in:anywhere' para encontrar e-mails mesmo fora da inbox.
    - Se after/before não vierem, usa newer_than:<fallback_days>d.
    - Inclui '-label:QUOTES' para evitar relabelar o que já está rotulado.
    """
    parts = ["in:anywhere"]
    if after or before:
        if after:
            parts.append(f"after:{after.replace('-', '/')}")
        if before:
            parts.append(f"before:{before.replace('-', '/')}")
    else:
        parts.append(f"newer_than:{fallback_days}d")
    parts.append("-label:QUOTES")
    return " ".join(parts)


def run_pipeline(
    only: list[str] | None = None,
    after: str | None = None,
    before: str | None = None,
    label: str | None = None,
    labeler_limit: int | None = None,
) -> int:
    s = get_settings()
    label = label or s.gmail_label or "QUOTES"
    selected = only or list(STEPS)
    summaries: dict[str, object] = {}

    try:
        # 1) LABELER — classifica threads para QUOTES
        if "labeler" in selected:
            q = _build_labeler_query(after=after, before=before, fallback_days=30)
            summaries["labeler"] = labeler_run(
                q=q,
                label=label,
                limit=labeler_limit or 100,
            )

        # 2) GMAIL — baixa/atualiza raws apenas do label alvo
        if "gmail" in selected:
            summaries["gmail"] = gmail_run(after=after, before=before, label=label)

        # 3) PARSER — extrai campos/headers brutos (gera parsed/)
        if "parser" in selected:
            summaries["parser"] = parser_run()

        # 4) NORMALIZE — transforma parsed/ → parsed_fixed/ (linhas prontas p/ Sheets)
        if "normalize" in selected:
            summaries["normalize"] = normalize_run()

        # 5) SHEETS — dedup e append na planilha (usa parsed_fixed/)
        if "sheets" in selected:
            summaries["sheets"] = sheets_run()

        print(json.dumps({"ok": True, "summaries": summaries}, ensure_ascii=False))
        return 0

    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "summaries": summaries}, ensure_ascii=False))
        return 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline: labeler → gmail → parser → normalize → sheets (rodada única)")
    p.add_argument("--only", nargs="+", choices=STEPS,
                   help="rodar etapas específicas (labeler gmail parser normalize sheets)")
    p.add_argument("--after", help="YYYY-MM-DD (janela Gmail)")
    p.add_argument("--before", help="YYYY-MM-DD (janela Gmail)")
    p.add_argument("--label", help='label do Gmail para destino (default: settings.gmail_label ou "QUOTES")')
    p.add_argument("--range", help='atalho, ex.: newer_than:7d')
    p.add_argument("--labeler-limit", type=int, help="limite de threads a rotular (default: 100)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    after, before = args.after, args.before
    if args.range and not (after or before):
        ra, rb = _resolve_range(args.range)
        after = after or ra
        before = before or rb
    sys.exit(
        run_pipeline(
            only=args.only,
            after=after,
            before=before,
            label=args.label,
            labeler_limit=args.labeler_limit,
        )
    )
