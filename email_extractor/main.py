from __future__ import annotations
import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Dict
import os

ROOT = Path(__file__).resolve().parent

STEPS = [
    ("dump_threads", "dump_threads.py"),
    ("llm_extract_data", "llm_extract_data.py"),
    ("llm_write_followup_emails", "llm_write_followup_emails.py"),
    ("save_quotes_to_csv", "save_quotes_to_csv.py"),
]

def run_cli(script: Path, extra_args: List[str]) -> int:
    """Executa um script Python como CLI, repassando argumentos extras."""
    cmd = [sys.executable, str(script), *extra_args]
    print(f"→ executando: {' '.join(cmd)}")

    # garante que 'modules/' no repo root esteja no sys.path de todos os subprocessos
    env = os.environ.copy()
    repo_root = ROOT.parent  # .../louro-jose
    env["PYTHONPATH"] = f"{str(repo_root)}{os.pathsep}{env.get('PYTHONPATH','')}".rstrip(os.pathsep)

    try:
        # use subprocess.run para poder passar env (ou call com env também funciona em py3.8+)
        completed = subprocess.run(cmd, env=env)
        return completed.returncode
    except KeyboardInterrupt:
        print("\n⏹️  Interrompido pelo usuário.")
        return 130

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pipeline Parrot: dump → extract → followups → sheets"
    )
    p.add_argument("--only", choices=[name for name, _ in STEPS],
                   help="Roda apenas uma etapa.")
    p.add_argument("--until", choices=[name for name, _ in STEPS],
                   help="Roda da primeira etapa até esta (inclusive).")
    p.add_argument("--skip", nargs="*", default=[],
                   help="Nomes de etapas para pular. Ex.: --skip llm_write_followup_emails")
    p.add_argument("--dump-args", nargs=argparse.REMAINDER,
                   help="Flags extras para dump_threads.py (use após '--').")
    p.add_argument("--extract-args", nargs=argparse.REMAINDER,
                   help="Flags extras para llm_extract_data.py (use após '--').")
    p.add_argument("--followup-args", nargs=argparse.REMAINDER,
                   help="Flags extras para llm_write_followup_emails.py (use após '--').")
    p.add_argument("--save-args", nargs=argparse.REMAINDER,
                   help="Flags extras para save_quotes_to_csv.py (use após '--').")
    return p.parse_args()

def main():
    args = parse_args()

    extras: Dict[str, List[str]] = {
        "dump_threads": args.dump_args or [],
        "llm_extract_data": args.extract_args or [],
        "llm_write_followup_emails": args.followup_args or [],
        "save_quotes_to_csv": args.save_args or [],
    }

    ordered_names = [name for name, _ in STEPS]
    if args.only:
        plan = [args.only]
    elif args.until:
        idx = ordered_names.index(args.until)
        plan = ordered_names[: idx + 1]
    else:
        plan = ordered_names[:]

    skipset = set(args.skip)
    plan = [name for name in plan if name not in skipset]

    if not plan:
        print("⚠️  Nada para executar (verifique --only/--until/--skip).")
        sys.exit(0)

    for name, filename in STEPS:
        if name not in plan:
            continue
        script_path = ROOT / filename
        if not script_path.exists():
            print(f"⛔ Arquivo não encontrado: {script_path}")
            sys.exit(1)

        print(f"\n=== [{name}] ===")
        code = run_cli(script_path, extras.get(name, []))
        if code != 0:
            print(f"❌ Etapa '{name}' falhou com código {code}. Abortando.")
            sys.exit(code)

    print("\n✅ Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()
