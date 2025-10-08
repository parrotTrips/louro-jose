from dotenv import load_dotenv
import argparse
from openai import OpenAI, AuthenticationError, RateLimitError, APIConnectionError, BadRequestError
import os

load_dotenv()

DEFAULT_TEXT = (
    "Este é um teste rápido para verificar se a minha chave da OpenAI está funcionando "
    "para gerar embeddings com o modelo text-embedding-3-small."
)

def masked_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 8:
        return "*" * len(key)
    return key[:4] + "*" * (len(key) - 8) + key[-4:]

def main():
    parser = argparse.ArgumentParser(description="Teste de embeddings com OpenAI.")
    parser.add_argument("--text", "-t", default=DEFAULT_TEXT, help="Texto para gerar embedding.")
    parser.add_argument("--model", "-m", default="text-embedding-3-small", help="Modelo de embedding.")
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        print("❌ A variável de ambiente OPENAI_API_KEY não está definida.")
        print("   Defina com: export OPENAI_API_KEY='sua_chave_aqui'")
        raise SystemExit(1)

    print(f"🔑 OPENAI_API_KEY: {masked_key(api_key)}")
    print(f"🧠 Modelo: {args.model}")
    print(f"📝 Texto (primeiros 80 chars): {args.text[:80]}{'...' if len(args.text) > 80 else ''}")

    try:
        client = OpenAI()  # usa OPENAI_API_KEY do ambiente
        resp = client.embeddings.create(
            input=args.text,
            model=args.model,
        )
        vec = resp.data[0].embedding
        print("✅ Embedding gerado com sucesso!")
        print(f"   • Dimensão: {len(vec)}")
        # Mostra os 8 primeiros valores para inspecionar rapidamente
        preview = ", ".join(f"{v:.6f}" for v in vec[:8])
        print(f"   • Início do vetor: [{preview}, ...]")
        # Campos auxiliares (se presentes)
        try:
            if hasattr(resp, "usage") and resp.usage:
                print(f"   • Tokens de prompt: {resp.usage.prompt_tokens} | Total: {resp.usage.total_tokens}")
        except Exception:
            pass
    except AuthenticationError as e:
        print("❌ Erro de autenticação. Verifique se sua OPENAI_API_KEY está correta e ativa.")
        print(f"   Detalhes: {e}")
    except RateLimitError as e:
        print("⏳ Rate limit atingido. Tente novamente em instantes ou ajuste seu plano/limites.")
        print(f"   Detalhes: {e}")
    except BadRequestError as e:
        print("❌ Requisição inválida (BadRequest). Verifique o texto ou o nome do modelo.")
        print(f"   Detalhes: {e}")
    except APIConnectionError as e:
        print("🌐 Falha de conexão com a API. Cheque sua internet ou tente mais tarde.")
        print(f"   Detalhes: {e}")
    except Exception as e:
        print("❌ Erro inesperado ao gerar embedding.")
        print(f"   Detalhes: {e}")

if __name__ == "__main__":
    main()
