import pytest


# --- _make_row_key ---

def test_make_row_key_is_stable():
    from app.agents.extractor_agent import _make_row_key
    row = {
        "Nome do hotel": "Hotel ABC",
        "Cidade": "São Paulo",
        "Check-in": "2024-03-10",
        "Check-out": "2024-03-12",
        "Categoria do quarto": "Standard",
        "Configuração do quarto": "Double",
        "Preço (num)": 350.0,
    }
    key1 = _make_row_key("thread123", row)
    key2 = _make_row_key("thread123", row)
    assert key1 == key2
    assert len(key1) == 40  # SHA-1 hex


def test_make_row_key_differs_by_price():
    from app.agents.extractor_agent import _make_row_key
    row_a = {"Nome do hotel": "H", "Cidade": "SP", "Check-in": "2024-03-10",
             "Check-out": "2024-03-12", "Categoria do quarto": "Std",
             "Configuração do quarto": "DBL", "Preço (num)": 300}
    row_b = {**row_a, "Preço (num)": 400}
    assert _make_row_key("t1", row_a) != _make_row_key("t1", row_b)


def test_make_row_key_differs_by_thread():
    from app.agents.extractor_agent import _make_row_key
    row = {"Nome do hotel": "H", "Cidade": "SP", "Check-in": "2024-03-10",
           "Check-out": "2024-03-12", "Categoria do quarto": "Std",
           "Configuração do quarto": "DBL", "Preço (num)": 300}
    assert _make_row_key("thread_a", row) != _make_row_key("thread_b", row)


# --- _score_message ---

def test_score_external_sender_with_price():
    from app.agents.extractor_agent import _score_message
    body = "Tarifa: R$ 350,00 por diária. Quarto standard disponível para check-in."
    score = _score_message("reservas@hotel.com.br", body)
    assert score > 0


def test_score_parrot_sender_penalized():
    from app.agents.extractor_agent import _score_message
    body = "Tarifa: R$ 350,00 por diária. Quarto standard disponível."
    score_external = _score_message("hotel@externo.com", body)
    score_parrot = _score_message("fulano@parrottrips.com", body)
    assert score_external > score_parrot


def test_score_short_thanks_message():
    from app.agents.extractor_agent import _score_message
    score = _score_message("hotel@externo.com", "Obrigado pelo contato!")
    assert score < 0


def test_score_empty_body():
    from app.agents.extractor_agent import _score_message
    assert _score_message("hotel@externo.com", "") == -999


# --- _strip_reply_history_and_signature ---

def test_strip_removes_forwarded_block():
    from app.agents.extractor_agent import _strip_reply_history_and_signature
    body = "Preço R$ 200\n\n-----Mensagem Original-----\nDe: alguém"
    result = _strip_reply_history_and_signature(body)
    assert "200" in result
    assert "Mensagem Original" not in result


def test_strip_removes_quoted_lines():
    from app.agents.extractor_agent import _strip_reply_history_and_signature
    body = "Tarifa disponível\n> Em 10 mar, alguém escreveu:\n> texto antigo"
    result = _strip_reply_history_and_signature(body)
    assert "Tarifa disponível" in result
    assert "texto antigo" not in result


def test_strip_removes_disclaimer():
    from app.agents.extractor_agent import _strip_reply_history_and_signature
    body = "Tarifa R$300\n\nEsta mensagem é confidencial e pode conter informação"
    result = _strip_reply_history_and_signature(body)
    assert "300" in result
    assert "confidencial" not in result
