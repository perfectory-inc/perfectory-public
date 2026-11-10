#!/usr/bin/env python3
"""Measures one local model runtime on the notice sample: accuracy, citations and speed (root ADR-0478).

The runtime is reached only through its OpenAI-compatible chat endpoint (the same port the
application uses, MODEL_RUNTIME_*), so the same run measures Ollama, Strata or any other engine.

A sample is a directory of documents, each `<id>.txt` (the document's text, pages joined by \\f)
and `<id>.gold.json` ({"facts": {field: value}}): the values a person checked, or the provider's
own structured fields for the same notice (ILIS gives the notice number, the complex name and the
notice date beside the attachment). Every document is asked the same question, at temperature
`evaluation.temperature` of config/model-runtime.contract.json, one at a time (the runtimes serve
one request at a time). A fact counts as correct when its normalized value equals the gold value;
a citation counts as valid when its quote is in the document's text verbatim, whitespace aside.

    python3 eval_extraction.py --sample DIR --base-url http://127.0.0.1:11434 --model NAME --out report.json

Standard library only, so it runs on the host without an environment of its own.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
import time
import urllib.error
import urllib.request

CONTRACT = pathlib.Path(__file__).resolve().parents[2] / "config/model-runtime.contract.json"
FIELDS = {
    "notice_number": "고시 번호(예: OO시 고시 제2026-1호)",
    "notice_date": "고시한 날짜, YYYY-MM-DD",
    "complex_name": "산업단지 이름",
    "notice_kind": "고시의 종류(지정, 계획 승인, 변경, 실시계획 승인, 준공 등)",
    "developer": "사업시행자",
    "area_m2": "산업단지 면적(제곱미터, 숫자만). 변경이면 변경 후 면적",
}
SYSTEM = (
    "너는 한국 산업단지 고시문에서 사실을 뽑는다. 문서에 없는 것은 만들지 않는다. "
    "각 사실은 그 값을 뒷받침하는 문서 속 문장 조각을 글자 그대로 quote 에 옮긴다. "
    "JSON 하나만 답한다."
)
WHITESPACE = re.compile(r"\s+")
# A PDF text layer can carry NUL and other control characters (2026-11-11: ILIS notice 20117,
# pdfplumber); a C-string runtime ends the prompt at the first NUL, so the model saw 255 of ~9,000 tokens.
CONTROL = re.compile(r"[\x00-\x08\x0b\x0e-\x1f]")


def squeeze(text: str) -> str:
    return WHITESPACE.sub("", text)


DASHES = str.maketrans({c: "-" for c in "\u2010\u2011\u2012\u2013\u2014\u2212"})


def normalize(field: str, value: object) -> str:
    text = squeeze(str(value or "")).translate(DASHES)
    if field == "notice_date":
        digits = re.sub(r"[^0-9]", "", text)
        return digits[:8]
    if field == "area_m2":
        return re.sub(r"[^0-9.]", "", text).rstrip("0").rstrip(".")
    if field == "complex_name":
        text = re.sub(r"[()（）\[\]·,]", "", text)
    return text.replace("「", "").replace("」", "")


def matches(field: str, answer: object, gold: object) -> bool:
    """A complex name matches when one spelling holds the other: ILIS keeps the short name
    (김해테크노밸리) where the notice writes the full one (김해테크노밸리 일반산업단지)."""
    got, want = normalize(field, answer), normalize(field, gold)
    if field == "complex_name":
        return bool(got and want) and (want in got or got in want)
    return got == want


def prompt(text: str) -> str:
    wanted = "\n".join(f"- {name}: {meaning}" for name, meaning in FIELDS.items())
    return (
        f"다음 고시문에서 아래 항목을 뽑아라. 문서에 없으면 그 항목을 빼라.\n{wanted}\n\n"
        '답의 모양: {"facts": [{"field": "<항목>", "value": "<값>", "quote": "<문서 속 그대로의 조각>"}]}\n\n'
        f"고시문:\n{text}"
    )


def ask(base_url: str, path: str, model: str, text: str, temperature: float, max_tokens: int,
        timeout: int) -> tuple[dict, float]:
    body = {
        "model": model,
        "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt(text)}],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
        "stream": False,
        "reasoning_effort": "none",
    }
    request = urllib.request.Request(base_url.rstrip("/") + path, data=json.dumps(body).encode(),
                                     headers={"content-type": "application/json"})
    started = time.monotonic()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        answer = json.loads(response.read())
    return answer, time.monotonic() - started


def parse_facts(content: str) -> list[dict]:
    match = re.search(r"\{.*\}", content or "", re.S)
    if not match:
        return []
    try:
        facts = json.loads(match.group(0)).get("facts", [])
    except (json.JSONDecodeError, AttributeError):
        return []
    return [fact for fact in facts if isinstance(fact, dict) and fact.get("field") in FIELDS]


def score(text: str, gold: dict, facts: list[dict]) -> dict:
    by_field = {}
    for fact in facts:
        by_field.setdefault(fact["field"], fact)
    checked = {field: matches(field, by_field[field].get("value"), value)
               for field, value in gold.items() if field in by_field}
    haystack = squeeze(text)
    cited = [bool(squeeze(str(fact.get("quote") or ""))) and squeeze(str(fact["quote"])) in haystack for fact in facts]
    return {
        "gold_fields": len(gold),
        "answered": len(checked),
        "correct": sum(checked.values()),
        "wrong": sorted(field for field, ok in checked.items() if not ok),
        "missing": sorted(set(gold) - set(by_field)),
        "facts": len(facts),
        "valid_citations": sum(cited),
    }


def evaluate(sample: pathlib.Path, base_url: str, chat_path: str, model: str, max_chars: int,
             temperature: float, max_tokens: int, timeout: int) -> dict:
    documents = []
    for text_file in sorted(sample.glob("*.txt")):
        gold_file = text_file.with_suffix(".gold.json")
        gold = json.loads(gold_file.read_text(encoding="utf-8"))["facts"] if gold_file.is_file() else {}
        text = CONTROL.sub(" ", text_file.read_text(encoding="utf-8"))[:max_chars]
        entry = {"id": text_file.stem, "chars": len(text)}
        try:
            answer, seconds = ask(base_url, chat_path, model, text, temperature, max_tokens, timeout)
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as error:
            entry.update(error=str(error))
            documents.append(entry)
            continue
        usage = answer.get("usage") or {}
        content = ((answer.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        facts = parse_facts(content)
        entry.update(seconds=round(seconds, 2), prompt_tokens=usage.get("prompt_tokens"),
                     completion_tokens=usage.get("completion_tokens"), **score(text, gold, facts),
                     answer=facts)
        documents.append(entry)
        print(json.dumps({k: v for k, v in entry.items() if k != "answer"}, ensure_ascii=False), flush=True)
    done = [d for d in documents if "error" not in d]
    total_seconds = sum(d["seconds"] for d in done)
    completion = sum(d.get("completion_tokens") or 0 for d in done)
    answered = sum(d["answered"] for d in done)
    gold = sum(d["gold_fields"] for d in done)
    return {
        "model": model,
        "base_url": base_url,
        "max_chars": max_chars,
        "documents": len(documents),
        "failed": len(documents) - len(done),
        "seconds_per_document": round(total_seconds / len(done), 2) if done else None,
        "documents_per_hour": round(3600 * len(done) / total_seconds, 1) if total_seconds else None,
        "completion_tokens_per_second": round(completion / total_seconds, 2) if total_seconds else None,
        "gold_fields": gold,
        "answered": answered,
        "precision": round(sum(d["correct"] for d in done) / answered, 4) if answered else None,
        "recall": round(sum(d["correct"] for d in done) / gold, 4) if gold else None,
        "citation_validity": round(sum(d["valid_citations"] for d in done) / max(1, sum(d["facts"] for d in done)), 4),
        "per_document": documents,
    }


def main(argv: list[str]) -> int:
    evaluation = json.loads(CONTRACT.read_text(encoding="utf-8"))["evaluation"]
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sample", type=pathlib.Path, required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--chat-path", default="/v1/chat/completions")
    parser.add_argument("--model", required=True)
    parser.add_argument("--max-chars", type=int, default=12000,
                        help="the first characters of each document the model reads (the same for every runtime)")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    arguments = parser.parse_args(argv)
    report = evaluate(arguments.sample, arguments.base_url, arguments.chat_path, arguments.model,
                      arguments.max_chars, evaluation["temperature"], evaluation["max_output_tokens"],
                      arguments.timeout)
    arguments.out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "per_document"}, ensure_ascii=False))
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
