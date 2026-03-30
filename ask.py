"""
Natural language Q&A against ART business data.
PHI-safe: only aggregated, de-identified summaries are sent to the Claude API.

Usage:
  python ask.py                          # interactive mode
  python ask.py "what was net income in october?"  # single question
"""

import argparse
import json
import os
import re
import sys
import anthropic
import db_cache


def _load_api_key():
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "art_config.json")
    if os.path.exists(config_path):
        with open(config_path) as f:
            key = json.load(f).get("anthropic_api_key", "")
        if key:
            return key
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        return key
    print("ERROR: Anthropic API key not found.")
    print("Add to art_config.json:  \"anthropic_api_key\": \"sk-ant-...\"")
    print("Or set env var:          $env:ANTHROPIC_API_KEY = \"sk-ant-...\"")
    sys.exit(1)

MODEL = "claude-3-haiku-20240307"
OLLAMA_MODEL = "glm-4.7-flash:latest"
OLLAMA_BASE_URL = "http://localhost:11434/v1"

SYSTEM_PROMPT = """You are a business analyst for ART (Accelerated Rehab Therapy), \
a physical therapy practice with two locations in Colorado.

- ART Greeley — Greeley, CO
- ART Denver  — Denver, CO

You answer questions using the monthly data summaries provided. \
Be concise, specific, and format currency as $X,XXX. \
When comparing months or locations, highlight the key differences. \
If the data doesn't support a confident answer, say so clearly."""


def _build_visit_summary():
    df = db_cache.query("""
        SELECT
            strftime(DOS, '%B %Y')  AS month,
            strftime(DOS, '%Y-%m')  AS sort_key,
            "Visit Facility"        AS facility,
            COUNT(*) FILTER (WHERE "Visit Stage" IN ('Open','Closed','Review'))
                AS checked_in,
            COUNT(*) FILTER (WHERE "Visit Stage" IN ('Open','Closed','Review')
                AND ("Last Billed" IS NULL OR "Last Billed" = 0))
                AS unbilled,
            COUNT(*) FILTER (WHERE "Visit Stage" IN ('Patient Canceled','Center Canceled'))
                AS cancelled,
            COUNT(*) FILTER (WHERE "Visit Stage" = 'No Show')
                AS no_show,
            ROUND(SUM(COALESCE("Last Billed", 0))
                FILTER (WHERE "Visit Stage" IN ('Open','Closed','Review')), 0)
                AS gross_charges,
            ROUND(SUM(COALESCE("Total Paid", 0)), 0) AS collected,
            ROUND(SUM(COALESCE("Primary Allowed", 0))
                FILTER (WHERE "Visit Stage" IN ('Open','Closed','Review')), 0)
                AS primary_allowed
        FROM prompt_visits
        GROUP BY month, sort_key, facility
        ORDER BY sort_key, facility
    """)

    lines = ["## Visit Metrics by Month and Facility (Source: Prompt EMR)\n"]
    lines.append(
        f"{'Month':<17} {'Facility':<13} {'Checked-In':>11} {'Unbilled':>9} "
        f"{'Cancelled':>10} {'No-Show':>8} {'Gross Charges':>14} {'Collected':>11} {'Allowed':>10}"
    )
    lines.append("-" * 100)
    for _, row in df.iterrows():
        lines.append(
            f"{row['month']:<17} {row['facility']:<13} {int(row['checked_in']):>11} "
            f"{int(row['unbilled']):>9} {int(row['cancelled']):>10} {int(row['no_show']):>8} "
            f"${int(row['gross_charges']):>13,} ${int(row['collected']):>10,} ${int(row['primary_allowed']):>9,}"
        )
    return "\n".join(lines)


def _parse_pl_amount(val):
    if val is None:
        return 0.0
    cleaned = re.sub(r"[$,\s]", "", str(val)).strip()
    if not cleaned or cleaned in ("-", "nan"):
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _extract_pl_data(pl_df):
    label_col = "Profit and Loss - Monthly"
    month_cols = [c for c in pl_df.columns if c != label_col]

    period_map = {}
    for _, row in pl_df.iterrows():
        val = str(row.iloc[1]).strip()
        if re.match(
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)",
            val,
        ):
            period_map = {col: str(row[col]).strip() for col in month_cols}
            break

    key_lines = ["Total for Income", "Total for Expenses", "Net Income", "Contract labor"]
    rows = []
    for label in key_lines:
        match = pl_df[pl_df[label_col].astype(str).str.strip() == label]
        if match.empty:
            continue
        row = match.iloc[0]
        for col, period in period_map.items():
            if period == "Total":
                continue
            rows.append(
                {
                    "period": period,
                    "line_item": label,
                    "amount": _parse_pl_amount(row[col]),
                }
            )
    return rows


def _build_pl_summary():
    greeley_pl = db_cache.query("SELECT * FROM greeley_pl")
    denver_pl  = db_cache.query("SELECT * FROM denver_pl")

    greeley_rows = _extract_pl_data(greeley_pl)
    denver_rows  = _extract_pl_data(denver_pl)

    month_order = [
        "September 2025", "October 2025", "November 2025", "December 2025",
        "January 2026", "February 2026", "March 2026",
    ]

    def pl_table(rows, facility_name):
        by_period = {}
        for r in rows:
            by_period.setdefault(r["period"], {})[r["line_item"]] = r["amount"]

        lines = [f"\n### {facility_name} P&L (Source: QuickBooks)\n"]
        lines.append(
            f"{'Month':<17} {'Revenue':>12} {'Expenses':>12} {'Net Income':>12} {'Contract Labor':>15}"
        )
        lines.append("-" * 72)
        for month in month_order:
            d = by_period.get(month, {})
            rev  = d.get("Total for Income",    0.0)
            exp  = d.get("Total for Expenses",  0.0)
            net  = d.get("Net Income",           0.0)
            clab = d.get("Contract labor",       0.0)
            lines.append(
                f"{month:<17} ${rev:>11,.0f} ${exp:>11,.0f} ${net:>11,.0f} ${clab:>14,.0f}"
            )
        return "\n".join(lines)

    return pl_table(greeley_rows, "ART Greeley") + "\n" + pl_table(denver_rows, "ART Denver")


def build_context():
    visit_summary = _build_visit_summary()
    pl_summary    = _build_pl_summary()
    return f"{visit_summary}\n\n{pl_summary}"


def ask_question_ollama(client, context, question, history):
    history.append({"role": "user", "content": question})

    system_msg = {
        "role": "system",
        "content": f"{SYSTEM_PROMPT}\n\n## Business Data\n\n{context}",
    }
    stream = client.chat.completions.create(
        model=OLLAMA_MODEL,
        messages=[system_msg, *history],
        stream=True,
    )
    answer = ""
    for chunk in stream:
        text = chunk.choices[0].delta.content or ""
        print(text, end="", flush=True)
        answer += text
    print()

    history.append({"role": "assistant", "content": answer})
    return answer


def ask_question(client, context, question, history):
    history.append({"role": "user", "content": question})

    with client.messages.stream(
        model=MODEL,
        max_tokens=4096,
        system=[
            {"type": "text", "text": SYSTEM_PROMPT},
            {
                "type": "text",
                "text": f"## Business Data\n\n{context}",
                "cache_control": {"type": "ephemeral"},
            },
        ],
        messages=history,
    ) as stream:
        answer = ""
        for text in stream.text_stream:
            print(text, end="", flush=True)
            answer += text
        print()

    history.append({"role": "assistant", "content": answer})
    return answer


def main():
    con = db_cache.connect()
    tables = {r[0] for r in con.execute("SELECT table_name FROM _cache_meta").fetchall()}
    con.close()

    missing = {"prompt_visits", "greeley_pl", "denver_pl"} - tables
    if missing:
        print(f"ERROR: Cache missing: {missing}. Run: python db_cache.py")
        sys.exit(1)

    print("Building data summaries...", end=" ", flush=True)
    context = build_context()
    print("done.")

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--ollama", action="store_true")
    args, remaining = parser.parse_known_args()

    if args.ollama:
        from openai import OpenAI
        client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
        ask_fn = ask_question_ollama
        label = "GLM"
    else:
        client = anthropic.Anthropic(api_key=_load_api_key())
        ask_fn = ask_question
        label = "Claude"

    history = []

    if remaining:
        question = " ".join(remaining)
        print(f"\n{question}\n")
        ask_fn(client, context, question, history)
        return

    print(f"\nART Business Q&A ({label}) — type your question, or 'quit' to exit.\n")
    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            break
        print(f"\n{label}: ", end="")
        ask_fn(client, context, question, history)
        print()


if __name__ == "__main__":
    main()
