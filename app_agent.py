from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, Response

from nested_json_to_csv import (
    iter_candidate_paths,
    json_to_rows,
    rows_to_csv_text,
    rows_to_excel_bytes,
    suggest_records_path,
)

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


def load_dotenv(dotenv_path: str | Path) -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


for dotenv_name in (".env.local", ".env"):
    load_dotenv(dotenv_name)

app = FastAPI(title="Nested JSON Agent")
DOWNLOAD_CACHE: dict[str, dict[str, Any]] = {}

INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Nested JSON Agent</title>
  <style>
    :root {
      --bg: #f4efe6;
      --panel: #fffaf2;
      --ink: #1e1b18;
      --muted: #6c6258;
      --accent: #0e7490;
      --accent-strong: #155e75;
      --line: #d8cbb8;
      --shadow: 0 20px 50px rgba(78, 61, 38, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(14, 116, 144, 0.16), transparent 30%),
        linear-gradient(135deg, #f8f2e9 0%, #efe4d4 100%);
      min-height: 100vh;
    }
    .page {
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 18px 40px;
    }
    .hero {
      background: rgba(255, 250, 242, 0.84);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(216, 203, 184, 0.9);
      border-radius: 24px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .hero-top {
      padding: 28px 24px 18px;
      border-bottom: 1px solid rgba(216, 203, 184, 0.8);
      background: linear-gradient(135deg, rgba(14, 116, 144, 0.08), rgba(255, 250, 242, 0.55));
    }
    h1 {
      margin: 0 0 8px;
      font-size: clamp(2rem, 4vw, 3.6rem);
      line-height: 0.95;
      letter-spacing: -0.05em;
    }
    .subtext {
      margin: 0;
      max-width: 760px;
      color: var(--muted);
      font-size: 1rem;
    }
    .grid {
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 0;
    }
    .controls {
      padding: 24px;
      border-right: 1px solid rgba(216, 203, 184, 0.85);
      background: rgba(255, 248, 238, 0.9);
    }
    .results {
      padding: 24px;
      min-width: 0;
    }
    label {
      display: block;
      margin-bottom: 8px;
      font-size: 0.95rem;
      font-weight: 700;
    }
    .field {
      margin-bottom: 18px;
    }
    input[type="file"], input[type="text"] {
      width: 100%;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fffdf8;
      color: var(--ink);
      font: inherit;
    }
    .hint {
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.88rem;
      line-height: 1.4;
    }
    button {
      width: 100%;
      padding: 12px 16px;
      border: 0;
      border-radius: 999px;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      color: white;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.15s ease, opacity 0.15s ease;
    }
    button:hover { transform: translateY(-1px); }
    button:disabled { opacity: 0.6; cursor: wait; }
    .meta {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 18px;
      color: var(--muted);
      font-size: 0.92rem;
    }
    .pill {
      padding: 8px 12px;
      border-radius: 999px;
      background: #f5ede0;
      border: 1px solid var(--line);
    }
    .actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 18px;
    }
    .actions a {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 140px;
      padding: 10px 14px;
      text-decoration: none;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--ink);
      background: #fffdf8;
      font-weight: 700;
    }
    .table-shell {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow-x: auto;
      overflow-y: auto;
      max-width: 100%;
      max-height: 62vh;
      background: #fffdf9;
      display: block;
    }
    table {
      width: max-content;
      min-width: 100%;
      border-collapse: collapse;
      font-size: 0.94rem;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid #eadfce;
      border-right: 1px solid #f1e8db;
      white-space: nowrap;
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      background: #f7efe3;
      z-index: 1;
    }
    .empty {
      padding: 48px 20px;
      text-align: center;
      color: var(--muted);
      border: 1px dashed var(--line);
      border-radius: 18px;
      background: rgba(255, 250, 242, 0.7);
    }
    .error {
      margin-top: 12px;
      padding: 12px 14px;
      border-radius: 14px;
      color: #8a1c1c;
      background: #fde8e8;
      border: 1px solid #f5b7b7;
      display: none;
    }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      .controls { border-right: 0; border-bottom: 1px solid rgba(216, 203, 184, 0.85); }
      .table-shell { max-height: 50vh; }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="hero-top">
        <h1>Nested JSON to Table Agent</h1>
        <p class="subtext">Upload a nested JSON file, describe what you want in plain English, and the app will choose the records path for you before flattening to CSV or Excel.</p>
      </div>
      <div class="grid">
        <aside class="controls">
          <form id="convert-form">
            <div class="field">
              <label for="json-file">JSON file</label>
              <input id="json-file" name="file" type="file" accept=".json,application/json" required />
            </div>
            <div class="field">
              <label for="instruction">Ask in plain English</label>
              <input id="instruction" name="instruction" type="text" placeholder="extract only orders" />
              <div class="hint">Example: <code>extract only orders</code> or <code>export customer records</code>.</div>
            </div>
            <div class="field">
              <label for="records-path">Records path override</label>
              <input id="records-path" name="records_path" type="text" placeholder="data.items" />
              <div class="hint">Optional. If provided, it wins over the agent's guess.</div>
            </div>
            <button id="submit-btn" type="submit">Convert and Preview</button>
            <div id="error-box" class="error"></div>
          </form>
        </aside>
        <main class="results">
          <div id="result-meta" class="meta" hidden></div>
          <div id="result-actions" class="actions" hidden></div>
          <div id="table-root" class="empty">Your flattened output will appear here.</div>
        </main>
      </div>
    </section>
  </div>
  <script>
    const form = document.getElementById("convert-form");
    const submitBtn = document.getElementById("submit-btn");
    const errorBox = document.getElementById("error-box");
    const meta = document.getElementById("result-meta");
    const actions = document.getElementById("result-actions");
    const tableRoot = document.getElementById("table-root");

    function showError(message) {
      errorBox.textContent = message;
      errorBox.style.display = "block";
    }

    function clearError() {
      errorBox.textContent = "";
      errorBox.style.display = "none";
    }

    function escapeCell(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function renderTable(columns, rows) {
      if (!columns.length) {
        tableRoot.className = "empty";
        tableRoot.textContent = "No columns were produced for this file.";
        return;
      }

      const head = columns.map((column) => `<th>${escapeCell(column)}</th>`).join("");
      const body = rows.map((row) => {
        const cells = columns.map((column) => `<td>${escapeCell(row[column] ?? "")}</td>`).join("");
        return `<tr>${cells}</tr>`;
      }).join("");

      tableRoot.className = "table-shell";
      tableRoot.innerHTML = `
        <table>
          <thead><tr>${head}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      `;
    }

    function renderMeta(payload) {
      meta.hidden = false;
      const modeLabel = payload.agent_used ? `Agent: ${payload.path_source}` : `Path source: ${payload.path_source}`;
      const chosenPath = payload.resolved_records_path || "root";
      meta.innerHTML = `
        <span class="pill">${payload.row_count} row(s)</span>
        <span class="pill">${payload.column_count} column(s)</span>
        <span class="pill">Preview shows ${payload.preview_count} row(s)</span>
        <span class="pill">${escapeCell(modeLabel)}</span>
        <span class="pill">Using ${escapeCell(chosenPath)}</span>
      `;
    }

    function renderActions(payload) {
      actions.hidden = false;
      actions.innerHTML = `
        <a href="${payload.csv_download_url}">Download CSV</a>
        <a href="${payload.excel_download_url}">Download Excel</a>
      `;
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      clearError();
      submitBtn.disabled = true;
      submitBtn.textContent = "Processing...";

      const formData = new FormData(form);

      try {
        const response = await fetch("/api/convert", {
          method: "POST",
          body: formData
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "Conversion failed.");
        }
        renderMeta(payload);
        renderActions(payload);
        renderTable(payload.columns, payload.preview_rows);
      } catch (error) {
        meta.hidden = true;
        actions.hidden = true;
        tableRoot.className = "empty";
        tableRoot.textContent = "Your flattened output will appear here.";
        showError(error.message);
      } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = "Convert and Preview";
      }
    });
  </script>
</body>
</html>
"""


def sanitize_filename(filename: str | None) -> str:
    if not filename:
        return "flattened_output"
    return Path(filename).stem or "flattened_output"


def serialize_preview_row(row: dict[str, Any], fieldnames: list[str]) -> dict[str, str]:
    serialized: dict[str, str] = {}
    for key in fieldnames:
        value = row.get(key, "")
        serialized[key] = "" if value is None else str(value)
    return serialized


def build_path_catalog(payload: Any, limit: int = 80) -> list[str]:
    catalog: list[str] = []
    for path, value in iter_candidate_paths(payload):
        if not path:
            continue
        if isinstance(value, (list, dict)):
            kind = "list" if isinstance(value, list) else "object"
            size = len(value)
            catalog.append(f"{path} ({kind}, size={size})")
        if len(catalog) >= limit:
            break
    return catalog


def guess_records_path_with_llm(payload: Any, instruction: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return ""

    client = OpenAI(api_key=api_key)
    path_catalog = build_path_catalog(payload)
    if not path_catalog:
        return ""

    response = client.responses.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You select the best dot-separated records path for exporting JSON. "
                            "Return only the path text. Return ROOT if the top-level payload itself should be exported."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                                f"Instruction: {instruction}\n\n"
                                "Available paths:\n"
                                + "\n".join(path_catalog)
                        ),
                    }
                ],
            },
        ],
    )
    path = (response.output_text or "").strip()
    if path.upper() == "ROOT":
        return ""
    return path


def resolve_records_path_from_request(payload: Any, instruction: str) -> tuple[str, str, bool]:
    cleaned_instruction = instruction.strip()
    if not cleaned_instruction:
        return "", "default-root", False

    llm_path = guess_records_path_with_llm(payload, cleaned_instruction)
    if llm_path:
        return llm_path, "llm", True

    heuristic_path = suggest_records_path(payload, cleaned_instruction)
    if heuristic_path:
        return heuristic_path, "heuristic", True

    return "", "default-root", True


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)


@app.post("/api/convert")
async def convert_json(
        file: UploadFile = File(...),
        records_path: str = Form(default=""),
        instruction: str = Form(default=""),
) -> dict[str, Any]:
    try:
        payload = json.loads(await file.read())
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc.msg}") from exc

    chosen_path = records_path.strip()
    path_source = "manual"
    agent_used = False

    if not chosen_path:
        chosen_path, path_source, agent_used = resolve_records_path_from_request(
            payload, instruction
        )

    try:
        rows, fieldnames = json_to_rows(payload, records_path=chosen_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    csv_text = rows_to_csv_text(rows, fieldnames)
    excel_bytes = rows_to_excel_bytes(rows, fieldnames)
    job_id = str(uuid4())
    base_name = sanitize_filename(file.filename)
    DOWNLOAD_CACHE[job_id] = {
        "base_name": base_name,
        "csv": csv_text.encode("utf-8-sig"),
        "excel": excel_bytes,
    }

    return {
        "job_id": job_id,
        "row_count": len(rows),
        "column_count": len(fieldnames),
        "preview_count": min(len(rows), 200),
        "columns": fieldnames,
        "preview_rows": [serialize_preview_row(row, fieldnames) for row in rows[:200]],
        "csv_download_url": f"/download/{job_id}/csv",
        "excel_download_url": f"/download/{job_id}/xlsx",
        "resolved_records_path": chosen_path,
        "path_source": path_source,
        "agent_used": agent_used,
    }


@app.get("/download/{job_id}/{file_type}")
async def download_file(job_id: str, file_type: str) -> Response:
    job = DOWNLOAD_CACHE.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Download not found or expired.")

    if file_type == "csv":
        filename = f"{job['base_name']}.csv"
        return Response(
            content=job["csv"],
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    if file_type == "xlsx":
        filename = f"{job['base_name']}.xlsx"
        return Response(
            content=job["excel"],
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    raise HTTPException(status_code=400, detail="Unsupported download type.")


if __name__ == "__main__":
    import uvicorn

    # uvicorn.run("app_agent:app", host="127.0.0.1", port=8000, reload=True)
    uvicorn.run("app_agent:app", host="0.0.0.0", port=8000, reload=True)
