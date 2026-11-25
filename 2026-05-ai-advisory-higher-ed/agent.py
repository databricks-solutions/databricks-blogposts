# Databricks notebook source
"""
Higher Education Advisory Services - LangGraph Agent

This agent orchestrates the full advisory call processing pipeline via
Unity Catalog function tools (for AI analysis) and custom Python tools
(for pipeline operations that write data to tables).

Architecture:
  - UC SQL functions: governed, read-only AI analysis (classify, sentiment, etc.)
  - Custom tools: pipeline operations that persist results (transcribe+save, enrich+save)
  - Both execute SQL via the Databricks SQL Statement API
"""
from typing import Any, Generator, Optional
import json
import os
import re
import time

import mlflow
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk, ChatAgentMessage, ChatAgentResponse, ChatContext,
)

mlflow.langchain.autolog()

# Configuration is read from environment variables so the same agent.py file
# works in any workspace. 02_deploy.py sets these via model_config when logging
# the model; for local interactive runs you can export them in your shell.
LLM_ENDPOINT_NAME = os.environ.get("AGENT_LLM_ENDPOINT", "databricks-claude-3-7-sonnet")
CATALOG = os.environ["AGENT_CATALOG"]
SCHEMA = os.environ["AGENT_SCHEMA"]
FQ = f"{CATALOG}.{SCHEMA}"
WAREHOUSE_ID = os.environ["AGENT_WAREHOUSE_ID"]

llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)

# ---------------------------------------------------------------------------
# Helper: execute SQL via the Statement Execution API (supports DML)
# ---------------------------------------------------------------------------

def _execute_sql(
    sql: str,
    parameters: Optional[list] = None,
    timeout: int = 120,
) -> dict:
    """Execute SQL via the Databricks SDK Statement API.

    Use `parameters` for any user/LLM-supplied values (referenced as :name in `sql`)
    so the API binds them safely instead of string-interpolating them.

    Returns dict with 'rows' or 'error'.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    w = WorkspaceClient()
    stmt = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        parameters=parameters or None,
        wait_timeout="50s",
    )
    deadline = time.time() + timeout
    while stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            return {"error": "Query timed out"}
        time.sleep(5)
        stmt = w.statement_execution.get_statement(stmt.statement_id)

    if stmt.status.state == StatementState.SUCCEEDED:
        rows = []
        if stmt.result and stmt.result.data_array:
            cols = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest else []
            for row in stmt.result.data_array:
                rows.append(dict(zip(cols, row)) if cols else row)
        return {"status": "success", "rows": rows}
    else:
        error_msg = stmt.status.error.message if stmt.status.error else "Unknown error"
        return {"error": error_msg}


def _param(name: str, value, type_text: str = "STRING"):
    """Build a Statement API named parameter (lazy import to keep top-of-file light)."""
    from databricks.sdk.service.sql import StatementParameterListItem
    return StatementParameterListItem(name=name, value=value, type=type_text)


# ---------------------------------------------------------------------------
# Custom Pipeline Tools (write operations the UC functions cannot do)
# ---------------------------------------------------------------------------

@tool
def transcribe_and_save_to_silver(file_path: str) -> str:
    """Transcribe a single audio file using Whisper and save the result to the silver_transcriptions table.

    Use this when a user asks to transcribe a specific audio file or speaker.
    The file_path should be a full Volume path like /Volumes/<catalog>/<schema>/audio_files/Speaker_0001.wav

    Args:
        file_path: Full Volume path to the audio file.

    Returns:
        JSON with transcription result including filename, speaker_id, word_count, and a preview.
    """
    # Check if already transcribed (parameterized)
    check = _execute_sql(
        f"SELECT COUNT(*) AS cnt FROM {FQ}.silver_transcriptions WHERE file_path = :file_path",
        parameters=[_param("file_path", file_path)],
    )
    if check.get("rows") and check["rows"][0].get("cnt", "0") != "0":
        return json.dumps({"status": "already_exists", "message": f"{file_path} is already transcribed in silver."})

    # Transcribe using the UC function (calls Whisper via ai_query)
    result = _execute_sql(
        f"SELECT {FQ}.transcribe_audio(:file_path)",
        parameters=[_param("file_path", file_path)],
        timeout=180,
    )
    if "error" in result:
        return json.dumps({"status": "error", "message": f"Transcription failed: {result['error']}"})

    first_row = result.get("rows", [{}])[0]
    transcript = first_row.get(list(first_row.keys())[0]) if first_row else None
    if not transcript:
        return json.dumps({"status": "error", "message": "No transcription returned from Whisper"})

    # Extract metadata
    filename = file_path.split("/")[-1]
    match = re.search(r"Speaker[_\s]*0*(\d+)", file_path)
    speaker_id = match.group(1) if match else "unknown"
    word_count = len(transcript.split())
    duration_hint = "short" if word_count < 100 else ("medium" if word_count < 500 else "long")

    # Save to silver (parameterized)
    insert_sql = f"""
    INSERT INTO {FQ}.silver_transcriptions
        (filename, file_path, speaker_id, transcription, word_count, duration_hint, transcribed_at)
    VALUES
        (:filename, :file_path, :speaker_id, :transcription,
         :word_count, :duration_hint, current_timestamp())
    """
    save = _execute_sql(
        insert_sql,
        parameters=[
            _param("filename", filename),
            _param("file_path", file_path),
            _param("speaker_id", speaker_id),
            _param("transcription", transcript),
            _param("word_count", str(word_count), type_text="INT"),
            _param("duration_hint", duration_hint),
        ],
    )
    if "error" in save:
        return json.dumps({
            "status": "partial",
            "message": f"Transcribed but save failed: {save['error']}",
            "transcription_preview": transcript[:300],
        })

    return json.dumps({
        "status": "success",
        "filename": filename,
        "speaker_id": speaker_id,
        "word_count": word_count,
        "duration_hint": duration_hint,
        "transcription_preview": transcript[:300],
        "saved_to": "silver_transcriptions",
    })


@tool
def enrich_and_save_to_gold(file_path: str) -> str:
    """Enrich a transcribed call with full AI analysis and save to the gold_enriched_calls table.

    Runs sentiment analysis, topic extraction, call classification, and rubric scoring
    on a transcript that is already in silver, then persists the results to gold.

    Args:
        file_path: The file_path of a record already in silver_transcriptions.

    Returns:
        JSON with enrichment results and save status.
    """
    # Check if already enriched
    check = _execute_sql(
        f"SELECT COUNT(*) AS cnt FROM {FQ}.gold_enriched_calls WHERE file_path = :file_path",
        parameters=[_param("file_path", file_path)],
    )
    if check.get("rows") and check["rows"][0].get("cnt", "0") != "0":
        return json.dumps({"status": "already_exists", "message": f"{file_path} is already enriched in gold."})

    # Get transcription from silver
    silver = _execute_sql(
        f"SELECT transcription, filename, speaker_id, word_count "
        f"FROM {FQ}.silver_transcriptions WHERE file_path = :file_path LIMIT 1",
        parameters=[_param("file_path", file_path)],
    )
    if "error" in silver or not silver.get("rows"):
        return json.dumps({"status": "error", "message": "Transcription not found in silver. Transcribe the file first."})

    row = silver["rows"][0]
    transcription = row.get("transcription", "")
    filename = row.get("filename", "")
    speaker_id = row.get("speaker_id", "")
    word_count = int(row.get("word_count", 0))

    # Run each analysis function with the transcript as a bound parameter.
    analyses = {}
    for fn, key in [
        ("classify_call_category", "category"),
        ("analyze_call_sentiment", "sentiment"),
        ("extract_topics_and_intent", "topics"),
        ("assess_rubric_rag", "rubric"),
    ]:
        r = _execute_sql(
            f"SELECT {FQ}.{fn}(:transcription)",
            parameters=[_param("transcription", transcription)],
            timeout=120,
        )
        if "error" in r:
            analyses[key] = f"error: {r['error']}"
        else:
            val = r["rows"][0].get(list(r["rows"][0].keys())[0]) if r.get("rows") else ""
            analyses[key] = val or ""

    # Parse structured fields for gold table columns
    sentiment_label = "Unknown"
    sentiment_confidence = 0.0
    try:
        s = json.loads(analyses.get("sentiment", "{}"))
        sentiment_label = s.get("sentiment", s.get("label", "Unknown"))
        sentiment_confidence = float(s.get("confidence", 0.0))
    except (json.JSONDecodeError, TypeError, ValueError):
        sentiment_label = analyses.get("sentiment", "Unknown")[:50]

    topics_str = ""
    intent_str = ""
    try:
        t = json.loads(analyses.get("topics", "{}"))
        topics_str = ", ".join(t.get("topics", [])) if isinstance(t.get("topics"), list) else str(t.get("topics", ""))
        intent_str = t.get("primary_intent", t.get("intent", ""))
    except (json.JSONDecodeError, TypeError):
        topics_str = analyses.get("topics", "")[:200]

    category = analyses.get("category", "Other")

    rubric_score = 3
    rubric_assessment = ""
    improvement_areas = ""
    try:
        rb = json.loads(analyses.get("rubric", "{}"))
        rubric_score = int(rb.get("overall_score", rb.get("score", 3)))
        rubric_assessment = json.dumps(rb) if isinstance(rb, dict) else str(rb)
        areas = rb.get("improvement_areas", rb.get("improvements", []))
        improvement_areas = ", ".join(areas) if isinstance(areas, list) else str(areas)
    except (json.JSONDecodeError, TypeError, ValueError):
        rubric_assessment = analyses.get("rubric", "")[:500]

    # Insert into gold (parameterized)
    insert_sql = f"""
    INSERT INTO {FQ}.gold_enriched_calls
        (filename, file_path, speaker_id, transcription,
         sentiment, sentiment_confidence, topics, intent, call_category,
         rubric_score, rubric_assessment, improvement_areas, word_count, enriched_at)
    VALUES
        (:filename, :file_path, :speaker_id, :transcription,
         :sentiment, :sentiment_confidence, :topics, :intent, :call_category,
         :rubric_score, :rubric_assessment, :improvement_areas, :word_count, current_timestamp())
    """
    save = _execute_sql(
        insert_sql,
        parameters=[
            _param("filename", filename),
            _param("file_path", file_path),
            _param("speaker_id", speaker_id),
            _param("transcription", transcription),
            _param("sentiment", sentiment_label),
            _param("sentiment_confidence", str(sentiment_confidence), type_text="DOUBLE"),
            _param("topics", topics_str),
            _param("intent", intent_str),
            _param("call_category", category),
            _param("rubric_score", str(rubric_score), type_text="INT"),
            _param("rubric_assessment", rubric_assessment),
            _param("improvement_areas", improvement_areas),
            _param("word_count", str(word_count), type_text="INT"),
        ],
    )
    if "error" in save:
        return json.dumps({
            "status": "partial",
            "message": f"Enrichment succeeded but save to gold failed: {save['error']}",
            "category": category,
            "sentiment": sentiment_label,
        })

    return json.dumps({
        "status": "success",
        "filename": filename,
        "speaker_id": speaker_id,
        "call_category": category,
        "sentiment": sentiment_label,
        "sentiment_confidence": sentiment_confidence,
        "topics": topics_str,
        "intent": intent_str,
        "rubric_score": rubric_score,
        "improvement_areas": improvement_areas,
        "saved_to": "gold_enriched_calls",
    })


@tool
def check_pipeline_status() -> str:
    """Check the current status of the transcription and enrichment pipeline.

    Returns counts for bronze (audio files), silver (transcriptions), and gold (enriched) tables,
    plus how many are pending at each stage.
    """
    result = _execute_sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {FQ}.bronze_audio_files) AS bronze,
        (SELECT COUNT(*) FROM {FQ}.silver_transcriptions) AS silver,
        (SELECT COUNT(*) FROM {FQ}.gold_enriched_calls) AS gold
    """)
    if "error" in result:
        return json.dumps({"status": "error", "message": result["error"]})

    row = result.get("rows", [{}])[0]
    bronze = int(row.get("bronze", 0))
    silver = int(row.get("silver", 0))
    gold = int(row.get("gold", 0))

    return json.dumps({
        "bronze_audio_files": bronze,
        "silver_transcriptions": silver,
        "gold_enriched_calls": gold,
        "pending_transcription": bronze - silver,
        "pending_enrichment": silver - gold,
        "message": (
            "All files processed through gold."
            if bronze == silver == gold and gold > 0
            else f"{bronze - silver} files need transcription, {silver - gold} need enrichment."
        ),
    })


# ---------------------------------------------------------------------------
# UC Function Tools (read-only AI analysis — governed by Unity Catalog)
# ---------------------------------------------------------------------------

uc_tool_names = [
    f"{CATALOG}.{SCHEMA}.find_audio_file",
    f"{CATALOG}.{SCHEMA}.find_all_audio_files",
    f"{CATALOG}.{SCHEMA}.classify_call_category",
    f"{CATALOG}.{SCHEMA}.analyze_call_sentiment",
    f"{CATALOG}.{SCHEMA}.extract_topics_and_intent",
    f"{CATALOG}.{SCHEMA}.assess_rubric_rag",
    f"{CATALOG}.{SCHEMA}.enrich_single_call",
]

custom_tools = [
    transcribe_and_save_to_silver,
    enrich_and_save_to_gold,
    check_pipeline_status,
]

# Lazy init: UCFunctionToolkit.get_function calls can be slow on first load
_tools_cache = None

def get_tools():
    global _tools_cache
    if _tools_cache is None:
        uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
        _tools_cache = uc_toolkit.tools + custom_tools
    return _tools_cache


# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

system_prompt = """You are an AI-powered advisor quality analyst for a Higher Education call center.

You help administrators and QA managers process, transcribe, and analyze advisory calls
(financial aid, admissions, enrollment, academic advising) at scale.

## Your Tools

### Discovery
1. **find_audio_file(speaker_query)** - Locate a specific speaker's audio file by name or number.
2. **find_all_audio_files()** - List every audio file in the advisory services Volume.

### Transcription & Pipeline
3. **transcribe_and_save_to_silver(file_path)** - Transcribe a single audio file with Whisper and save the result to the silver table. You MUST have the full file_path — use find_audio_file first.
4. **enrich_and_save_to_gold(file_path)** - Run full AI analysis (sentiment, topics, category, rubric) on a silver transcript and save to the gold table. The file must be transcribed first.
5. **check_pipeline_status()** - Show counts for bronze/silver/gold tables and how many are pending.

### Analysis (work on any text — does NOT save to tables)
6. **classify_call_category(transcription)** - Classify into: Financial Aid, Admissions, Enrollment, Academic Advising, Registration, Housing, Billing, Career Services, or Other.
7. **analyze_call_sentiment(transcription)** - Analyze student sentiment. Returns JSON with label and confidence.
8. **extract_topics_and_intent(transcription)** - Extract key topics and primary intent.
9. **assess_rubric_rag(transcription)** - Score advisor performance 1-5 across rubric criteria using RAG.
10. **enrich_single_call(transcription)** - Run ALL analysis at once (sentiment + topics + category + rubric).

## Recommended Workflows

| User Request | Tool Sequence |
|---|---|
| "Transcribe speaker 12" | find_audio_file → transcribe_and_save_to_silver |
| "Full analysis of speaker 5" | find_audio_file → transcribe_and_save_to_silver → enrich_and_save_to_gold |
| "Analyze this transcript" | enrich_single_call (or individual analysis tools) |
| "Pipeline status" | check_pipeline_status |
| "What files do we have?" | find_all_audio_files |

## Guidelines
- Always use find_audio_file first to get the full file_path before transcribing.
- After transcribing, offer to run enrichment to gold.
- Report exact counts and status after pipeline operations.
- For ad-hoc analysis of text the user provides, use the analysis tools directly (they don't save to tables).
- The rubric scores advisors 1-5 across: Greeting, Active Listening, Accurate Information, Empathy, and Resolution.
"""


# ---------------------------------------------------------------------------
# LangGraph Agent
# ---------------------------------------------------------------------------

def create_tool_calling_agent(model, tools, system_prompt=None):
    model = model.bind_tools(tools)

    def should_continue(state):
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "continue"
        if isinstance(last, dict) and last.get("tool_calls"):
            return "continue"
        return "end"

    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    def call_model(state, config):
        return {"messages": [model_runnable.invoke(state, config)]}

    workflow = StateGraph(ChatAgentState)
    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ChatAgentToolNode(tools))
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent", should_continue, {"continue": "tools", "end": END}
    )
    workflow.add_edge("tools", "agent")
    return workflow.compile()


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent):
        self.agent = agent

    def predict(self, messages, context=None, custom_inputs=None):
        request = {"messages": self._convert_messages_to_dict(messages)}
        out_msgs = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for m in node_data.get("messages", []):
                    if isinstance(m, dict):
                        out_msgs.append(ChatAgentMessage(**m))
                    else:
                        role = getattr(m, "type", "assistant")
                        role = "assistant" if role == "ai" else role
                        kwargs = {}
                        if getattr(m, "tool_calls", None):
                            kwargs["tool_calls"] = m.tool_calls
                        out_msgs.append(ChatAgentMessage(
                            role=role,
                            content=getattr(m, "content", str(m)),
                            **kwargs,
                        ))
        return ChatAgentResponse(messages=out_msgs)

    def predict_stream(self, messages, context=None, custom_inputs=None):
        request = {"messages": self._convert_messages_to_dict(messages)}
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for m in node_data.get("messages", []):
                    if isinstance(m, dict):
                        yield ChatAgentChunk(**{"delta": m})
                    else:
                        role = getattr(m, "type", "assistant")
                        role = "assistant" if role == "ai" else role
                        yield ChatAgentChunk(**{"delta": {
                            "role": role,
                            "content": getattr(m, "content", str(m)),
                        }})


# Lazy agent creation on first request
_agent_instance = None

def _get_agent():
    global _agent_instance
    if _agent_instance is None:
        tools = get_tools()
        _agent_instance = create_tool_calling_agent(llm, tools, system_prompt)
    return _agent_instance


class LazyChatAgent(ChatAgent):
    """Wraps the LangGraphChatAgent with lazy initialization."""

    def predict(self, messages, context=None, custom_inputs=None):
        agent = _get_agent()
        return LangGraphChatAgent(agent).predict(messages, context, custom_inputs)

    def predict_stream(self, messages, context=None, custom_inputs=None):
        agent = _get_agent()
        return LangGraphChatAgent(agent).predict_stream(messages, context, custom_inputs)


AGENT = LazyChatAgent()
mlflow.models.set_model(AGENT)
