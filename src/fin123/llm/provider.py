"""Minimal LLM provider for fin123 AI workbench.

Supports Anthropic (Claude) and OpenAI (GPT) via direct HTTP calls.
No SDK dependency required — uses httpx for HTTP.

Configuration via environment variables:

    FIN123_LLM_PROVIDER   "anthropic" or "openai"  (default: "anthropic")
    ANTHROPIC_API_KEY      required if provider=anthropic
    OPENAI_API_KEY         required if provider=openai
    FIN123_LLM_MODEL       model override (default: provider-specific)

All methods return structured dicts, never raw provider payloads.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

_TIMEOUT = 60  # seconds

# ── Provider defaults ──

_DEFAULTS = {
    "anthropic": {
        "url": "https://api.anthropic.com/v1/messages",
        "model": "claude-sonnet-4-20250514",
        "key_env": "ANTHROPIC_API_KEY",
    },
    "openai": {
        "url": "https://api.openai.com/v1/chat/completions",
        "model": "gpt-4o",
        "key_env": "OPENAI_API_KEY",
    },
}

# ── Prompt templates ──

_EXPLAIN_FORMULA_PROMPT = """\
You are a financial modeling assistant for the fin123 workbook engine.

Explain what the following formula does in plain language.
Focus on economic meaning and computational mechanics.
Be concise (2-4 sentences).
Do NOT suggest modifications.

Cell: {ref}
Formula: {formula}
Current display value: {display}
{context}"""

_EXPLAIN_OUTPUT_PROMPT = """\
You are a financial modeling assistant for the fin123 workbook engine.

Explain what this output scalar represents in plain language.
Focus on economic meaning, how it is derived, and why it matters.
Be concise (2-4 sentences).
Do NOT suggest modifications.

Output name: {name}
Current value: {value}
{context}"""

_PLUGIN_REQUIREMENTS = """\
- Import register_scalar from fin123.functions.registry
- Define a PLUGIN_META dict with at least: version (int), deterministic (bool)
- Define one or more pure, deterministic Python functions (no side effects)
- Define a register() function that registers each function via register_scalar("name")(fn) and returns {{"name": "plugin_name", "version": PLUGIN_META["version"]}}
- Do NOT import os, sys, subprocess, requests, random, or any forbidden module
- Do NOT use eval(), exec(), or __import__()
- Functions should accept and return simple types (float, int, str)
- Include brief docstrings"""

_DRAFT_ADDIN_PROMPT = """\
You are a code generator for the fin123 workbook engine.
Generate a Python plugin file that implements the following:

{description}

Requirements:
""" + _PLUGIN_REQUIREMENTS + """

Return ONLY the Python code, no markdown fences, no explanation."""

_REVISE_ADDIN_PROMPT = """\
You are a code generator for the fin123 workbook engine.
You are revising an existing plugin. Here is the current code:

```python
{existing_code}
```

The original task was: {original_prompt}

Apply this revision: {instruction}

Requirements:
""" + _PLUGIN_REQUIREMENTS + """

Return ONLY the revised Python code, no markdown fences, no explanation.
Preserve existing functionality unless the revision explicitly changes it."""


class LLMProviderError(Exception):
    """Raised when the LLM provider call fails."""

    def __init__(self, message: str, provider: str | None = None) -> None:
        self.provider = provider
        super().__init__(message)


def get_config() -> dict[str, Any]:
    """Load LLM provider configuration from environment.

    Returns dict with: provider, model, api_key (masked), configured (bool).
    """
    provider = os.environ.get("FIN123_LLM_PROVIDER", "anthropic").lower()
    if provider not in _DEFAULTS:
        return {"configured": False, "error": f"Unknown provider: {provider}"}

    defaults = _DEFAULTS[provider]
    model = os.environ.get("FIN123_LLM_MODEL", defaults["model"])
    api_key = os.environ.get(defaults["key_env"], "")

    return {
        "configured": bool(api_key),
        "provider": provider,
        "model": model,
        "key_env": defaults["key_env"],
        "has_key": bool(api_key),
    }


def _get_client():
    """Get httpx client, raising clear error if not installed."""
    try:
        import httpx
    except ImportError:
        raise LLMProviderError(
            "httpx is required for LLM integration. Install with: pip install httpx"
        )
    return httpx


def _call_provider(prompt: str, system: str = "") -> dict[str, Any]:
    """Call the configured LLM provider and return structured result.

    Returns:
        {"ok": True, "content": str, "provider": str, "model": str}
        or {"ok": False, "error": str, "provider": str}
    """
    config = get_config()
    if not config.get("configured"):
        key_env = config.get("key_env", "ANTHROPIC_API_KEY")
        return {
            "ok": False,
            "error": f"LLM provider not configured. Set {key_env} environment variable.",
            "provider": config.get("provider", "unknown"),
        }

    provider = config["provider"]
    model = config["model"]
    defaults = _DEFAULTS[provider]
    api_key = os.environ[defaults["key_env"]]

    httpx = _get_client()

    try:
        if provider == "anthropic":
            resp = httpx.post(
                defaults["url"],
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 2048,
                    "system": system or "You are a financial modeling assistant.",
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=_TIMEOUT,
            )
        elif provider == "openai":
            messages = []
            if system:
                messages.append({"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})
            resp = httpx.post(
                defaults["url"],
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "max_tokens": 2048,
                    "messages": messages,
                },
                timeout=_TIMEOUT,
            )
        else:
            return {"ok": False, "error": f"Unsupported provider: {provider}", "provider": provider}

        if resp.status_code != 200:
            error_text = resp.text[:500]
            log.warning("LLM provider error (%s): %s %s", provider, resp.status_code, error_text)
            return {
                "ok": False,
                "error": f"Provider returned {resp.status_code}: {error_text}",
                "provider": provider,
            }

        data = resp.json()

        # Extract content from provider-specific response shape
        if provider == "anthropic":
            content = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    content += block.get("text", "")
        elif provider == "openai":
            choices = data.get("choices", [])
            content = choices[0]["message"]["content"] if choices else ""
        else:
            content = ""

        return {
            "ok": True,
            "content": content.strip(),
            "provider": provider,
            "model": data.get("model", model),
        }

    except Exception as exc:
        log.warning("LLM provider call failed: %s", exc)
        return {
            "ok": False,
            "error": f"Provider call failed: {exc}",
            "provider": provider,
        }


def explain_formula(
    ref: str,
    formula: str,
    display: str = "",
    context: str = "",
) -> dict[str, Any]:
    """Ask the LLM to explain a formula.

    Non-mutating. Returns structured explanation.
    """
    prompt = _EXPLAIN_FORMULA_PROMPT.format(
        ref=ref,
        formula=formula,
        display=display,
        context=f"Context: {context}" if context else "",
    )
    result = _call_provider(prompt)
    result["type"] = "formula_explanation"
    result["ref"] = ref
    return result


def explain_output(
    name: str,
    value: Any,
    context: str = "",
) -> dict[str, Any]:
    """Ask the LLM to explain a scalar output.

    Non-mutating. Returns structured explanation.
    """
    prompt = _EXPLAIN_OUTPUT_PROMPT.format(
        name=name,
        value=value,
        context=f"Context: {context}" if context else "",
    )
    result = _call_provider(prompt)
    result["type"] = "output_explanation"
    result["name"] = name
    return result


def draft_addin(description: str) -> dict[str, Any]:
    """Ask the LLM to generate plugin code for a scalar add-in.

    Returns the generated code and metadata. Does NOT save or validate.
    """
    prompt = _DRAFT_ADDIN_PROMPT.format(description=description)
    result = _call_provider(
        prompt,
        system="You are a Python code generator for financial modeling plugins. Return only valid Python code.",
    )

    if result.get("ok") and result.get("content"):
        code = result["content"]
        # Strip markdown fences if present
        if code.startswith("```"):
            lines = code.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            code = "\n".join(lines)
        result["code"] = code
        result["prompt_hash"] = hashlib.sha256(description.encode()).hexdigest()[:16]

    result["type"] = "addin_draft"
    return result


def revise_addin(
    existing_code: str,
    instruction: str,
    original_prompt: str = "",
) -> dict[str, Any]:
    """Ask the LLM to revise existing plugin code.

    Returns the revised code and metadata. Does NOT save or validate.
    """
    prompt = _REVISE_ADDIN_PROMPT.format(
        existing_code=existing_code,
        instruction=instruction,
        original_prompt=original_prompt or "(not available)",
    )
    result = _call_provider(
        prompt,
        system="You are a Python code generator for financial modeling plugins. Return only valid Python code.",
    )

    if result.get("ok") and result.get("content"):
        code = result["content"]
        if code.startswith("```"):
            lines = code.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            code = "\n".join(lines)
        result["code"] = code
        combined = (original_prompt + "\n" + instruction).encode()
        result["prompt_hash"] = hashlib.sha256(combined).hexdigest()[:16]

    result["type"] = "addin_revision"
    return result
