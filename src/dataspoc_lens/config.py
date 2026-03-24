"""Configuration models for DataSpoc Lens."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field

DATASPOC_LENS_HOME = Path.home() / ".dataspoc-lens"
CONFIG_FILE = DATASPOC_LENS_HOME / "config.yaml"
TRANSFORMS_DIR = DATASPOC_LENS_HOME / "transforms"
HISTORY_FILE = DATASPOC_LENS_HOME / "history"


class LLMConfig(BaseModel):
    """LLM configuration for AI features."""

    provider: str = Field("ollama", description="LLM provider: ollama (local), anthropic, openai")
    model: str = Field("duckdb-nsql:7b", description="Model name (e.g. duckdb-nsql:7b, qwen2.5-coder:1.5b)")
    api_key: str = Field("", description="API key (not needed for ollama)")


class LensConfig(BaseModel):
    """Main configuration for DataSpoc Lens."""

    buckets: list[str] = Field(default_factory=list, description="Registered bucket URIs")
    llm: LLMConfig = Field(default_factory=LLMConfig, description="LLM settings for AI features")


def load_config() -> LensConfig:
    """Load configuration from config.yaml."""
    if not CONFIG_FILE.exists():
        return LensConfig()

    with open(CONFIG_FILE) as f:
        data = yaml.safe_load(f) or {}

    return LensConfig(**data)


def save_config(config: LensConfig) -> Path:
    """Save configuration to config.yaml."""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(CONFIG_FILE, "w") as f:
        yaml.dump(
            config.model_dump(),
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    return CONFIG_FILE
