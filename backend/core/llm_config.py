"""Configuración del modelo base. La API Key llega desde el request del usuario."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.runnables import RunnableConfig

LLMProvider = Literal["openai", "anthropic", "gemini"]

DEFAULT_MODELS: dict[str, str] = {
    "openai": "gpt-4o-mini",
    "anthropic": "claude-sonnet-4-5",
    "gemini": "gemini-2.5-flash",
}


class LLMConfigError(ValueError):
    """Error de configuración del LLM (key ausente, proveedor no soportado...)."""


@dataclass(frozen=True)
class LLMSettings:
    provider: LLMProvider
    api_key: str
    model: str | None = None
    temperature: float = 0.0

    def __repr__(self) -> str:  # nunca exponer la key en logs
        return f"LLMSettings(provider={self.provider!r}, model={self.model!r})"


class LLMFactory:
    """Factory (Open/Closed): añadir un proveedor = añadir un método _build_*."""

    @staticmethod
    def create(settings: LLMSettings) -> BaseChatModel:
        if not settings.api_key or not settings.api_key.strip():
            raise LLMConfigError("La API Key del LLM es obligatoria.")
        builder = getattr(LLMFactory, f"_build_{settings.provider}", None)
        if builder is None:
            raise LLMConfigError(f"Proveedor LLM no soportado: {settings.provider}")
        model = settings.model or DEFAULT_MODELS[settings.provider]
        return builder(settings.api_key.strip(), model, settings.temperature)

    @staticmethod
    def _build_openai(api_key: str, model: str, temperature: float) -> BaseChatModel:
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(api_key=api_key, model=model, temperature=temperature, timeout=300, max_retries=2)

    @staticmethod
    def _build_anthropic(api_key: str, model: str, temperature: float) -> BaseChatModel:
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(api_key=api_key, model=model, temperature=temperature,
                             timeout=300, max_retries=2, max_tokens=16000)

    @staticmethod
    def _build_gemini(api_key: str, model: str, temperature: float) -> BaseChatModel:
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(google_api_key=api_key, model=model, temperature=temperature)


def get_llm_from_config(config: RunnableConfig) -> BaseChatModel:
    """Construye el LLM a partir de `config['configurable']` (inyectado por request)."""
    conf = (config or {}).get("configurable", {})
    settings = LLMSettings(
        provider=conf.get("llm_provider", "openai"),
        api_key=conf.get("llm_api_key", ""),
        model=conf.get("llm_model") or None,
    )
    return LLMFactory.create(settings)
