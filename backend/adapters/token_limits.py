"""Output allowances include provider reasoning as well as visible text."""
SCORING_OUTPUT_TOKENS = 16_384
RESEARCH_OUTPUT_TOKENS = 65_536
MAX_OUTPUT_TOKENS = 131_072
CONNECTION_OUTPUT_TOKENS = 4_096
SEARCH_OUTPUT_TOKENS = 32_768


def output_tokens(private=False, requested=None):
    default = SCORING_OUTPUT_TOKENS if private else RESEARCH_OUTPUT_TOKENS
    return min(MAX_OUTPUT_TOKENS, max(default, int(requested or default)))


def truncated_output_error():
    return {"code": "model_output_truncated", "message": "模型输出达到 token 上限而被截断，未接受为完整结果。",
            "http_status": None, "provider_error_code": None}
