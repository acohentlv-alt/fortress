"""Natural language query route — LLM integration stub.

This endpoint is a placeholder for future LLM-powered query interpretation.
It accepts a natural language prompt and will eventually translate it into
structured database queries via the query_interpreter module.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/query", tags=["query"])


class NaturalQueryRequest(BaseModel):
    """JSON body for POST /api/query/natural."""
    prompt: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Natural language query prompt",
    )


@router.post("/natural", status_code=501)
async def natural_query(body: NaturalQueryRequest):
    """Accept a natural language query for future LLM processing.

    Currently returns 501 Not Implemented. When the LLM pipeline is
    integrated, this will parse the prompt, generate SQL via the
    query_interpreter, execute searches, and return structured results.
    """
    return JSONResponse(
        status_code=501,
        content={
            "status": "pending_integration",
            "prompt_received": body.prompt,
            "message": (
                "Natural language query engine is not yet integrated. "
                "This endpoint will be connected to the LLM pipeline "
                "in a future release."
            ),
        },
    )
