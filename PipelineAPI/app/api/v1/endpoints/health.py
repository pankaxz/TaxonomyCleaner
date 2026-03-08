from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.dependencies import get_app_settings
from app.core.config import Settings
from app.schemas import HealthResponse

router = APIRouter()


@router.get("/health", summary="Health check", response_model=HealthResponse)
def health_check(settings: Annotated[Settings, Depends(get_app_settings)]) -> HealthResponse:
    return HealthResponse(status="ok", service=settings.PROJECT_NAME)
