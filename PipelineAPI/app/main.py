import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi import APIRouter, Depends
from app.api.v1.api import api_router
from app.core.config import get_settings
from app.core.logging import configure_logging

router = APIRouter()

def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging("DEBUG" if settings.DEBUG else "INFO")

    # Make DataFactory modules importable (TaxonomyManager, SkillExtractor, etc.)
    if settings.DATAFACTORY_ROOT not in sys.path:
        sys.path.insert(0, settings.DATAFACTORY_ROOT)

    application = FastAPI(
        title=settings.PROJECT_NAME,
        debug=settings.DEBUG,
    )

    if settings.CORS_ORIGINS:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=settings.CORS_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    application.include_router(api_router, prefix=settings.API_V1_STR)
    return application


app = create_app()

@app.get("/")
def home():
    return {"message": "Hello World"}

from src.core.Taxonomy import TaxonomyManager
from main import JobDataProcessingPipeline

@app.get("/taxonomy/skills")
def list_skills():
  return TaxonomyManager.get_all_canonicals()

