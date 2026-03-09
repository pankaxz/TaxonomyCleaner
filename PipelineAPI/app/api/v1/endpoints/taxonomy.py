from fastapi import APIRouter

router = APIRouter()


@router.get("/skills")
def list_skills():
    from src.core.Taxonomy import TaxonomyManager
    return TaxonomyManager.get_all_canonicals()