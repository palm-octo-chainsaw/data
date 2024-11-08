from fastapi import APIRouter

router = APIRouter(
    prefix='/weather',
    tags=['Weather'],
)


@router.get('/')
def weather():
    return {'status': 'success'}
