from fastapi import FastAPI

from routers.weather import router as weather_router

app = FastAPI()

app.include_router(weather_router)
