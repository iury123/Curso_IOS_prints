from fastapi import FastAPI

app = FastAPI(
    title="FastAPI Project 1",
    description="This is a sample FastAPI project.",
    version="1.0.0",
)

@app.get("/")
async def home():
    return {"message": "Welcome to FastAPI Project 1!"}

