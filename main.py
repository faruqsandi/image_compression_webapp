import os
from typing import Optional, Union

import aiofiles
import filetype
from celery.exceptions import CeleryError
from fastapi import FastAPI, UploadFile
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from celery_app import app as celery_app
from celery_app import compress_image

app = FastAPI()

TEMP_DIR = "uploads"
os.makedirs(TEMP_DIR, exist_ok=True)


class ResponseSchema(BaseModel):
    data: Union[dict, list[dict]]
    message: str
    code: int = 200


class ErrorResponseSchema(BaseModel):
    error: str = "An error occurred"
    message: str
    code: int


@app.get("/")
async def index():
    return RedirectResponse(url="/docs")


class TaskOut(BaseModel):
    id: str
    status: str
    result: Optional[Union[dict, list, str]]


@app.post("/upload")
async def upload(file: UploadFile):
    temporary_file_path = f"{TEMP_DIR}/{file.filename}"
    async with aiofiles.open(temporary_file_path, "wb") as buffer:
        data = await file.read()
        await buffer.write(data)

    if not filetype.helpers.is_image(temporary_file_path):
        return ErrorResponseSchema(message="File is not an image", code=400)

    task = compress_image.delay(temporary_file_path)  # type: ignore
    return ResponseSchema(
        data={"task_id": task.id}, message="File uploaded successfully"
    )


@app.get("/task/{task_id}")
async def get_task_result(task_id: str):
    try:
        r = celery_app.AsyncResult(task_id)

        result = r.result
        if isinstance(result, CeleryError):
            result = repr(result)

        task_response = TaskOut(id=r.task_id, status=r.status, result=result).dict()
        return ResponseSchema(
            data=task_response,
            message="Task status retrieved successfully",
        )
    except Exception:
        return ErrorResponseSchema(
            code=500, message="Something went wrong while getting Task status"
        )
