import os
import uuid
from typing import Optional, Union

import aiofiles
import filetype
from celery.exceptions import CeleryError
from fastapi import FastAPI, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
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
    random_uuid = uuid.uuid4().hex

    temporary_file_path = f"{TEMP_DIR}/{random_uuid}_{file.filename}"
    os.makedirs(os.path.dirname(temporary_file_path), exist_ok=True)
    async with aiofiles.open(temporary_file_path, "wb") as buffer:
        data = await file.read()
        await buffer.write(data)

    if not filetype.helpers.is_image(temporary_file_path):
        return ErrorResponseSchema(message="File is not an image", code=400)

    task = compress_image.delay(temporary_file_path, 50)  # type: ignore
    return ResponseSchema(
        data={"task_id": task.id}, message="File uploaded successfully"
    )


@app.get("/download/{task_id}")
def download(task_id: str):
    task = celery_app.AsyncResult(task_id)
    if task.status == "SUCCESS":
        result = task.result
        if result is None:
            return ErrorResponseSchema(
                message="File is already deleted, please try to upload again", code=500
            )

        file_path = result["file_path"]

        if not os.path.isfile(file_path):
            return ErrorResponseSchema(
                message="File is not exist, please try to upload again.", code=400
            )

        kind = filetype.guess(file_path)
        media_type = "application/octet-stream"
        if kind:
            media_type = kind.mime
        file_name = os.path.basename(file_path)
        file_name = file_name.split("_", 1)[1]
        return FileResponse(
            file_path, media_type=media_type, filename=os.path.basename(file_path)
        )
    return ErrorResponseSchema(
        message="File is not ready for download yet. Please try again later", code=400
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
