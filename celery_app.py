from celery import Celery


app = Celery(
    main="tnr_imgcmprs",
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/0",
)


app.conf.task_queues = {
    "tnr_imgcmprs_queue": {
        "exchange": "compressor",
        "exchange_type": "direct",
        "binding_key": "compressor",
    }
}

app.conf.task_default_queue = "tnr_imgcmprs_queue"
app.conf.task_default_exchange = "compressor" # type: ignore
app.conf.task_default_routing_key = "direct" # type: ignore


from PIL import Image
import os
from pydantic import BaseModel


class Result(BaseModel):
    file_path: str
    old_filesize: int
    new_filesize: int


@app.task(queue="tnr_imgcmprs_queue", name="compress_image")
def compress_image(file_path: str, quality: int = 50):
    img = Image.open(file_path)
    old_filesize = os.path.getsize(file_path)
    img.save(file_path, quality=quality)
    new_filesize = os.path.getsize(file_path)
    result = Result(
        file_path=file_path, old_filesize=old_filesize, new_filesize=new_filesize
    )
    return result.model_dump()
