import os

from celery import Celery, signals
from PIL import Image
from pydantic import BaseModel

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
app.conf.task_default_exchange = "compressor"  # type: ignore
app.conf.task_default_routing_key = "direct"  # type: ignore
app.conf.result_extended = True


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


@app.task(queue="tnr_imgcmprs_queue", name="delete_task_result")
def delete_task_result(task_id):
    result = app.AsyncResult(task_id)
    file_path, *_ = result.args
    if file_path and os.path.isfile(file_path):
        os.remove(file_path)
    result.forget()


@app.task(queue="tnr_imgcmprs_queue", name="schedule_deletion")
def schedule_deletion(task_id, countdown: int):
    delete_task_result.apply_async((task_id,), countdown=countdown)  # type: ignore


@signals.task_success.connect(sender=compress_image)
def task_success_handler(sender, result, **kwargs):
    task_id = sender.request.correlation_id
    schedule_deletion.delay(task_id, 60 * 30)  # type: ignore


@signals.task_failure.connect(sender=compress_image)
def task_failure_handler(sender, task_id, **kwargs):
    task_id = task_id
    schedule_deletion.delay(task_id, 0)  # type: ignore


@signals.task_revoked.connect(sender=compress_image)
def task_revoked_handler(sender, request, **kwargs):
    task_id = request.id
    schedule_deletion.delay(task_id, 0)  # type: ignore
