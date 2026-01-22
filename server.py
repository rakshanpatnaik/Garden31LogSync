# server.py
import os
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import PlainTextResponse, JSONResponse

from main import run_sync  # reuse your sync logic

app = FastAPI()


@app.get("/graph/webhook")
async def graph_validation(validationToken: str):
    """
    Microsoft Graph calls this GET *once* when you create a subscription.
    You MUST echo back validationToken as plain text.
    """
    return PlainTextResponse(validationToken)


@app.post("/graph/webhook")
async def graph_notifications(request: Request, background_tasks: BackgroundTasks):
    """
    Microsoft Graph sends change notifications here.
    We don't have to inspect them deeply – just trigger the sync.
    """
    body = await request.json()
    # Optional: log notifications for debugging
    print("Received Graph notification:", body)

    # Run sync in the background so we can respond quickly
    background_tasks.add_task(run_sync, None)

    return JSONResponse({"status": "ok"})
