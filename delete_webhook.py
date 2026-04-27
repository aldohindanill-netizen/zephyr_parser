#!/usr/bin/env python3
"""One-shot script to delete the active Telegram webhook.

Run this once if the bot fails with:
  TelegramConflictError: can't use getUpdates method while webhook is active

Usage:
    TELEGRAM_BOT_TOKEN=<token> python delete_webhook.py
    # or pass it as a CLI arg:
    python delete_webhook.py <token>
"""

import sys
import urllib.request
import urllib.error
import os
import json


def delete_webhook(token: str) -> None:
    url = f"https://api.telegram.org/bot{token}/deleteWebhook?drop_pending_updates=true"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = json.loads(exc.read().decode())

    if body.get("ok"):
        print("Webhook deleted successfully. Bot can now use polling (getUpdates).")
    else:
        print(f"Error: {body}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    token = (
        sys.argv[1] if len(sys.argv) > 1 else os.environ.get("TELEGRAM_BOT_TOKEN", "")
    )
    if not token:
        print(
            "Usage: TELEGRAM_BOT_TOKEN=<token> python delete_webhook.py",
            file=sys.stderr,
        )
        sys.exit(1)
    delete_webhook(token)
