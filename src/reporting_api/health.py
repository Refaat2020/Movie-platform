import os
from django.http import JsonResponse
from django.db import connections
from django.core.cache import cache

def healthcheck(request):
    status = {"status": "ok", "checks": {}}

    # =========================
    # Database
    # =========================
    if os.getenv("HEALTHCHECK_DB", "True") == "True":
        try:
            db_conn = connections["default"]
            db_conn.cursor()
            status["checks"]["database"] = "ok"
        except Exception as e:
            status["checks"]["database"] = "error"
            status["status"] = "error"

    # =========================
    # Redis / Cache
    # =========================
    if os.getenv("HEALTHCHECK_REDIS", "True") == "True":
        try:
            cache.set("healthcheck", "ok", timeout=1)
            status["checks"]["redis"] = "ok"
        except Exception:
            status["checks"]["redis"] = "error"
            status["status"] = "error"

    return JsonResponse(status)