from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlmodel import Session, select, func
from pydantic import BaseModel, Field
from core.db import AccountModel, get_session
from typing import Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import io, csv, json, logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/accounts", tags=["accounts"])


class AccountCreate(BaseModel):
    platform: str
    email: str
    password: str
    status: str = "registered"
    token: str = ""
    cashier_url: str = ""


class AccountUpdate(BaseModel):
    status: Optional[str] = None
    token: Optional[str] = None
    cashier_url: Optional[str] = None


class ImportRequest(BaseModel):
    platform: str
    lines: list[str]


class BatchDeleteRequest(BaseModel):
    ids: list[int]


class DeleteAllAccountsRequest(BaseModel):
    platform: str
    status: Optional[str] = None
    email: Optional[str] = None


class BatchChatGPTCpaUploadRequest(BaseModel):
    ids: list[int] = Field(default_factory=list)
    status: Optional[str] = None
    email: Optional[str] = None
    api_url: Optional[str] = None
    api_key: Optional[str] = None


class BatchChatGPT401CheckRequest(BaseModel):
    ids: list[int] = Field(default_factory=list)
    status: Optional[str] = None
    email: Optional[str] = None
    workers: Optional[int] = None


class BatchChatGPT401DeleteRequest(BaseModel):
    ids: list[int] = Field(default_factory=list)
    status: Optional[str] = None
    email: Optional[str] = None


@router.get("")
def list_accounts(
    platform: Optional[str] = None,
    status: Optional[str] = None,
    email: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    session: Session = Depends(get_session),
):
    q = select(AccountModel)
    if platform:
        q = q.where(AccountModel.platform == platform)
    if status:
        q = q.where(AccountModel.status == status)
    if email:
        q = q.where(AccountModel.email.contains(email))
    total = len(session.exec(q).all())
    items = session.exec(q.offset((page - 1) * page_size).limit(page_size)).all()
    return {"total": total, "page": page, "items": items}


@router.post("")
def create_account(body: AccountCreate, session: Session = Depends(get_session)):
    acc = AccountModel(
        platform=body.platform,
        email=body.email,
        password=body.password,
        status=body.status,
        token=body.token,
        cashier_url=body.cashier_url,
    )
    session.add(acc)
    session.commit()
    session.refresh(acc)
    return acc


@router.get("/stats")
def get_stats(session: Session = Depends(get_session)):
    """统计各平台账号数量和状态分布"""
    accounts = session.exec(select(AccountModel)).all()
    platforms: dict = {}
    statuses: dict = {}
    for acc in accounts:
        platforms[acc.platform] = platforms.get(acc.platform, 0) + 1
        statuses[acc.status] = statuses.get(acc.status, 0) + 1
    return {"total": len(accounts), "by_platform": platforms, "by_status": statuses}


@router.get("/export")
def export_accounts(
    platform: Optional[str] = None,
    status: Optional[str] = None,
    session: Session = Depends(get_session),
):
    q = select(AccountModel)
    if platform:
        q = q.where(AccountModel.platform == platform)
    if status:
        q = q.where(AccountModel.status == status)
    accounts = session.exec(q).all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["platform", "email", "password", "user_id", "region",
                     "status", "cashier_url", "created_at"])
    for acc in accounts:
        writer.writerow([acc.platform, acc.email, acc.password, acc.user_id,
                         acc.region, acc.status, acc.cashier_url,
                         acc.created_at.strftime("%Y-%m-%d %H:%M:%S")])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=accounts.csv"}
    )


@router.post("/import")
def import_accounts(
    body: ImportRequest,
    session: Session = Depends(get_session),
):
    """批量导入，每行格式: email password [extra]"""
    created = 0
    for line in body.lines:
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        email, password = parts[0], parts[1]
        extra = parts[2] if len(parts) > 2 else ""
        if extra:
            try:
                json.loads(extra)
            except (json.JSONDecodeError, ValueError):
                extra = "{}"
        else:
            extra = "{}"
        acc = AccountModel(platform=body.platform, email=email,
                           password=password, extra_json=extra)
        session.add(acc)
        created += 1
    session.commit()
    return {"created": created}


@router.post("/batch-delete")
def batch_delete_accounts(
    body: BatchDeleteRequest,
    session: Session = Depends(get_session)
):
    """批量删除账号"""
    if not body.ids:
        raise HTTPException(400, "账号 ID 列表不能为空")
    
    if len(body.ids) > 1000:
        raise HTTPException(400, "单次最多删除 1000 个账号")
    
    deleted_count = 0
    not_found_ids = []
    
    try:
        for account_id in body.ids:
            acc = session.get(AccountModel, account_id)
            if acc:
                session.delete(acc)
                deleted_count += 1
            else:
                not_found_ids.append(account_id)
        
        session.commit()
        logger.info(f"批量删除成功: {deleted_count} 个账号")
        
        return {
            "deleted": deleted_count,
            "not_found": not_found_ids,
            "total_requested": len(body.ids)
        }
    except Exception as e:
        session.rollback()
        logger.exception("批量删除失败")
        raise HTTPException(500, f"批量删除失败: {str(e)}")


@router.post("/delete-all")
def delete_all_accounts(
    body: DeleteAllAccountsRequest,
    session: Session = Depends(get_session),
):
    """删除指定平台下、符合当前筛选条件的全部账号。"""
    platform = (body.platform or "").strip()
    if not platform:
        raise HTTPException(400, "平台不能为空")

    q = select(AccountModel).where(AccountModel.platform == platform)
    if body.status:
        q = q.where(AccountModel.status == body.status)
    if body.email:
        q = q.where(AccountModel.email.contains(body.email))

    rows = session.exec(q).all()
    if not rows:
        raise HTTPException(404, "未找到可删除的账号")

    deleted_count = 0
    try:
        for acc in rows:
            session.delete(acc)
            deleted_count += 1

        session.commit()
        logger.info(f"全部删除成功: platform={platform}, deleted={deleted_count}")
        return {
            "deleted": deleted_count,
            "platform": platform,
            "status": body.status or "",
            "email": body.email or "",
        }
    except Exception as e:
        session.rollback()
        logger.exception("全部删除失败")
        raise HTTPException(500, f"全部删除失败: {str(e)}")


@router.post("/chatgpt/upload-cpa")
def batch_upload_chatgpt_cpa(
    body: BatchChatGPTCpaUploadRequest,
    session: Session = Depends(get_session),
):
    """批量上传 ChatGPT 账号到 CPA，支持按 ID 或当前筛选条件上传。"""
    requested_ids: list[int] = []
    rows: list[AccountModel | None]

    if body.ids:
        seen: set[int] = set()
        for raw_id in body.ids:
            try:
                account_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if account_id <= 0 or account_id in seen:
                continue
            seen.add(account_id)
            requested_ids.append(account_id)

        if not requested_ids:
            raise HTTPException(400, "有效账号 ID 列表不能为空")
        if len(requested_ids) > 2000:
            raise HTTPException(400, "单次最多上传 2000 个账号")

        matched_rows: list[AccountModel] = []
        for start in range(0, len(requested_ids), 500):
            chunk = requested_ids[start:start + 500]
            matched_rows.extend(
                session.exec(
                    select(AccountModel).where(AccountModel.id.in_(chunk))
                ).all()
            )
        row_map = {
            row.id: row
            for row in matched_rows
            if row.id is not None and row.platform == "chatgpt"
        }
        rows = [row_map.get(account_id) for account_id in requested_ids]
    else:
        q = select(AccountModel).where(AccountModel.platform == "chatgpt")
        if body.status:
            q = q.where(AccountModel.status == body.status)
        if body.email:
            q = q.where(AccountModel.email.contains(body.email))
        matched_rows = session.exec(q.order_by(AccountModel.created_at.desc())).all()
        if not matched_rows:
            raise HTTPException(404, "未找到可上传的 ChatGPT 账号")
        if len(matched_rows) > 2000:
            raise HTTPException(400, "单次最多上传 2000 个账号，请先缩小筛选范围")
        rows = list(matched_rows)

    from core.config_store import config_store

    api_url = (body.api_url or config_store.get("cpa_api_url", "")).strip()
    api_key = (body.api_key or "").strip() or None
    if not api_url:
        raise HTTPException(400, "CPA API URL 未配置，请先在设置页填写")

    from services.chatgpt_sync import upload_account_model_to_cpa

    summary = {
        "total": len(rows),
        "success": 0,
        "failed": 0,
        "items": [],
    }

    for index, row in enumerate(rows):
        if row is None:
            missing_id = requested_ids[index] if index < len(requested_ids) else None
            summary["failed"] += 1
            summary["items"].append({
                "id": missing_id,
                "email": "",
                "ok": False,
                "msg": "账号不存在或不是 ChatGPT",
            })
            continue

        ok, msg = upload_account_model_to_cpa(
            row,
            session=session,
            api_url=api_url,
            api_key=api_key,
            commit=False,
        )
        if ok:
            summary["success"] += 1
        else:
            summary["failed"] += 1
        summary["items"].append({
            "id": row.id,
            "email": row.email,
            "ok": ok,
            "msg": msg,
        })

    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logger.exception("批量上传 ChatGPT 账号到 CPA 后写入状态失败")
        raise HTTPException(500, f"批量上传 ChatGPT 账号到 CPA 后写入状态失败: {str(e)}")

    return summary


def _check_chatgpt_access_token(email: str, access_token: str) -> dict:
    if not access_token:
        return {
            "email": email,
            "ok": False,
            "invalid_401": False,
            "status_code": None,
            "msg": "缺少 access_token，无法检测",
        }

    from curl_cffi import requests as cffi_requests

    try:
        response = cffi_requests.get(
            "https://chatgpt.com/backend-api/me",
            headers={
                "authorization": f"Bearer {access_token}",
                "accept": "application/json",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
            timeout=30,
            impersonate="chrome120",
        )
        status_code = response.status_code
        if status_code == 200:
            return {
                "email": email,
                "ok": True,
                "invalid_401": False,
                "status_code": status_code,
                "msg": "Token 有效",
            }
        if status_code == 401:
            return {
                "email": email,
                "ok": False,
                "invalid_401": True,
                "status_code": status_code,
                "msg": "401 无效或已过期",
            }
        if status_code == 403:
            return {
                "email": email,
                "ok": False,
                "invalid_401": False,
                "status_code": status_code,
                "msg": "403 拒绝访问，账号可能受限",
            }
        if status_code == 429:
            return {
                "email": email,
                "ok": False,
                "invalid_401": False,
                "status_code": status_code,
                "msg": "429 请求过多，稍后重试",
            }

        detail = ""
        try:
            detail = (response.text or "").strip()
        except Exception:
            detail = ""
        detail = detail[:200]
        return {
            "email": email,
            "ok": False,
            "invalid_401": False,
            "status_code": status_code,
            "msg": f"检测失败: HTTP {status_code}{f' - {detail}' if detail else ''}",
        }
    except Exception as e:
        return {
            "email": email,
            "ok": False,
            "invalid_401": False,
            "status_code": None,
            "msg": f"检测异常: {str(e)}",
        }


def _is_chatgpt_invalid_401(acc: AccountModel) -> bool:
    try:
        extra = acc.get_extra()
    except Exception:
        extra = {}
    return bool(extra.get("last_401_check_invalid_401"))


@router.post("/chatgpt/check-401")
def batch_check_chatgpt_401(
    body: BatchChatGPT401CheckRequest,
    session: Session = Depends(get_session),
):
    """批量检测 ChatGPT 账号 access_token 是否出现 401，并将命中的账号标记为 invalid。"""
    requested_ids: list[int] = []
    rows: list[AccountModel | None]

    if body.ids:
        seen: set[int] = set()
        for raw_id in body.ids:
            try:
                account_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if account_id <= 0 or account_id in seen:
                continue
            seen.add(account_id)
            requested_ids.append(account_id)

        if not requested_ids:
            raise HTTPException(400, "有效账号 ID 列表不能为空")
        if len(requested_ids) > 2000:
            raise HTTPException(400, "单次最多检测 2000 个账号")

        matched_rows: list[AccountModel] = []
        for start in range(0, len(requested_ids), 500):
            chunk = requested_ids[start:start + 500]
            matched_rows.extend(
                session.exec(
                    select(AccountModel).where(AccountModel.id.in_(chunk))
                ).all()
            )
        row_map = {
            row.id: row
            for row in matched_rows
            if row.id is not None and row.platform == "chatgpt"
        }
        rows = [row_map.get(account_id) for account_id in requested_ids]
    else:
        q = select(AccountModel).where(AccountModel.platform == "chatgpt")
        if body.status:
            q = q.where(AccountModel.status == body.status)
        if body.email:
            q = q.where(AccountModel.email.contains(body.email))
        matched_rows = session.exec(q.order_by(AccountModel.created_at.desc())).all()
        if not matched_rows:
            raise HTTPException(404, "未找到可检测的 ChatGPT 账号")
        if len(matched_rows) > 2000:
            raise HTTPException(400, "单次最多检测 2000 个账号，请先缩小筛选范围")
        rows = list(matched_rows)

    tasks: list[dict] = []
    summary = {
        "total": len(rows),
        "valid": 0,
        "invalid_401": 0,
        "failed": 0,
        "updated": 0,
        "workers": 0,
        "items": [],
    }

    for index, row in enumerate(rows):
        if row is None:
            missing_id = requested_ids[index] if index < len(requested_ids) else None
            summary["failed"] += 1
            summary["items"].append({
                "id": missing_id,
                "email": "",
                "ok": False,
                "invalid_401": False,
                "status_code": None,
                "msg": "账号不存在或不是 ChatGPT",
            })
            continue

        extra = row.get_extra()
        access_token = extra.get("access_token") or row.token
        tasks.append({
            "row": row,
            "id": row.id,
            "email": row.email,
            "access_token": access_token,
        })

    results_by_id: dict[int, dict] = {}
    requested_workers = body.workers or 8
    worker_count = max(1, min(int(requested_workers), 64))
    summary["workers"] = worker_count

    if tasks:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_check_chatgpt_access_token, task["email"], task["access_token"]): task["id"]
                for task in tasks
            }
            for future in as_completed(future_map):
                account_id = future_map[future]
                try:
                    results_by_id[account_id] = future.result()
                except Exception as e:
                    results_by_id[account_id] = {
                        "email": "",
                        "ok": False,
                        "invalid_401": False,
                        "status_code": None,
                        "msg": f"检测异常: {str(e)}",
                    }

    checked_at = datetime.now(timezone.utc)

    try:
        for task in tasks:
            row = task["row"]
            result = results_by_id.get(task["id"]) or {
                "email": row.email,
                "ok": False,
                "invalid_401": False,
                "status_code": None,
                "msg": "检测结果缺失",
            }

            extra = row.get_extra()
            extra["last_401_check_at"] = checked_at.isoformat()
            extra["last_401_check_status_code"] = result.get("status_code")
            extra["last_401_check_message"] = result.get("msg")
            extra["last_401_check_invalid_401"] = bool(result.get("invalid_401"))
            row.set_extra(extra)

            if result.get("invalid_401"):
                row.status = "invalid"
                summary["invalid_401"] += 1
            elif result.get("ok"):
                summary["valid"] += 1
            else:
                summary["failed"] += 1

            row.updated_at = checked_at
            session.add(row)
            summary["updated"] += 1
            summary["items"].append({
                "id": row.id,
                "email": row.email,
                "ok": bool(result.get("ok")),
                "invalid_401": bool(result.get("invalid_401")),
                "status_code": result.get("status_code"),
                "msg": result.get("msg") or "",
            })

        session.commit()
    except Exception as e:
        session.rollback()
        logger.exception("ChatGPT 401 检测失败")
        raise HTTPException(500, f"ChatGPT 401 检测失败: {str(e)}")

    return summary


@router.post("/chatgpt/delete-invalid-401")
def batch_delete_chatgpt_invalid_401(
    body: BatchChatGPT401DeleteRequest,
    session: Session = Depends(get_session),
):
    """一键删除已标记为 401 无效的 ChatGPT 账号。"""
    requested_ids: list[int] = []
    rows: list[AccountModel | None]

    if body.ids:
        seen: set[int] = set()
        for raw_id in body.ids:
            try:
                account_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if account_id <= 0 or account_id in seen:
                continue
            seen.add(account_id)
            requested_ids.append(account_id)

        if not requested_ids:
            raise HTTPException(400, "有效账号 ID 列表不能为空")
        if len(requested_ids) > 2000:
            raise HTTPException(400, "单次最多删除 2000 个账号")

        matched_rows: list[AccountModel] = []
        for start in range(0, len(requested_ids), 500):
            chunk = requested_ids[start:start + 500]
            matched_rows.extend(
                session.exec(
                    select(AccountModel).where(AccountModel.id.in_(chunk))
                ).all()
            )
        row_map = {
            row.id: row
            for row in matched_rows
            if row.id is not None and row.platform == "chatgpt"
        }
        rows = [row_map.get(account_id) for account_id in requested_ids]
    else:
        q = select(AccountModel).where(AccountModel.platform == "chatgpt")
        if body.status:
            q = q.where(AccountModel.status == body.status)
        if body.email:
            q = q.where(AccountModel.email.contains(body.email))
        matched_rows = session.exec(q.order_by(AccountModel.created_at.desc())).all()
        if not matched_rows:
            raise HTTPException(404, "未找到可删除的 ChatGPT 账号")
        if len(matched_rows) > 2000:
            raise HTTPException(400, "单次最多删除 2000 个账号，请先缩小筛选范围")
        rows = list(matched_rows)

    summary = {
        "total": len(rows),
        "matched": 0,
        "deleted": 0,
        "skipped": 0,
        "items": [],
    }

    try:
        for index, row in enumerate(rows):
            if row is None:
                missing_id = requested_ids[index] if index < len(requested_ids) else None
                summary["skipped"] += 1
                summary["items"].append({
                    "id": missing_id,
                    "email": "",
                    "ok": False,
                    "msg": "账号不存在或不是 ChatGPT，已跳过",
                })
                continue

            if not _is_chatgpt_invalid_401(row):
                summary["skipped"] += 1
                summary["items"].append({
                    "id": row.id,
                    "email": row.email,
                    "ok": False,
                    "msg": "不是已标记的 401 无效账号，已跳过",
                })
                continue

            summary["matched"] += 1
            session.delete(row)
            summary["deleted"] += 1
            summary["items"].append({
                "id": row.id,
                "email": row.email,
                "ok": True,
                "msg": "已删除",
            })

        session.commit()
        logger.info(
            "删除 ChatGPT 401 无效账号完成: total=%s matched=%s deleted=%s skipped=%s",
            summary["total"], summary["matched"], summary["deleted"], summary["skipped"]
        )
    except Exception as e:
        session.rollback()
        logger.exception("删除 ChatGPT 401 无效账号失败")
        raise HTTPException(500, f"删除 ChatGPT 401 无效账号失败: {str(e)}")

    return summary


@router.post("/check-all")
def check_all_accounts(platform: Optional[str] = None,
                       background_tasks: BackgroundTasks = None):
    from core.scheduler import scheduler
    background_tasks.add_task(scheduler.check_accounts_valid, platform)
    return {"message": "批量检测任务已启动"}


@router.get("/{account_id}")
def get_account(account_id: int, session: Session = Depends(get_session)):
    acc = session.get(AccountModel, account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    return acc


@router.patch("/{account_id}")
def update_account(account_id: int, body: AccountUpdate,
                   session: Session = Depends(get_session)):
    acc = session.get(AccountModel, account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    if body.status is not None:
        acc.status = body.status
    if body.token is not None:
        acc.token = body.token
    if body.cashier_url is not None:
        acc.cashier_url = body.cashier_url
    acc.updated_at = datetime.now(timezone.utc)
    session.add(acc)
    session.commit()
    session.refresh(acc)
    return acc


@router.delete("/{account_id}")
def delete_account(account_id: int, session: Session = Depends(get_session)):
    acc = session.get(AccountModel, account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    session.delete(acc)
    session.commit()
    return {"ok": True}


@router.post("/{account_id}/check")
def check_account(account_id: int, background_tasks: BackgroundTasks,
                  session: Session = Depends(get_session)):
    acc = session.get(AccountModel, account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    background_tasks.add_task(_do_check, account_id)
    return {"message": "检测任务已启动"}


def _do_check(account_id: int):
    from core.db import engine
    from sqlmodel import Session
    with Session(engine) as s:
        acc = s.get(AccountModel, account_id)
    if acc:
        from core.base_platform import Account, RegisterConfig
        from core.registry import get
        try:
            PlatformCls = get(acc.platform)
            plugin = PlatformCls(config=RegisterConfig())
            obj = Account(platform=acc.platform, email=acc.email,
                         password=acc.password, user_id=acc.user_id,
                         region=acc.region, token=acc.token,
                         extra=json.loads(acc.extra_json or "{}"))
            valid = plugin.check_valid(obj)
            with Session(engine) as s:
                a = s.get(AccountModel, account_id)
                if a:
                    if a.platform != "chatgpt":
                        a.status = a.status if valid else "invalid"
                    a.updated_at = datetime.now(timezone.utc)
                    s.add(a)
                    s.commit()
        except Exception:
            logger.exception("检测账号 %s 时出错", account_id)
