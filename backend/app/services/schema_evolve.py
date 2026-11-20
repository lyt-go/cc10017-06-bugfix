"""Schema演进业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：执行回滚时生成演进回滚清单——先冻结依赖的流式任务并记录停止边界，
再恢复已删除字段；清单跨模块交付给流式计算模块，反序列化异常写入可追溯链路。
清单生命周期只允许单向流转，写入中断的半成品清单可被隔离并恢复到最后一份完整清单。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.flink_job import FlinkJobService
from app.store import store

MODULE = "schema_evolve"
MANIFEST_TABLE = "schema_rollback_manifests"
LIST_FIELDS = ["规则编号", "关联主题", "新增字段", "删除字段", "类型变更", "默认值填充", "兼容模式", "生效版本"]
REQUIRED_FIELDS = ["规则编号", "关联主题", "新增字段"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已回滚"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "执行回滚": "已回滚"}
NEGATIVE_ACTIONS = []

# 演进回滚清单生命周期：只允许沿序列单向流转，每次切换都写入 history 留痕；
# 写入中断的半成品清单只能进入「生成中断」终态，不可回退、不可复活。
MANIFEST_STATUS_ORDER = ["已生成", "已交付", "已恢复"]
MANIFEST_INTERRUPTED = "生成中断"
# 冲突判定：已删除字段与任务最后检查点同时存在时，先冻结依赖任务再恢复字段，
# 清单只保留一个可执行恢复点，恢复时从冻结位置继续，避免重复写入。
FREEZE_THEN_RESTORE = "先冻结依赖任务再恢复字段"


def _now() -> str:
    return datetime.now().isoformat()


def _parse_fields(raw: Any) -> list[str]:
    """把「删除字段」解析成字段列表；「-」与空串视为没有删除字段。"""
    text = str(raw or "").strip()
    if not text or text == "-":
        return []
    return [part.strip() for part in text.replace("，", ",").split(",") if part.strip()]


class SchemaEvolveService:
    def __init__(self) -> None:
        self._flink = FlinkJobService()

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"演进规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于Schema演进可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        # 状态在生命周期内单向流转，不允许回退
        current = str(entry.get("status", ""))
        current_idx = STATUS_ORDER.index(current) if current in STATUS_ORDER else -1
        target_idx = STATUS_ORDER.index(target)
        if target_idx < current_idx:
            return None, f"状态不能从「{current}」回退到「{target}」"
        if action == "执行回滚" and current not in ("已生效", "已回滚"):
            return None, f"只有已生效的演进规则才能执行回滚，当前状态为「{current}」"

        message = f"演进规则已{action}"
        if action == "执行回滚":
            manifest, manifest_message = self._execute_rollback(entry)
            entry["_rollback_manifest"] = manifest["manifest_id"]
            message = f"{message}，{manifest_message}"

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, message

    # ───────────────────────────────
    # 演进回滚清单
    # ───────────────────────────────

    def list_manifests(self, rule_id: int | None = None) -> list[dict[str, Any]]:
        """清单列表：停止边界、恢复字段与异常消息同时可见。"""
        rows = store.rows(MANIFEST_TABLE)
        if rule_id is not None:
            rows = [m for m in rows if int(m.get("rule_id", 0)) == rule_id]
        return sorted(rows, key=lambda m: int(m.get("id", 0)))

    def active_manifest(self, rule_id: int) -> dict[str, Any] | None:
        """规则当前生效的清单：最后一份完整清单，即唯一可执行恢复点。"""
        candidates = [
            m for m in store.rows(MANIFEST_TABLE)
            if int(m.get("rule_id", 0)) == rule_id
            and m.get("complete")
            and m.get("status") == MANIFEST_STATUS_ORDER[-1]
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda m: int(m.get("version", 0)))

    def recover_interrupted(self) -> dict[str, Any]:
        """回滚中断恢复：隔离半成品清单，核对每个规则只保留最后一份完整清单。"""
        quarantined = self._quarantine_interrupted()
        actives: dict[int, dict[str, Any]] = {}
        for manifest in store.rows(MANIFEST_TABLE):
            if not manifest.get("complete") or manifest.get("status") != MANIFEST_STATUS_ORDER[-1]:
                continue
            rule_id = int(manifest.get("rule_id", 0))
            current = actives.get(rule_id)
            if current is None or int(manifest.get("version", 0)) > int(current.get("version", 0)):
                actives[rule_id] = manifest
        return {"quarantined": quarantined, "active_manifests": list(actives.values())}

    def _execute_rollback(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        """生成演进回滚清单：先冻结依赖任务再恢复字段，避免恢复后重复写入。"""
        rule_id = int(entry.get("id", 0))
        active = self.active_manifest(rule_id)
        if active is not None:
            return active, f"清单 {active['manifest_id']} 仍在生效，复用避免重复写入"

        # 写入中断的半成品清单先隔离，保证只保留最后一份完整清单
        self._quarantine_interrupted(rule_id)

        topic_code = str(entry.get("关联主题") or "")
        recovery_fields = _parse_fields(entry.get("删除字段"))
        seq = self._next_manifest_seq()
        manifest = {
            "id": seq,
            "manifest_id": f"RBCK-{seq:04d}",
            "rule_id": rule_id,
            "rule_code": entry.get("规则编号"),
            "topic_code": topic_code,
            "version": self._next_version(rule_id),
            "deleted_fields": recovery_fields,
            "recovery_fields": recovery_fields,
            "stop_boundary": None,
            "recovery_point": None,
            "frozen_jobs": [],
            "exception_message": "",
            "schema_version_restored": None,
            "status": MANIFEST_STATUS_ORDER[0],
            "complete": False,
            "history": [{"from": None, "to": MANIFEST_STATUS_ORDER[0], "at": _now(),
                         "note": "执行回滚，生成演进回滚清单"}],
            "created_at": _now(),
            "updated_at": _now(),
        }
        store.rows(MANIFEST_TABLE).append(manifest)

        # 阶段二：跨模块交付——流式计算模块接入清单并暂停订单聚合任务，
        # 停止边界取各任务最后检查点，反序列化异常写入可追溯链路
        applied = self._flink.apply_rollback_manifest(manifest)
        manifest["frozen_jobs"] = applied["frozen_jobs"]
        manifest["stop_boundary"] = {
            "strategy": FREEZE_THEN_RESTORE,
            "determined_at": _now(),
            "jobs": applied["frozen_jobs"],
        }
        manifest["exception_message"] = "；".join(applied["exceptions"])
        self._transition(manifest, MANIFEST_STATUS_ORDER[1],
                         "交付流式计算模块，冻结依赖任务并记录停止边界")

        # 阶段三：恢复字段，固定唯一可执行恢复点
        offsets = [int(job["last_checkpoint"]) for job in applied["frozen_jobs"]]
        manifest["recovery_point"] = {
            "offset": min(offsets) if offsets else 0,
            "fields": recovery_fields,
            "executable": True,
            "determined_by": FREEZE_THEN_RESTORE,
        }
        manifest["schema_version_restored"] = self._restore_topic_version(topic_code)
        manifest["complete"] = True
        self._transition(manifest, MANIFEST_STATUS_ORDER[2],
                         "恢复已删除字段，固定可执行恢复点")
        return manifest, f"演进回滚清单 {manifest['manifest_id']} 已生成并生效"

    def _transition(self, manifest: dict[str, Any], target: str, note: str) -> bool:
        """清单状态单向流转：只允许走到下一格，每次切换写入 history。"""
        current = str(manifest.get("status", ""))
        if current == MANIFEST_INTERRUPTED:
            return False
        try:
            current_idx = MANIFEST_STATUS_ORDER.index(current)
            target_idx = MANIFEST_STATUS_ORDER.index(target)
        except ValueError:
            return False
        if target_idx != current_idx + 1:
            return False
        manifest["history"].append({"from": current, "to": target, "at": _now(), "note": note})
        manifest["status"] = target
        manifest["updated_at"] = _now()
        return True

    def _quarantine_interrupted(self, rule_id: int | None = None) -> list[dict[str, Any]]:
        """把写入中断的半成品清单隔离到「生成中断」终态，保留最后一份完整清单。"""
        quarantined: list[dict[str, Any]] = []
        for manifest in store.rows(MANIFEST_TABLE):
            if rule_id is not None and int(manifest.get("rule_id", 0)) != rule_id:
                continue
            if manifest.get("complete") or manifest.get("status") == MANIFEST_INTERRUPTED:
                continue
            manifest["history"].append({
                "from": manifest.get("status"), "to": MANIFEST_INTERRUPTED, "at": _now(),
                "note": "写入中断，隔离半成品清单，保留最后一份完整清单",
            })
            manifest["status"] = MANIFEST_INTERRUPTED
            manifest["updated_at"] = _now()
            quarantined.append(manifest)
        return quarantined

    def _restore_topic_version(self, topic_code: str) -> dict[str, str] | None:
        """恢复字段后同步回退主题Schema版本，让演进列表与流式计算页面读到同一份状态。"""
        for row in store.rows("topic_registry"):
            if row.get("主题编号") != topic_code:
                continue
            current = str(row.get("Schema版本") or "")
            if current.startswith("v") and current[1:].isdigit():
                restored = f"v{max(1, int(current[1:]) - 1)}"
                row["Schema版本"] = restored
                return {"from": current, "to": restored}
            return None
        return None

    def _next_manifest_seq(self) -> int:
        return max((int(m.get("id", 0)) for m in store.rows(MANIFEST_TABLE)), default=0) + 1

    def _next_version(self, rule_id: int) -> int:
        versions = [
            int(m.get("version", 0))
            for m in store.rows(MANIFEST_TABLE)
            if int(m.get("rule_id", 0)) == rule_id
        ]
        return max(versions, default=0) + 1
