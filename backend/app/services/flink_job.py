"""流式计算业务规则：作业管理、链路消费与状态推进。

增强：Flink作业启动时校验上游主题状态，运行中消费主题数据生成计算产物。
接入Schema演进交付的演进回滚清单：按清单暂停订单聚合任务、记录停止边界，
恢复时从清单的恢复点继续，避免回滚后仍读取已删除字段。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from app.store import store

MODULE = "flink_job"
ROLLBACK_MANIFEST_TABLE = "schema_rollback_manifests"
REQUIRED_FIELDS = ["任务编号", "消费主题", "转换逻辑"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止"]
ACTION_RULES = {"提交校验": "待校验", "启动作业": "运行中", "停止作业": "已停止"}
NEGATIVE_ACTIONS = []


class FlinkJobService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            logs = f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}"
            rollback = self._rollback_info(row)
            if rollback:
                logs += (
                    f"\n[{rollback['stopped_at']}] 演进回滚清单 {rollback['manifest_id']} 冻结任务，"
                    f"停止位置 offset={rollback['stop_offset']}"
                    f"\n恢复字段：{'、'.join(rollback['recovery_fields']) or '无'}；"
                    f"异常消息：{rollback['exception'] or '无'}"
                )
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": logs,
                "rollback": rollback,
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        # 校验消费主题是否存在
        topic_code = values.get("消费主题")
        if topic_code:
            topic = self._find_topic(topic_code)
            if not topic:
                return None, [f"消费主题 {topic_code} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        # 补充可选字段
        for f in ["输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]:
            entry[f] = values.get(f, "")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流式任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式计算可执行范围"
        target = ACTION_RULES[action]

        # 启动作业时校验上游主题必须已上线
        if action == "启动作业":
            topic_code = entry.get("消费主题")
            topic = self._find_topic(topic_code) if topic_code else None
            if not topic:
                return None, f"消费主题 {topic_code} 未注册"
            if topic.get("status") != "已上线":
                return None, f"消费主题 {topic_code} 状态为「{topic.get('status')}」，须先上线"
            # 存在已交付但未完成字段恢复的回滚清单时禁止启动，避免继续读取已删除字段
            pending = self._pending_manifest_for_topic(str(topic_code))
            if pending is not None:
                return None, (
                    f"主题 {topic_code} 的演进回滚清单 {pending['manifest_id']} 尚未完成字段恢复，"
                    "须先完成恢复再启动"
                )
            # 已恢复的清单：从恢复点继续，按恢复字段读取，避免重复写入
            recovered = self._recovered_manifest_for_topic(str(topic_code))
            if recovered is not None:
                point = recovered.get("recovery_point") or {}
                entry["_resume_offset"] = point.get("offset", 0)
                entry["_active_fields"] = point.get("fields", [])
                entry["_rollback_manifest"] = recovered.get("manifest_id")
            # 生成计算产物
            self._generate_compute_artifact(entry, topic)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"流式任务已{action}"

    # ───────────────────────────────
    # 演进回滚清单接入
    # ───────────────────────────────

    def apply_rollback_manifest(self, manifest: dict[str, Any]) -> dict[str, Any]:
        """接收Schema演进交付的演进回滚清单：冻结依赖任务并记录停止边界。

        反序列化异常写入死信可追溯链路；清单状态流转由Schema演进模块负责。
        """
        topic_code = str(manifest.get("topic_code") or "")
        recovery_fields = list(manifest.get("recovery_fields") or [])
        now = datetime.now().isoformat()
        frozen_jobs: list[dict[str, Any]] = []
        exceptions: list[str] = []
        for job in store.rows(MODULE):
            if str(job.get("消费主题") or "") != topic_code:
                continue
            if job.get("status") != "运行中":
                continue
            offset = self._topic_checkpoint(topic_code)
            job["status"] = "已停止"
            job["pending"] = False
            job["_rollback_manifest"] = manifest.get("manifest_id")
            job["_stopped_offset"] = offset
            job["_stopped_at"] = now
            job["_recovery_fields"] = recovery_fields
            frozen_jobs.append({
                "job_id": job.get("id"),
                "job_code": job.get("任务编号"),
                "last_checkpoint": offset,
                "stopped_at": now,
            })
            if recovery_fields:
                message = (
                    f"任务 {job.get('任务编号')} 在规则 {manifest.get('rule_code')} 回滚前"
                    f"仍按已删除字段 {'、'.join(recovery_fields)} 读取主题 {topic_code}，出现反序列化失败"
                )
                exceptions.append(message)
                self._append_dead_letter(job, manifest, message, now)
        return {"frozen_jobs": frozen_jobs, "exceptions": exceptions}

    def get_rollback_manifests(self) -> list[dict[str, Any]]:
        """流式计算模块接收到的演进回滚清单，保留当前版本。"""
        delivered = ("已交付", "已恢复")
        return [
            m for m in store.rows(ROLLBACK_MANIFEST_TABLE)
            if m.get("status") in delivered
        ]

    def _rollback_info(self, row: dict[str, Any]) -> dict[str, Any] | None:
        manifest_id = row.get("_rollback_manifest")
        if not manifest_id:
            return None
        manifest = next(
            (m for m in store.rows(ROLLBACK_MANIFEST_TABLE) if m.get("manifest_id") == manifest_id),
            None,
        )
        if manifest is None:
            return None
        return {
            "manifest_id": manifest_id,
            "stop_offset": row.get("_stopped_offset"),
            "stopped_at": row.get("_stopped_at"),
            "recovery_fields": list(manifest.get("recovery_fields") or []),
            "exception": manifest.get("exception_message", ""),
            "status": manifest.get("status"),
        }

    def _pending_manifest_for_topic(self, topic_code: str) -> dict[str, Any] | None:
        """已交付但尚未完成字段恢复的清单：存在时禁止任务启动。"""
        for manifest in store.rows(ROLLBACK_MANIFEST_TABLE):
            if str(manifest.get("topic_code") or "") == topic_code and manifest.get("status") == "已交付":
                return manifest
        return None

    def _recovered_manifest_for_topic(self, topic_code: str) -> dict[str, Any] | None:
        """主题当前生效的已恢复清单：启动时按恢复点继续。"""
        candidates = [
            m for m in store.rows(ROLLBACK_MANIFEST_TABLE)
            if str(m.get("topic_code") or "") == topic_code
            and m.get("complete")
            and m.get("status") == "已恢复"
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda m: int(m.get("version", 0)))

    def _topic_checkpoint(self, topic_code: str) -> int:
        """主题当前最后检查点：以水位追踪的消费偏移为准。"""
        for row in store.rows("watermark_track"):
            if row.get("数据主题") == topic_code:
                try:
                    return int(str(row.get("消费偏移", "0")))
                except (ValueError, TypeError):
                    return 0
        return 0

    def _append_dead_letter(
        self,
        job: dict[str, Any],
        manifest: dict[str, Any],
        message: str,
        now: str,
    ) -> None:
        """反序列化异常写入死信，关联清单编号，进入可追溯链路。"""
        rows = store.rows("dead_letter")
        dl_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append({
            "id": dl_id,
            "status": "待处置",
            "pending": True,
            "abnormal": True,
            "记录编号": f"DEAD-{dl_id:04d}",
            "原始主题": manifest.get("topic_code"),
            "原始分区": "0",
            "失败原因": "反序列化失败",
            "原始消息体": message,
            "重试次数": "0",
            "入库时间": now,
            "处置状态": "待分析",
            "_manifest_id": manifest.get("manifest_id"),
            "_job_code": job.get("任务编号"),
        })

    # ───────────────────────────────
    # 既有能力
    # ───────────────────────────────

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _generate_compute_artifact(self, entry: dict[str, Any], topic: dict[str, Any]) -> None:
        """Flink作业运行时生成计算产物记录。"""
        output_table = entry.get("输出表名", "unknown")
        artifact = {
            "job_id": entry.get("id"),
            "job_code": entry.get("任务编号"),
            "topic_code": topic.get("主题编号"),
            "topic_name": topic.get("主题名称"),
            "output_table": output_table,
            "transform": entry.get("转换逻辑"),
            "parallelism": entry.get("并行度"),
            "computed_at": datetime.now().isoformat(),
            "status": "running",
        }
        if entry.get("_resume_offset") is not None:
            artifact["resume_offset"] = entry["_resume_offset"]
            artifact["active_fields"] = entry.get("_active_fields", [])
        self._ensure_artifact_table("flink_compute_artifacts")
        store.rows("flink_compute_artifacts").append(artifact)
        entry["_last_artifact"] = artifact

    def get_compute_artifacts(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("flink_compute_artifacts")
        return [a for a in store.rows("flink_compute_artifacts") if a.get("job_id") == entry_id]

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
