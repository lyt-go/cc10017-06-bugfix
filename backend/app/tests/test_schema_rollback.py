"""Schema回滚缺陷修正测试：演进回滚清单的生成、交付、恢复与中断保护。

缺陷背景：Schema回滚只更新规则状态，流式计算任务仍按已删除字段运行，
反序列化失败也没有清单说明任务停在哪里、恢复时使用哪些字段。
修正后：回滚生成演进回滚清单，先冻结依赖任务再恢复字段，
清单跨模块交付流式计算，异常进入可追溯链路，状态单向流转留痕。
"""
from __future__ import annotations

import unittest
from typing import Any

from app.services.flink_job import FlinkJobService
from app.services.schema_evolve import (
    MANIFEST_INTERRUPTED,
    MANIFEST_STATUS_ORDER,
    MANIFEST_TABLE,
    SchemaEvolveService,
)
from app.services.topic_registry import TopicRegistryService
from app.store import store


class SchemaRollbackCase(unittest.TestCase):
    """每条用例搭建独立的订单链路：主题上线→订单聚合任务运行→演进规则生效。"""

    _seq = 0

    def setUp(self) -> None:
        self.schema_svc = SchemaEvolveService()
        self.flink_svc = FlinkJobService()
        self.topic_svc = TopicRegistryService()

    def _build_order_chain(self, deleted_fields: str = "coupon_code") -> dict[str, Any]:
        # 全局递增，保证跨测试类的主题/任务/规则编号不冲突
        SchemaRollbackCase._seq += 1
        seq = SchemaRollbackCase._seq
        topic_code = f"TOPI-RB{seq:02d}"
        offset = 1048576 + seq
        topic, _ = self.topic_svc.create_entry({
            "主题编号": topic_code, "主题名称": f"订单事件流{seq}", "消息格式": "JSON",
            "Schema版本": "v3",
        })
        self.topic_svc.run_action(topic["id"], "确认上线")
        store.rows("watermark_track").append({
            "id": 900 + seq, "status": "正常", "pending": False, "abnormal": False,
            "标记编号": f"WATE-RB{seq:02d}", "数据主题": topic_code, "分区编号": "0",
            "事件时间水位": "2026-11-19T10:00:00", "处理时间水位": "2026-11-19T10:02:00",
            "延迟记录数": "0", "消费偏移": str(offset), "检查时间": "2026-11-19T10:05:00",
        })
        job, _ = self.flink_svc.create_entry({
            "任务编号": f"FLIN-RB{seq:02d}", "消费主题": topic_code,
            "转换逻辑": "窗口聚合计算订单金额", "输出表名": f"order_agg_{seq}",
        })
        entry, msg = self.flink_svc.run_action(job["id"], "启动作业")
        self.assertIsNotNone(entry, msg)
        rule, _ = self.schema_svc.create_entry({
            "规则编号": f"SCHE-RB{seq:02d}", "关联主题": topic_code,
            "新增字段": "shipping_address", "删除字段": deleted_fields,
            "类型变更": "-", "默认值填充": "-", "兼容模式": "FORWARD", "生效版本": "v3",
        })
        self.schema_svc.run_action(rule["id"], "提交校验")
        self.schema_svc.run_action(rule["id"], "标记生效")
        return {"topic": topic, "job": job, "rule": rule, "offset": offset, "topic_code": topic_code}

    def _rollback(self, rule_id: int) -> dict[str, Any]:
        entry, msg = self.schema_svc.run_action(rule_id, "执行回滚")
        self.assertIsNotNone(entry, msg)
        return entry


class TestRollbackManifest(SchemaRollbackCase):
    """演进回滚清单：停止边界与恢复字段同时可见。"""

    def test_rollback_generates_manifest_with_boundary_and_fields(self) -> None:
        chain = self._build_order_chain()
        entry = self._rollback(chain["rule"]["id"])
        self.assertEqual(entry["status"], "已回滚")

        manifest = self.schema_svc.active_manifest(chain["rule"]["id"])
        self.assertIsNotNone(manifest)
        # 恢复字段来自订单字段演进规则的已删除字段
        self.assertEqual(manifest["recovery_fields"], ["coupon_code"])
        # 停止边界取任务最后检查点（水位消费偏移）
        self.assertEqual(manifest["stop_boundary"]["jobs"][0]["last_checkpoint"], chain["offset"])
        self.assertEqual(manifest["stop_boundary"]["strategy"], "先冻结依赖任务再恢复字段")
        # 清单只有一个可执行恢复点
        self.assertEqual(manifest["recovery_point"]["offset"], chain["offset"])
        self.assertEqual(manifest["recovery_point"]["fields"], ["coupon_code"])
        self.assertTrue(manifest["recovery_point"]["executable"])
        self.assertEqual(manifest["status"], MANIFEST_STATUS_ORDER[-1])
        self.assertTrue(manifest["complete"])

    def test_rollback_pauses_order_aggregation_job(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        job = store.find("flink_job", chain["job"]["id"])
        # 流式计算模块按清单暂停订单聚合任务
        self.assertEqual(job["status"], "已停止")
        self.assertEqual(job["_stopped_offset"], chain["offset"])
        self.assertEqual(job["_rollback_manifest"], entry_manifest_id(chain["rule"]["id"]))

    def test_deserialization_exception_enters_traceable_chain(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        manifest = self.schema_svc.active_manifest(chain["rule"]["id"])
        # 异常消息写进清单
        self.assertIn("反序列化失败", manifest["exception_message"])
        self.assertIn("coupon_code", manifest["exception_message"])
        # 异常消息进入死信可追溯链路，并关联清单编号
        dead_letters = [
            r for r in store.rows("dead_letter")
            if r.get("_manifest_id") == manifest["manifest_id"]
        ]
        self.assertEqual(len(dead_letters), 1)
        self.assertEqual(dead_letters[0]["失败原因"], "反序列化失败")
        self.assertEqual(dead_letters[0]["原始主题"], chain["topic_code"])

    def test_manifest_delivered_to_flink_with_current_version(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        manifest = self.schema_svc.active_manifest(chain["rule"]["id"])
        delivered = self.flink_svc.get_rollback_manifests()
        match = [m for m in delivered if m["manifest_id"] == manifest["manifest_id"]]
        # 跨模块交付保留当前版本
        self.assertEqual(len(match), 1)
        self.assertEqual(match[0]["version"], manifest["version"])

    def test_rollback_idempotent_avoids_duplicate_manifest(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        first = self.schema_svc.active_manifest(chain["rule"]["id"])
        # 重复回滚复用生效清单，避免重复写入
        entry, msg = self.schema_svc.run_action(chain["rule"]["id"], "执行回滚")
        self.assertIsNotNone(entry)
        self.assertIn("避免重复写入", msg)
        manifests = self.schema_svc.list_manifests(rule_id=chain["rule"]["id"])
        self.assertEqual(len(manifests), 1)
        self.assertEqual(manifests[0]["manifest_id"], first["manifest_id"])

    def test_no_half_rollback_state(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        rule = store.find("schema_evolve", chain["rule"]["id"])
        job = store.find("flink_job", chain["job"]["id"])
        manifest = self.schema_svc.active_manifest(chain["rule"]["id"])
        # 规则、任务、清单、异常都不落在半回滚状态
        self.assertEqual(rule["status"], "已回滚")
        self.assertEqual(job["status"], "已停止")
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["status"], "已恢复")
        self.assertEqual(job["_stopped_offset"], manifest["recovery_point"]["offset"])
        self.assertEqual(len(manifest["history"]), 3)
        # 主题Schema版本随字段恢复回退，演进列表与流式计算页面读到同一份状态
        topic = store.find("topic_registry", chain["topic"]["id"])
        self.assertEqual(topic["Schema版本"], "v2")
        self.assertEqual(manifest["schema_version_restored"], {"from": "v3", "to": "v2"})


class TestManifestLifecycle(SchemaRollbackCase):
    """清单生命周期单向流转，切换、变更与恢复都留下可查记录。"""

    def test_manifest_status_flows_unidirectionally_with_history(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        manifest = self.schema_svc.active_manifest(chain["rule"]["id"])
        transitions = [(h["from"], h["to"]) for h in manifest["history"]]
        self.assertEqual(
            transitions,
            [(None, "已生成"), ("已生成", "已交付"), ("已交付", "已恢复")],
        )
        # 不允许回退流转
        self.assertFalse(self.schema_svc._transition(manifest, "已生成", "尝试回退"))
        self.assertEqual(manifest["status"], "已恢复")

    def test_interrupted_manifest_quarantined_keeps_last_complete(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        complete = self.schema_svc.active_manifest(chain["rule"]["id"])
        # 模拟写入中断：半成品清单只走完「已生成」
        store.rows(MANIFEST_TABLE).append({
            "id": 9000, "manifest_id": "RBCK-9000", "rule_id": chain["rule"]["id"],
            "rule_code": chain["rule"]["规则编号"], "topic_code": chain["topic_code"],
            "version": 2, "deleted_fields": ["coupon_code"], "recovery_fields": ["coupon_code"],
            "stop_boundary": None, "recovery_point": None, "frozen_jobs": [],
            "exception_message": "", "schema_version_restored": None,
            "status": MANIFEST_STATUS_ORDER[0], "complete": False,
            "history": [{"from": None, "to": "已生成", "at": "2026-11-19T11:00:00", "note": "写入中断"}],
            "created_at": "2026-11-19T11:00:00", "updated_at": "2026-11-19T11:00:00",
        })
        result = self.schema_svc.recover_interrupted()
        # 半成品被隔离到终态（按本用例的规则过滤，避免其他用例残留干扰）
        quarantined_ids = [
            m["manifest_id"] for m in result["quarantined"]
            if int(m.get("rule_id", 0)) == chain["rule"]["id"]
        ]
        self.assertEqual(quarantined_ids, ["RBCK-9000"])
        interrupted = [m for m in self.schema_svc.list_manifests(rule_id=chain["rule"]["id"])
                       if m["manifest_id"] == "RBCK-9000"][0]
        self.assertEqual(interrupted["status"], MANIFEST_INTERRUPTED)
        # 保留最后一份完整清单作为唯一可执行恢复点
        active = self.schema_svc.active_manifest(chain["rule"]["id"])
        self.assertEqual(active["manifest_id"], complete["manifest_id"])
        self.assertEqual(active["recovery_point"]["offset"], chain["offset"])
        # 中断清单不可再流转
        self.assertFalse(self.schema_svc._transition(interrupted, "已交付", "尝试复活"))

    def test_rule_status_cannot_move_backwards(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        entry, msg = self.schema_svc.run_action(chain["rule"]["id"], "提交校验")
        self.assertIsNone(entry)
        self.assertIn("回退", msg)

    def test_rollback_requires_effective_rule(self) -> None:
        rule, _ = self.schema_svc.create_entry({
            "规则编号": "SCHE-RB-DRAFT", "关联主题": "TOPI-0001",
            "新增字段": "x", "删除字段": "y",
        })
        entry, msg = self.schema_svc.run_action(rule["id"], "执行回滚")
        self.assertIsNone(entry)
        self.assertIn("已生效", msg)


class TestFlinkSideIntegration(SchemaRollbackCase):
    """流式计算模块按清单暂停与恢复，时间线可见停止位置、恢复字段与异常消息。"""

    def test_timeline_shows_boundary_fields_and_exception(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        timeline = self.flink_svc.get_timeline()
        item = next(i for i in timeline if i["id"] == chain["job"]["id"])
        self.assertEqual(item["status"], "已停止")
        self.assertIsNotNone(item["rollback"])
        self.assertEqual(item["rollback"]["stop_offset"], chain["offset"])
        self.assertEqual(item["rollback"]["recovery_fields"], ["coupon_code"])
        self.assertIn("反序列化失败", item["rollback"]["exception"])
        self.assertIn("停止位置", item["logs"])
        self.assertIn("恢复字段", item["logs"])
        self.assertIn("异常消息", item["logs"])

    def test_restart_resumes_from_recovery_point(self) -> None:
        chain = self._build_order_chain()
        self._rollback(chain["rule"]["id"])
        entry, msg = self.flink_svc.run_action(chain["job"]["id"], "启动作业")
        self.assertIsNotNone(entry, msg)
        # 从清单恢复点继续，按恢复字段读取，避免重复写入
        self.assertEqual(entry["_resume_offset"], chain["offset"])
        self.assertEqual(entry["_active_fields"], ["coupon_code"])

    def test_start_blocked_while_manifest_not_recovered(self) -> None:
        chain = self._build_order_chain()
        # 模拟清单已交付但字段恢复未完成（半回滚状态）
        fake = {
            "id": 9001, "manifest_id": "RBCK-9001", "rule_id": chain["rule"]["id"],
            "rule_code": chain["rule"]["规则编号"], "topic_code": chain["topic_code"],
            "version": 1, "deleted_fields": ["coupon_code"], "recovery_fields": ["coupon_code"],
            "stop_boundary": None, "recovery_point": None, "frozen_jobs": [],
            "exception_message": "", "schema_version_restored": None,
            "status": MANIFEST_STATUS_ORDER[1], "complete": False,
            "history": [], "created_at": "2026-11-19T11:00:00", "updated_at": "2026-11-19T11:00:00",
        }
        store.rows(MANIFEST_TABLE).append(fake)
        try:
            self.flink_svc.run_action(chain["job"]["id"], "停止作业")
            entry, msg = self.flink_svc.run_action(chain["job"]["id"], "启动作业")
            self.assertIsNone(entry)
            self.assertIn("RBCK-9001", msg)
            self.assertIn("恢复", msg)
        finally:
            store.rows(MANIFEST_TABLE).remove(fake)


def entry_manifest_id(rule_id: int) -> str:
    rule = store.find("schema_evolve", rule_id)
    return str(rule.get("_rollback_manifest"))


if __name__ == "__main__":
    unittest.main()
