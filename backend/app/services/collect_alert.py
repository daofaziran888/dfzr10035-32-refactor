"""采集告警来源服务：收拢重放耗尽与抽样偏差的治理报告建档过程。

重放队列「标记失败」和抽样校验「标记告警」各自在领域服务里直接拼过一遍
治理报告。这里把来源事实登记、动作回执、按动作标识幂等建档收拢到同一处，
报告字段与既有行为保持不变：

- 每条来源事实一条 ``collect_alert_source`` 记录，同一端点、同一登记时间的
  不同来源各自保留，不做合并；
- 同一动作标识（来源模块+来源记录+动作）并发或重试只落一份治理报告；
- 来源登记与动作回执分开记录：报告可读但回执丢失时，按原动作标识找回原报告，
  报告及其之后的归档状态原样保留，仅补写回执；
- 来源事实不完整的登记标记为「来源不完整」，治理报告据此拒绝生成/归档。
"""
from __future__ import annotations

import threading
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 来源类型沿用报告编号前缀语义，保留两类来源的原有报告编号体系。
SOURCE_RETRY = "retry_exhausted"
SOURCE_SAMPLE = "sample_deviation"

RECEIPT_PENDING = "未回执"
RECEIPT_DONE = "已回执"

SOURCE_COMPLETE = "来源完整"
SOURCE_INCOMPLETE = "来源不完整"

# 两类来源各自必须齐备的事实字段；缺一即视为来源不完整。
REQUIRED_FACTS = {
    SOURCE_RETRY: ["source_ref", "endpoint", "report_no", "scope", "metric", "owner"],
    SOURCE_SAMPLE: ["source_ref", "endpoint", "report_no", "scope", "metric", "owner"],
}

# 与重放/抽样服务历史建档完全一致的报告字段模板，保证报告字段不变。
REPORT_TEMPLATE: dict[str, dict[str, Any]] = {
    SOURCE_RETRY: {
        "合规指标": "数据采集完整性",
        "当前得分": 60.0,
        "评估周期": "2026-Q4",
        "负责人": "运维值班",
        "改进事项": "任务{ref}重试耗尽，需人工介入",
        "评估范围": "重放队列永久失败: {endpoint}",
    },
    SOURCE_SAMPLE: {
        "合规指标": "数据质量一致性",
        "当前得分": 55.0,
        "评估周期": "2026-Q4",
        "负责人": "数据质量组",
        "改进事项": "端点{endpoint}抽样偏差超出阈值{threshold}",
        "评估范围": "抽样偏差告警:{endpoint}",
    },
}


class CollectAlertService(BaseModuleService):
    """采集告警来源记录：重放队列与抽样校验共用的治理报告建档入口。"""

    MODULE = "collect_alert_source"
    ENTITY = "采集告警来源"
    OPERATION_TERM = "登记"
    LIST_FIELDS = [
        "来源编号", "来源类型", "动作标识", "来源模块", "来源记录编号",
        "目标端点", "报告编号", "登记时间", "来源状态", "回执状态",
    ]
    REQUIRED_FIELDS = ["来源编号", "来源类型", "动作标识"]
    STATUS_ORDER = ["来源完整", "来源不完整"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        # 同一动作标识的并发送档需要串行化，保证只写一份报告。
        self._lock = threading.RLock()
        # 测试/恢复场景：关闭自动回执即可模拟“报告已登记、动作回执丢失”的中断窗口。
        self.auto_receipt = True

    # ---------- 动作标识 ----------

    @staticmethod
    def action_id(source_module: str, source_ref: str, action: str) -> str:
        """按原动作生成稳定标识；同一次标记失败中断后按它重试。"""
        return f"{source_module}:{source_ref}:{action}"

    def find_by_action(self, act_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("动作标识") == act_id:
                return row
        return None

    # ---------- 两类来源的事实封装（各自保留原报告编号/范围/指标/得分/责任人） ----------

    def build_retry_facts(self, task: dict) -> dict[str, Any]:
        tid = str(task["id"]).zfill(4)
        endpoint = task.get("失败端点", "")
        return {
            "source_type": SOURCE_RETRY,
            "source_module": "retry_queue",
            "source_entry_id": int(task["id"]),
            "source_ref": tid,
            "source_record_no": task.get("任务编号", f"RETR-{tid}"),
            "endpoint": endpoint,
            "action": "标记失败",
            "report_no": f"GOVE-RETRY-{tid}",
            "scope": f"重放队列永久失败: {endpoint}",
            "metric": "数据采集完整性",
            "score": 60.0,
            "owner": "运维值班",
            "period": "2026-Q4",
            "registered_at": "2026-11-17",
        }

    def build_sample_facts(self, check: dict) -> dict[str, Any]:
        sid = str(check["id"]).zfill(4)
        endpoint = check.get("目标端点", "")
        return {
            "source_type": SOURCE_SAMPLE,
            "source_module": "sample_check",
            "source_entry_id": int(check["id"]),
            "source_ref": sid,
            "source_record_no": check.get("计划编号", f"SAMP-{sid}"),
            "endpoint": endpoint,
            "action": "标记告警",
            "report_no": f"GOVE-SAMP-{sid}",
            "scope": f"抽样偏差告警:{endpoint}",
            "metric": "数据质量一致性",
            "score": 55.0,
            "owner": "数据质量组",
            "period": "2026-Q4",
            "threshold": check.get("偏差阈值", ""),
            "registered_at": "2026-11-17",
        }

    # ---------- 共用建档过程 ----------

    def register_alert(self, facts: dict[str, Any]) -> dict[str, Any]:
        """按来源事实登记采集告警并幂等创建治理报告。

        返回 dict 含：``source`` 来源记录、``report`` 治理报告（不完整时为占位
        待生成报告）、``created`` 是否本次新建、``receipt`` 回执状态、
        ``recovered`` 是否为回执丢失后的找回。
        """
        with self._lock:
            act_id = self.action_id(
                str(facts.get("source_module", "")),
                str(facts.get("source_ref", "")),
                str(facts.get("action", "")),
            )
            existing = self.find_by_action(act_id)
            if existing is not None:
                # 同一动作标识：绝不重复建档，只在回执丢失时补写回执并找回原报告。
                recovered = False
                if existing.get("回执状态") == RECEIPT_PENDING and self.auto_receipt:
                    existing["回执状态"] = RECEIPT_DONE
                    recovered = True
                report = self._find_linked_report(existing)
                return {
                    "source": existing,
                    "report": report,
                    "created": False,
                    "receipt": existing.get("回执状态"),
                    "recovered": recovered,
                }

            source_type = str(facts.get("source_type", ""))
            missing = [
                field for field in REQUIRED_FACTS.get(source_type, [])
                if not str(facts.get(field) or "").strip()
            ]
            complete = not missing
            source_rows = store.rows(self.MODULE)
            source_id = max((int(r.get("id", 0)) for r in source_rows), default=0) + 1
            source_no = f"ALERT-{str(source_id).zfill(4)}"
            source_row = {
                "id": source_id,
                "来源编号": source_no,
                "来源类型": source_type,
                "动作标识": act_id,
                "来源模块": facts.get("source_module", ""),
                "来源记录编号": facts.get("source_record_no", ""),
                "来源记录ID": facts.get("source_entry_id"),
                "目标端点": facts.get("endpoint", ""),
                "报告编号": facts.get("report_no", ""),
                "登记时间": facts.get("registered_at", "2026-11-17"),
                "来源状态": SOURCE_COMPLETE if complete else SOURCE_INCOMPLETE,
                "缺失事实": "、".join(missing),
                "回执状态": RECEIPT_PENDING,
                "status": SOURCE_COMPLETE if complete else SOURCE_INCOMPLETE,
                "pending": True,
                "abnormal": not complete,
            }
            source_rows.append(source_row)

            report = self._build_report(source_row, facts, complete)
            # 回执与来源登记分开：默认立即回执；关闭自动回执时保留“已登记未回执”窗口。
            if self.auto_receipt:
                source_row["回执状态"] = RECEIPT_DONE
            return {
                "source": source_row,
                "report": report,
                "created": True,
                "receipt": source_row["回执状态"],
                "recovered": False,
            }

    def _build_report(
        self, source: dict[str, Any], facts: dict[str, Any], complete: bool
    ) -> dict[str, Any]:
        """按来源事实写治理报告；字段取值与重构前两处建档保持一致。"""
        gov_rows = store.rows("govern_report")
        report_id = max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1
        template = REPORT_TEMPLATE.get(str(facts.get("source_type", "")), {})
        ref = str(facts.get("source_ref", ""))
        endpoint = str(facts.get("endpoint", ""))
        if complete:
            entry = {
                "id": report_id,
                "报告编号": facts.get("report_no", ""),
                "评估范围": facts.get("scope")
                or str(template.get("评估范围", "")).format(endpoint=endpoint),
                "合规指标": facts.get("metric", template.get("合规指标", "")),
                "当前得分": facts.get("score", template.get("当前得分", 0.0)),
                "改进事项": str(template.get("改进事项", "")).format(
                    ref=ref, endpoint=endpoint,
                    threshold=facts.get("threshold", ""),
                ),
                "评估周期": facts.get("period", template.get("评估周期", "")),
                "负责人": facts.get("owner", template.get("负责人", "")),
                "生成时间": facts.get("registered_at", "2026-11-17"),
                "status": "待生成",
                "pending": True,
                "abnormal": True,
            }
        else:
            # 来源不完整的登记不能拿来生成/归档：先落占位报告并由治理服务拦截流转。
            entry = {
                "id": report_id,
                "报告编号": facts.get("report_no", ""),
                "评估范围": facts.get("scope", ""),
                "合规指标": facts.get("metric", ""),
                "当前得分": facts.get("score", 0.0),
                "改进事项": f"来源事实不完整（{source.get('缺失事实', '')}），待补全后生成",
                "评估周期": facts.get("period", ""),
                "负责人": facts.get("owner", ""),
                "生成时间": facts.get("registered_at", "2026-11-17"),
                "status": "待生成",
                "pending": True,
                "abnormal": True,
            }
        # 来源关联写在治理报告上，报告详情可据此核对来源；不影响既有列表字段。
        entry["来源编号"] = source.get("来源编号", "")
        entry["动作标识"] = source.get("动作标识", "")
        gov_rows.append(entry)
        return entry

    # ---------- 回执找回 ----------

    def _find_linked_report(self, source: dict[str, Any]) -> dict[str, Any] | None:
        for report in store.rows("govern_report"):
            if report.get("来源编号") == source.get("来源编号"):
                return report
        # 兼容升级前登记：没有反向链接时按原报告编号定位。
        report_no = source.get("报告编号", "")
        for report in store.rows("govern_report"):
            if report.get("报告编号") == report_no:
                return report
        return None

    def confirm_receipt(self, act_id: str) -> dict[str, Any] | None:
        """按原动作标识补写回执并找回原报告，报告与归档状态保持不变。"""
        with self._lock:
            source = self.find_by_action(act_id)
            if source is None:
                return None
            source["回执状态"] = RECEIPT_DONE
            return {
                "source": source,
                "report": self._find_linked_report(source),
            }

    def receipt_view(self, act_id: str) -> dict[str, Any] | None:
        """只查动作回执，不改动任何状态；与报告来源核对相互独立。"""
        source = self.find_by_action(act_id)
        if source is None:
            return None
        return {
            "动作标识": act_id,
            "回执状态": source.get("回执状态"),
            "来源编号": source.get("来源编号"),
            "报告编号": source.get("报告编号"),
        }

    def is_incomplete_report(self, report: dict[str, Any]) -> bool:
        """治理报告生成/归档前的来源完整性闸门。"""
        source_no = report.get("来源编号")
        if not source_no:
            return False
        for source in store.rows(self.MODULE):
            if source.get("来源编号") == source_no:
                return source.get("来源状态") == SOURCE_INCOMPLETE
        return False


# 单例：重放队列与抽样校验复用同一个建档入口与同一把动作标识幂等锁。
collect_alert_service = CollectAlertService()
