"""治理报告领域服务：管理数据质量与合规报告，与 pipeline_audit、data_version、call_log、api_monitor 联动。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.collect_alert import collect_alert_service
from app.store import store


class GovernReportService(BaseModuleService):
    MODULE = "govern_report"
    ENTITY = "治理报告"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["报告编号", "评估范围", "合规指标", "当前得分", "改进事项", "评估周期", "负责人", "生成时间"]
    REQUIRED_FIELDS = ["报告编号", "评估范围", "合规指标"]
    STATUS_ORDER = ["待生成", "已生成", "已审核", "已归档"]
    ACTION_RULES = {"生成报告": "已生成", "提交审核": "已审核", "归档报告": "已归档"}
    NEGATIVE_ACTIONS = []

    def get_entry(self, entry_id: int) -> dict | None:
        """报告详情附带来源核对信息；升级前登记的报告仍可核对原始来源。"""
        report = super().get_entry(entry_id)
        if report is None:
            return None
        report = dict(report)
        report["来源核对"] = self._source_verification(report)
        return report

    def run_action(self, entry_id: int, action: str):
        """生成报告时收集跨模块数据；审核时联动管道审计和调用日志。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is not None and action in ("生成报告", "归档报告"):
            # 来源不完整的登记不能拿来生成或归档报告。
            if collect_alert_service.is_incomplete_report(entry):
                return None, (
                    f"治理报告 {entry_id} 来源事实不完整，"
                    "不能生成或归档，请先补全采集告警来源"
                )
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "生成报告":
            self._collect_cross_module_data(entry)
            message += "，已采集跨模块全景数据"
        elif action == "提交审核":
            self._audit_report(entry)
            message += "，已记录审计并写入调用日志"
        elif action == "归档报告":
            self._snapshot_data_version(entry)
            message += "，已创建数据版本快照"
        return entry, message

    def _source_verification(self, report: dict) -> dict:
        """核对报告的采集告警来源，区分动作来源、升级前来源与来源待核。"""
        report_no = str(report.get("报告编号", ""))
        same_no = [
            row for row in store.rows(self.MODULE)
            if str(row.get("报告编号", "")) == report_no
        ]
        source_no = report.get("来源编号")
        source = None
        for row in store.rows(collect_alert_service.MODULE):
            if source_no and row.get("来源编号") == source_no:
                source = row
                break
        if source is None and not report.get("动作标识") and len(same_no) == 1:
            # 升级前登记的报告没有来源链接：编号唯一时按原报告编号反查升级后来源；
            # 同编号有多条报告时不做归属，保持来源待核。
            for row in store.rows(collect_alert_service.MODULE):
                if row.get("报告编号") == report_no:
                    source = row
                    break

        archive_versions = [
            ver for ver in store.rows("data_version")
            if ver.get("版本编号") == f"DATA-GOV-{str(report.get('id', 0)).zfill(4)}"
        ]
        base = {
            "报告编号": report_no,
            "同编号报告数": len(same_no),
            "归档版本": [ver.get("版本编号") for ver in archive_versions],
        }
        if source is not None:
            base.update({
                "来源状态": "来源已核",
                "来源待核": False,
                "来源编号": source.get("来源编号"),
                "来源类型": source.get("来源类型"),
                "动作标识": source.get("动作标识"),
                "来源模块": source.get("来源模块"),
                "来源记录编号": source.get("来源记录编号"),
                "目标端点": source.get("目标端点"),
                "登记时间": source.get("登记时间"),
                "回执状态": source.get("回执状态"),
                "来源完整性": source.get("来源状态"),
                "核对说明": "可核对采集告警动作来源",
            })
            return base

        # 只有编号线索且同编号有多条：详情须指出来源待核，保留每条原有归档关系。
        ambiguous = len(same_no) > 1
        base.update({
            "来源状态": "来源待核" if ambiguous else "升级前来源",
            "来源待核": ambiguous,
            "来源编号": None,
            "动作标识": None,
            "核对说明": (
                "同编号存在多条报告且缺少动作来源记录，来源待核，归档关系按各报告原记录保留"
                if ambiguous else "升级前登记的报告，按报告原始字段核对来源"
            ),
        })
        return base

    def _collect_cross_module_data(self, report: dict):
        """生成报告时采集跨模块统计。"""
        total_calls = len(store.rows("call_log"))
        pending_retries = sum(1 for r in store.rows("retry_queue") if r.get("status") == "待重试")
        alerts = sum(1 for m in store.rows("api_monitor") if m.get("status") == "已告警")
        broken = sum(1 for rl in store.rows("rate_limit") if rl.get("status") == "已熔断")
        pipeline_errors = sum(1 for d in store.rows("data_delivery") if d.get("status") == "投递异常")
        # 基于统计计算综合得分
        health = 100.0
        health -= 5 * pending_retries
        health -= 10 * alerts
        health -= 15 * broken
        health -= 10 * pipeline_errors
        report["当前得分"] = max(0.0, health)
        report["改进事项"] = (
            f"待重试{pending_retries}项/告警{alerts}条/"
            f"熔断{broken}路/投递异常{pipeline_errors}路"
        )
        report["stats"] = {
            "total_calls": total_calls,
            "pending_retries": pending_retries,
            "active_alerts": alerts,
            "circuit_breaks": broken,
            "pipeline_errors": pipeline_errors,
        }

    def _audit_report(self, report: dict):
        """审核报告时写审计日志和调用日志。"""
        rid = str(report["id"]).zfill(4)
        # 管道审计
        audit_rows = store.rows("pipeline_audit")
        audit_rows.append({
            "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
            "记录编号": f"PIPE-GOV-{rid}",
            "管道名称": f"govern_report:{rid}",
            "操作人": report.get("负责人", "system"),
            "操作类型": "审核",
            "变更摘要": f"治理报告{rid}提交审核",
            "操作时间": "2026-11-17",
            "来源节点": "govern_report",
            "审计结论": "待复核",
            "status": "待复核", "pending": True, "abnormal": False,
        })
        # 调用日志
        log_rows = store.rows("call_log")
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-GOV-{rid}",
            "接口端点": "govern_report",
            "请求参数": report.get("评估范围", ""),
            "响应状态": "200",
            "响应耗时(ms)": 200,
            "返回行数": 1,
            "错误信息": "",
            "调用时间": "2026-11-17",
            "status": "成功", "pending": False, "abnormal": False,
        })

    def _snapshot_data_version(self, report: dict):
        """归档时创建数据版本快照。"""
        ver_rows = store.rows("data_version")
        rid = str(report["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-GOV-{rid}",
            "数据来源": "govern_report",
            "行数统计": 1,
            "版本标签": "report_archived",
            "Schema摘要": report.get("合规指标", ""),
            "创建原因": f"治理报告{rid}已归档",
            "依赖版本": report.get("评估周期", ""),
            "创建时间": "2026-11-17",
            "status": "可用", "pending": False, "abnormal": False,
        })

    def calc_govern_score(self) -> dict:
        """跨模块治理全景评分。"""
        return {
            "reports_total": len(store.rows("govern_report")),
            "reports_pending": sum(1 for r in store.rows("govern_report") if r.get("status") == "待生成"),
            "audits_open": sum(1 for a in store.rows("pipeline_audit") if a.get("status") == "待复核"),
            "data_versions": len(store.rows("data_version")),
            "call_anomalies": sum(1 for c in store.rows("call_log") if c.get("abnormal")),
        }