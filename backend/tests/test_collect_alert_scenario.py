"""采集告警来源治理重构的场景化验证（仅用标准库，单服务进程内运行）。

覆盖任务书的全部验收点：
1. 重放耗尽/抽样偏差共用建档，同端点同时间不合并，各自保留原编号/范围/指标/得分/责任人；
2. 标记失败中断（报告已登记、回执未回）后的原动作标识重试与回执找回；
3. 同标识并发只写一份报告；来源不完整不能生成/归档；
4. 生成/归档动作与统计、归档版本输出不变，报告可核对来源；
5. 升级前报告可核对原始来源；同编号多条时详情标记来源待核并保留各自归档关系；
6. 监控告警与抽样重放待办仍由原动作产生。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.seed import SEED_ROWS
from app.store import store
from app.services import service_for
from app.services.collect_alert import (
    RECEIPT_DONE,
    RECEIPT_PENDING,
    SOURCE_INCOMPLETE,
    collect_alert_service,
)
from app.services.retry_queue import RetryQueueService
from app.services.sample_check import SampleCheckService
from app.services.govern_report import GovernReportService

ENDPOINT = "EP-E"
retry_svc = RetryQueueService()
sample_svc = SampleCheckService()
govern_svc = GovernReportService()

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name}" + (f" —— {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(name)


def reset_store() -> None:
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    collect_alert_service.auto_receipt = True


def find(rows: list[dict], **criteria) -> dict | None:
    for row in rows:
        if all(row.get(key) == value for key, value in criteria.items()):
            return row
    return None


def find_all(rows: list[dict], **criteria) -> list[dict]:
    return [row for row in rows if all(row.get(key) == value for key, value in criteria.items())]


# ---------- 主场景 ----------

reset_store()

# E 的监控指标（用于核对原监控告警动作）
mon_entry, missing = service_for("api_monitor").create_entry({
    "指标编号": "MON-E", "目标端点": ENDPOINT, "监控类型": "可用性",
})
check("监控指标已登记", mon_entry is not None)

# E 的重放任务
task, missing = retry_svc.create_entry({
    "任务编号": "RETR-E", "失败端点": ENDPOINT, "失败原因": "连接超时",
})
check("重放任务E已登记", task is not None and not missing, str(missing))
task_id = int(task["id"])
tid = str(task_id).zfill(4)
retry_report_no = f"GOVE-RETRY-{tid}"
retry_action_id = collect_alert_service.action_id("retry_queue", tid, "标记失败")

# 1) 标记失败，在“已登记未回执”窗口中断
collect_alert_service.auto_receipt = False
entry, message = retry_svc.run_action(task_id, "标记失败")
collect_alert_service.auto_receipt = True
check("重放任务已永久失败", entry is not None and entry["status"] == "永久失败")
retry_source = collect_alert_service.find_by_action(retry_action_id)
check("GOVE-RETRY 已登记未回执", retry_source is not None
      and retry_source["回执状态"] == RECEIPT_PENDING
      and retry_source["报告编号"] == retry_report_no)
retry_report = find(store.rows("govern_report"), 报告编号=retry_report_no)
check("回执丢失时报告仍可读", retry_report is not None and retry_report["status"] == "待生成")
check("重放报告字段与历史一致", retry_report is not None and all([
    retry_report["评估范围"] == f"重放队列永久失败: {ENDPOINT}",
    retry_report["合规指标"] == "数据采集完整性",
    retry_report["当前得分"] == 60.0,
    retry_report["负责人"] == "运维值班",
    retry_report["评估周期"] == "2026-Q4",
    retry_report["改进事项"] == f"任务{tid}重试耗尽，需人工介入",
]))
monitor = find(store.rows("api_monitor"), 目标端点=ENDPOINT)
check("原监控告警动作仍产生告警", monitor is not None and monitor["status"] == "已告警")

# 2) E 的抽样计划对同一端点标记告警（同登记时间，两种来源）
plan, missing = sample_svc.create_entry({
    "计划编号": "SAMP-E", "目标端点": ENDPOINT, "抽样比例": "20%",
    "校验字段": "amount", "偏差阈值": "5%",
})
check("抽样计划E已登记", plan is not None and not missing, str(missing))
plan_id = int(plan["id"])
sid = str(plan_id).zfill(4)
sample_report_no = f"GOVE-SAMP-{sid}"
_, msg2 = sample_svc.run_action(plan_id, "标记告警")
check("抽样计划已偏差告警", find(store.rows("sample_check"), id=plan_id)["status"] == "偏差告警")

sources = store.rows("collect_alert_source")
check("两种来源各自登记未合并", len(sources) == 2
      and {s["来源类型"] for s in sources} == {"retry_exhausted", "sample_deviation"})
check("同端点同时间不合并事件",
      len(find_all(sources, 目标端点=ENDPOINT, 登记时间="2026-11-17")) == 2)
sample_report = find(store.rows("govern_report"), 报告编号=sample_report_no)
check("抽样报告字段与历史一致", sample_report is not None and all([
    sample_report["评估范围"] == f"抽样偏差告警:{ENDPOINT}",
    sample_report["合规指标"] == "数据质量一致性",
    sample_report["当前得分"] == 55.0,
    sample_report["负责人"] == "数据质量组",
    sample_report["评估周期"] == "2026-Q4",
    sample_report["改进事项"] == f"端点{ENDPOINT}抽样偏差超出阈值5%",
]))
sample_task = find(store.rows("retry_queue"), 任务编号=f"RETR-SAMP-{sid}")
check("抽样重放待办仍由原动作产生",
      sample_task is not None and sample_task["status"] == "待重试")

# 3) 先归档重放报告（生成 -> 归档），归档发生在回执仍丢失时
_, m1 = govern_svc.run_action(int(retry_report["id"]), "生成报告")
check("重放报告生成成功", "已采集跨模块全景数据" in m1)
generated = find(store.rows("govern_report"), id=int(retry_report["id"]))
check("生成后报告主字段保留", generated["评估范围"] == f"重放队列永久失败: {ENDPOINT}"
      and generated["合规指标"] == "数据采集完整性"
      and generated["负责人"] == "运维值班")
check("生成动作原统计输出保留", isinstance(generated.get("stats"), dict)
      and set(generated["stats"]) == {"total_calls", "pending_retries",
                                      "active_alerts", "circuit_breaks", "pipeline_errors"})
_, m2 = govern_svc.run_action(int(retry_report["id"]), "归档报告")
check("重放报告归档成功", find(store.rows("govern_report"), id=int(retry_report["id"]))["status"] == "已归档")
archive_no = f"DATA-GOV-{str(retry_report['id']).zfill(4)}"
check("归档版本按原动作输出", find(store.rows("data_version"), 版本编号=archive_no) is not None)

# 4) 按原动作标识重试标记失败：找回原报告、补回执，不重复建档
gov_count_before = len(store.rows("govern_report"))
_, retry_msg = retry_svc.run_action(task_id, "标记失败")
check("重试提示回执找回", "找回原报告" in retry_msg, retry_msg)
check("重试未新增报告", len(store.rows("govern_report")) == gov_count_before)
check("回执已补写", collect_alert_service.find_by_action(retry_action_id)["回执状态"] == RECEIPT_DONE)
same_report = find(store.rows("govern_report"), 报告编号=retry_report_no)
check("找回的是原报告且归档状态保留",
      same_report["id"] == retry_report["id"] and same_report["status"] == "已归档")
check("重放报告只有一个归档版本",
      len(find_all(store.rows("data_version"), 版本编号=archive_no)) == 1)
check("来源记录仍只有两条", len(store.rows("collect_alert_source")) == 2)

# 5) 再生成抽样报告
_, m3 = govern_svc.run_action(int(sample_report["id"]), "生成报告")
check("抽样报告生成成功", "已生成" == find(store.rows("govern_report"), id=int(sample_report["id"]))["status"])

# 6) 报告详情核对来源（来源与回执分别检查）
retry_detail = govern_svc.get_entry(int(retry_report["id"]))
sample_detail = govern_svc.get_entry(int(sample_report["id"]))
check("重放报告详情可核对动作来源", retry_detail["来源核对"]["来源待核"] is False
      and retry_detail["来源核对"]["动作标识"] == retry_action_id
      and retry_detail["来源核对"]["来源模块"] == "retry_queue"
      and retry_detail["来源核对"]["回执状态"] == RECEIPT_DONE)
check("抽样报告详情可核对动作来源", sample_detail["来源核对"]["来源状态"] == "来源已核"
      and sample_detail["来源核对"]["来源类型"] == "sample_deviation")
receipt_view = collect_alert_service.receipt_view(retry_action_id)
check("回执可独立检查", receipt_view is not None and receipt_view["回执状态"] == RECEIPT_DONE)

# 7) 两个原编号并存、各自字段独立
nos = {r["报告编号"] for r in store.rows("govern_report")}
check("两个原编号均保留", {retry_report_no, sample_report_no}.issubset(nos))

# ---------- 并发同一动作标识 ----------

reset_store()
task, _ = retry_svc.create_entry({
    "任务编号": "RETR-C", "失败端点": "EP-C", "失败原因": "boom",
})
tid_c = str(task["id"]).zfill(4)
barrier = threading.Barrier(4)
errors: list[Exception] = []


def concurrent_mark() -> None:
    try:
        barrier.wait()
        retry_svc.run_action(int(task["id"]), "标记失败")
    except Exception as exc:  # pragma: no cover - 测试兜底
        errors.append(exc)


threads = [threading.Thread(target=concurrent_mark) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("并发提交无异常", not errors, str(errors))
act_c = collect_alert_service.action_id("retry_queue", tid_c, "标记失败")
check("同一标识只写入一份来源",
      len(find_all(store.rows("collect_alert_source"), 动作标识=act_c)) == 1)
check("同一标识只写入一份报告",
      len(find_all(store.rows("govern_report"), 报告编号=f"GOVE-RETRY-{tid_c}")) == 1)

# ---------- 回执丢失后同进程找回，保留归档状态 ----------

reset_store()
collect_alert_service.auto_receipt = False
task, _ = retry_svc.create_entry({
    "任务编号": "RETR-R", "失败端点": "EP-R", "失败原因": "boom",
})
tid_r = str(task["id"]).zfill(4)
retry_svc.run_action(int(task["id"]), "标记失败")
act_r = collect_alert_service.action_id("retry_queue", tid_r, "标记失败")
report_r = find(store.rows("govern_report"), 报告编号=f"GOVE-RETRY-{tid_r}")
govern_svc.run_action(int(report_r["id"]), "生成报告")
govern_svc.run_action(int(report_r["id"]), "归档报告")
check("找回前回执未回", collect_alert_service.receipt_view(act_r)["回执状态"] == RECEIPT_PENDING)
recovered = collect_alert_service.confirm_receipt(act_r)
check("同进程找回原报告", recovered is not None
      and recovered["report"]["id"] == report_r["id"])
check("找回后归档状态保留", recovered["report"]["status"] == "已归档"
      and len(find_all(store.rows("data_version"),
                       版本编号=f"DATA-GOV-{str(report_r['id']).zfill(4)}")) == 1)
check("找回后回执已回", collect_alert_service.receipt_view(act_r)["回执状态"] == RECEIPT_DONE)
check("未知标识找回返回空", collect_alert_service.confirm_receipt("retry_queue:9999:标记失败") is None)

# ---------- 来源不完整不能生成/归档 ----------

reset_store()
facts = collect_alert_service.build_retry_facts({
    "id": 42, "任务编号": "RETR-X", "失败端点": "",  # 端点缺失
})
result = collect_alert_service.register_alert(facts)
check("不完整来源已标记", result["source"]["来源状态"] == SOURCE_INCOMPLETE)
bad_report = result["report"]
blocked_gen, msg_gen = govern_svc.run_action(int(bad_report["id"]), "生成报告")
blocked_arc, msg_arc = govern_svc.run_action(int(bad_report["id"]), "归档报告")
check("不完整来源禁止生成报告", blocked_gen is None and "来源事实不完整" in msg_gen)
check("不完整来源禁止归档报告", blocked_arc is None and "来源事实不完整" in msg_arc)
check("被拦报告维持待生成", find(store.rows("govern_report"), id=int(bad_report["id"]))["status"] == "待生成")
check("完整来源仍可正常生成", collect_alert_service.is_incomplete_report(
    find(store.rows("govern_report"), 报告编号="GOVE-0001")) is False)

# ---------- 升级前报告 + 同编号多条来源待核 ----------

reset_store()
# 本次事件：有动作来源的重放报告
task, _ = retry_svc.create_entry({
    "任务编号": "RETR-H", "失败端点": "EP-H", "失败原因": "boom",
})
tid_h = str(task["id"]).zfill(4)
retry_svc.run_action(int(task["id"]), "标记失败")
new_no = f"GOVE-RETRY-{tid_h}"
new_report = find(store.rows("govern_report"), 报告编号=new_no)
govern_svc.run_action(int(new_report["id"]), "生成报告")
govern_svc.run_action(int(new_report["id"]), "归档报告")
new_archive = f"DATA-GOV-{str(new_report['id']).zfill(4)}"

# 历史报告：无动作标识，复用同一显示编号，带自己的原归档关联
gov_rows = store.rows("govern_report")
hist_id = max(int(r["id"]) for r in gov_rows) + 1
hist_archive = f"DATA-GOV-{str(hist_id).zfill(4)}"
gov_rows.append({
    "id": hist_id,
    "报告编号": new_no,  # 与本次事件同一显示编号
    "评估范围": f"历史重放报告: {ENDPOINT}",
    "合规指标": "数据采集完整性",
    "当前得分": 70.0,
    "改进事项": "历史登记",
    "评估周期": "2026-Q3",
    "负责人": "运维值班",
    "生成时间": "2026-09-30",
    "status": "已归档", "pending": False, "abnormal": False,
    # 无 来源编号 / 动作标识
})
store.rows("data_version").append({
    "id": max(int(r["id"]) for r in store.rows("data_version")) + 1,
    "版本编号": hist_archive,
    "数据来源": "govern_report",
    "行数统计": 1,
    "版本标签": "report_archived",
    "Schema摘要": "数据采集完整性",
    "创建原因": f"治理报告{hist_id}已归档",
    "依赖版本": "2026-Q3",
    "创建时间": "2026-09-30",
    "status": "可用", "pending": False, "abnormal": False,
})

same_no_reports = find_all(store.rows("govern_report"), 报告编号=new_no)
check("同编号存在两条报告", len(same_no_reports) == 2)
hist_detail = govern_svc.get_entry(hist_id)
new_detail = govern_svc.get_entry(int(new_report["id"]))
check("历史报告指出来源待核", hist_detail["来源核对"]["来源待核"] is True
      and hist_detail["来源核对"]["来源状态"] == "来源待核")
check("历史报告保留原归档关联", hist_archive in hist_detail["来源核对"]["归档版本"])
check("新报告核对动作来源", new_detail["来源核对"]["来源待核"] is False
      and new_detail["来源核对"]["动作标识"]
      == collect_alert_service.action_id("retry_queue", tid_h, "标记失败"))
check("新报告归档版本独立保留", new_archive in new_detail["来源核对"]["归档版本"]
      and hist_archive not in new_detail["来源核对"]["归档版本"])
check("两份归档版本均存在",
      find(store.rows("data_version"), 版本编号=hist_archive) is not None
      and find(store.rows("data_version"), 版本编号=new_archive) is not None)

# 升级前种子报告（无来源链接、编号唯一）仍可核对原始来源
seed_detail = govern_svc.get_entry(1)
check("升级前报告可核对原始来源",
      seed_detail["来源核对"]["来源状态"] == "升级前来源"
      and seed_detail["来源核对"]["来源待核"] is False)

# ---------- 既有统计与列表字段不变 ----------

reset_store()
retry_svc.create_entry({"任务编号": "RETR-S1", "失败端点": "EP-S", "失败原因": "x"})
coverage = sample_svc.calc_sampling_coverage()
check("抽样覆盖率统计保留", set(coverage) == {
    "total_plans", "active_sampling", "deviations", "version_snapshots", "govern_items"})
recovery = retry_svc.calc_recovery_rate()
check("重放恢复率统计保留", set(recovery) == {
    "total_retries", "success", "permanent_fail", "recovery_rate", "logged_events"})
check("治理报告列表字段不变", govern_svc.LIST_FIELDS == [
    "报告编号", "评估范围", "合规指标", "当前得分", "改进事项", "评估周期", "负责人", "生成时间"])
check("来源服务列表字段独立", service_for("collect_alert_source").LIST_FIELDS == [
    "来源编号", "来源类型", "动作标识", "来源模块", "来源记录编号",
    "目标端点", "报告编号", "登记时间", "来源状态", "回执状态"])

print()
if failures:
    print(f"失败 {len(failures)} 项：{failures}")
    sys.exit(1)
print("全部场景通过。")
