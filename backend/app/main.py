"""网关工坊 · API接口采集与数据网关平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store
from app.services import SERVICE_REGISTRY


app = FastAPI(title="网关工坊 · API接口采集与数据网关平台", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# ---------- 跨模块数据流链路查询接口 ----------

@app.get("/api/chain/sla-health/{provider_id}")
def chain_sla_health(provider_id: int):
    """链路1：API源注册→SLA健康检查（跨 api_provider/rate_limit/api_monitor/cost_account）"""
    from app.services.api_provider import ApiProviderService
    return ApiProviderService().check_sla_health(provider_id)


@app.get("/api/chain/auth-chain")
def chain_auth_chain(provider: str):
    """链路2：鉴权编排链路检查（跨 auth_profile/api_provider/endpoint_def/rate_limit）"""
    from app.services.auth_profile import AuthProfileService
    return AuthProfileService().check_auth_chain(provider)


@app.get("/api/chain/endpoint-chain")
def chain_endpoint_chain(endpoint_id: str):
    """链路3：端点定义全链路（跨 endpoint_def/rate_limit/response_parse/schema_diff）"""
    from app.services.endpoint_def import EndpointDefService
    return EndpointDefService().check_endpoint_chain(endpoint_id)


@app.get("/api/chain/rate-health")
def chain_rate_health(endpoint: str):
    """链路4：限流健康检查（跨 rate_limit/retry_queue/call_log/data_cache）"""
    from app.services.rate_limit import RateLimitService
    return RateLimitService().check_rate_health(endpoint)


@app.get("/api/chain/batch-throughput")
def chain_batch_throughput(endpoint_id: str):
    """链路5：请求批次吞吐量分析（跨 request_batch/call_log/retry_queue/data_cache）"""
    from app.services.request_batch import RequestBatchService
    return RequestBatchService().check_batch_throughput(endpoint_id)


@app.get("/api/chain/cache-hit-rate")
def chain_cache_hit_rate():
    """链路6：缓存命中率（跨 data_cache/data_delivery/subscriber）"""
    from app.services.data_cache import DataCacheService
    return DataCacheService().calc_cache_hit_rate()


@app.get("/api/chain/pipeline-health")
def chain_pipeline_health():
    """链路7：投递管道健康度（跨 data_delivery/subscriber/data_version/call_log）"""
    from app.services.data_delivery import DataDeliveryService
    return DataDeliveryService().calc_pipeline_health()


@app.get("/api/chain/recovery-rate")
def chain_recovery_rate():
    """链路8：重放恢复率（跨 retry_queue/call_log）"""
    from app.services.retry_queue import RetryQueueService
    return RetryQueueService().calc_recovery_rate()


@app.get("/api/chain/subscription-chain")
def chain_subscription_chain(topic: str):
    """链路9：订阅链路（跨 subscriber/webhook_router/data_version/data_cache/data_delivery）"""
    from app.services.subscriber import SubscriberService
    return SubscriberService().check_subscription_chain(topic)


@app.get("/api/chain/webhook-chain")
def chain_webhook_chain(event_type: str):
    """链路10：Webhook链路（跨 webhook_router/subscriber/pipeline_audit/call_log）"""
    from app.services.webhook_router import WebhookRouterService
    return WebhookRouterService().check_webhook_chain(event_type)


@app.get("/api/chain/chain-health")
def chain_chain_health():
    """链路11：全局链路健康监控（跨 api_monitor/call_log/retry_queue/rate_limit/govern_report）"""
    from app.services.api_monitor import ApiMonitorService
    return ApiMonitorService().analyze_chain_health()


@app.get("/api/chain/call-metrics")
def chain_call_metrics(endpoint: str):
    """链路12：调用指标统计（跨 call_log/retry_queue/api_monitor/rate_limit）"""
    from app.services.call_log import CallLogService
    return CallLogService().calc_call_metrics(endpoint)


@app.get("/api/chain/govern-score")
def chain_govern_score():
    """链路13：治理全景评分（跨 govern_report/pipeline_audit/data_version/call_log）"""
    from app.services.govern_report import GovernReportService
    return GovernReportService().calc_govern_score()


@app.get("/api/chain/schema-health")
def chain_schema_health():
    """链路14：Schema健康度（跨 schema_diff/endpoint_def/govern_report）"""
    from app.services.schema_diff import SchemaDiffService
    return SchemaDiffService().calc_schema_health()


@app.get("/api/chain/audit-trail")
def chain_audit_trail():
    """链路15：审计追踪（跨 pipeline_audit/govern_report/call_log）"""
    from app.services.pipeline_audit import PipelineAuditService
    return PipelineAuditService().calc_audit_trail()


@app.get("/api/chain/version-lineage")
def chain_version_lineage(source: str):
    """链路16：版本谱系（跨 data_version/data_delivery/subscriber）"""
    from app.services.data_version import DataVersionService
    return DataVersionService().check_version_lineage(source)


@app.get("/api/chain/incremental-health")
def chain_incremental_health(endpoint: str):
    """链路17：增量健康度（跨 incr_tracker/call_log/retry_queue）"""
    from app.services.incr_tracker import IncrTrackerService
    return IncrTrackerService().check_incremental_health(endpoint)


@app.get("/api/chain/sampling-coverage")
def chain_sampling_coverage():
    """链路18：抽样覆盖率（跨 sample_check/call_log/data_version）"""
    from app.services.sample_check import SampleCheckService
    return SampleCheckService().calc_sampling_coverage()


@app.get("/api/chain/cost-overview")
def chain_cost_overview():
    """链路19：成本全景（跨 cost_account/api_provider/call_log）"""
    from app.services.cost_account import CostAccountService
    return CostAccountService().calc_cost_overview()


@app.get("/api/chain/schedule-coverage")
def chain_schedule_coverage():
    """链路20：调度覆盖面（跨 task_schedule/request_batch/incr_tracker/data_version）"""
    from app.services.task_schedule import TaskScheduleService
    return TaskScheduleService().check_schedule_coverage()


@app.get("/api/chain/encrypt-coverage")
def chain_encrypt_coverage():
    """链路21：加密覆盖度（跨 data_encrypt/auth_profile/subscriber）"""
    from app.services.data_encrypt import DataEncryptService
    return DataEncryptService().check_encrypt_coverage()


@app.get("/api/chain/snapshot")
def chain_snapshot():
    """综合快照：一次性输出所有模块的 domain_snapshot 信息。"""
    results = {}
    for name, svc in SERVICE_REGISTRY.items():
        results[name] = svc.domain_snapshot()
    return {"modules": results, "total_modules": len(results)}


# ---------- 采集告警来源与动作回执（来源与回执分别检查） ----------

@app.get("/api/chain/alert-receipt/{action_id:path}")
def chain_alert_receipt(action_id: str):
    """按动作标识只查动作回执，不改动报告与归档状态。"""
    from app.services.collect_alert import collect_alert_service
    view = collect_alert_service.receipt_view(action_id)
    if view is None:
        raise HTTPException(status_code=404, detail=f"动作标识 {action_id} 无来源登记")
    return view


@app.post("/api/chain/alert-receipt/{action_id:path}/recover")
def chain_alert_receipt_recover(action_id: str):
    """回执丢失后在同一服务进程内按原动作标识找回原报告并补写回执。"""
    from app.services.collect_alert import collect_alert_service
    result = collect_alert_service.confirm_receipt(action_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"动作标识 {action_id} 无来源登记")
    report = result["report"]
    return {
        "ok": True,
        "source": result["source"],
        "report_id": report.get("id") if report else None,
        "报告编号": report.get("报告编号") if report else None,
        "报告状态": report.get("status") if report else None,
    }


@app.get("/api/chain/report-source/{report_id}")
def chain_report_source(report_id: int):
    """从选中的报告核对采集告警来源（含升级前来源与来源待核标记）。"""
    from app.services.govern_report import GovernReportService
    report = GovernReportService().get_entry(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"治理报告 {report_id} 不存在或已归档")
    return report


# 资源路由含 /api/{module}，必须放在 health / overview 及 chain 接口之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)