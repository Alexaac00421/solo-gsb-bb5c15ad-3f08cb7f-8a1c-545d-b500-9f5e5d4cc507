#!/usr/bin/env python3
"""现场断网采集包整包导入（POST /packages）最小示例。

设备现场断网后，把多台设备的登记信息（devices）、时钟周期（cycles）、
同步锚点（anchors）与声段（segments）汇成一个"采集包"，恢复网络后一次性
补传。服务保证：

  * 包内顺序不限——锚点可以乱序提交，声段可以前向引用同包登记的设备/周期；
  * 同一 package_id 的相同记录集合重传（网络重试）返回首次结果且不重复写入；
  * package_id 复用为不同内容、同标识记录与库中冲突、引用缺失、最终锚点
    序列非法、试图变更封存周期等情况整包拒绝、不留下部分数据。

本脚本自动演示：
  1) 首次导入（201，各类 added 计数与按新锚点计算的声段定位）；
  2) 原样/乱序/带重复项重试（200，idempotent_replay=true，计数与首次一致）；
  3) 整包拒绝：编号复用内容不同、引用缺失、锚点最终序列非法、封存周期变更；
  4) 已发布版本保持原快照，新发布版本才反映入库结果。

用法：
    1) 先启动服务：  python3 -m app
    2) 再运行本例：  python3 examples/package_example.py [base_url]
默认连接 http://127.0.0.1:8000，仅依赖标准库。
"""
from __future__ import annotations

import copy
import json
import sys
import urllib.error
import urllib.request


def call(base: str, method: str, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


def show(title: str, status: int, body: dict) -> None:
    print(f"\n=== {title} (HTTP {status}) ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))


def field_package():
    """模拟现场断网期两台设备汇成的采集包：包内顺序故意打乱。"""
    return {
        "package_id": "FIELD-20261006-01",
        # 声段在最前：引用同包稍后登记的设备与周期（前向引用允许）
        "segments": [
            {"capture_id": "FIELD-CAP-A", "device_id": "FIELD-D1",
             "cycle_id": "boot-2", "local_start_ms": 1000, "local_end_ms": 61000,
             "note": "黎明鸟鸣（第二轮开机）"},
            {"capture_id": "FIELD-CAP-B", "device_id": "FIELD-D2",
             "local_start_ms": 1000, "local_end_ms": 20000},
        ],
        # 锚点乱序提交（每周期按 local_ms 排序后校验严格递增）
        "anchors": [
            {"device_id": "FIELD-D1", "cycle_id": "boot-2",
             "local_ms": 61000, "utc_ms": 1700010060500},
            {"device_id": "FIELD-D2", "local_ms": 1000, "utc_ms": 1700000001000},
            {"device_id": "FIELD-D1", "cycle_id": "boot-2",
             "local_ms": 1000, "utc_ms": 1700010000000},
            {"device_id": "FIELD-D2", "local_ms": 20000, "utc_ms": 1700000020000},
        ],
        "cycles": [
            {"device_id": "FIELD-D1", "cycle_id": "boot-2", "note": "第二次开机"},
        ],
        "devices": [
            {"device_id": "FIELD-D1", "name": "现场录音笔-甲"},
            {"device_id": "FIELD-D2", "name": "现场录音笔-乙"},
        ],
    }


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"

    # 0) 导入前先发布一版（导入后该版本仍是原快照）
    call(base, "POST", "/versions", {"note": "现场采集包导入前"})

    # 1) 首次导入
    pkg = field_package()
    status, body = call(base, "POST", "/packages", pkg)
    show("1) 首次导入 POST /packages（201，包内顺序不限）", status,
         {"counts": body.get("counts"),
          "segments": body.get("segments"),
          "content_hash": body.get("content_hash")} if status < 300 else body)
    assert status == 201, body

    # 2) 网络超时后的重试：同一个包原样重发——返回首次结果，不重复写入
    status, replay = call(base, "POST", "/packages", copy.deepcopy(pkg))
    print("\n2) 原样重试:", status, "idempotent_replay =",
          replay.get("idempotent_replay"), "| 计数:",
          json.dumps(replay.get("counts"), ensure_ascii=False))

    # 2b) 即使重试时 JSON 顺序变了、夹带重复记录，也视为同一个记录集合
    retried = copy.deepcopy(pkg)
    retried["segments"].reverse()
    retried["anchors"].insert(0, dict(pkg["anchors"][0]))  # 追加一条完全相同的锚点
    retried["devices"].reverse()
    status, replay2 = call(base, "POST", "/packages", retried)
    print("    乱序+重复项重试:", status, "idempotent_replay =",
          replay2.get("idempotent_replay"),
          "| created_at 与首次一致:",
          replay2.get("created_at") == body.get("created_at"))
    assert status == 200 and replay2["counts"] == body["counts"]

    # 3) 整包拒绝场景（均不留下部分数据）
    # 3a) 包编号复用但内容不同
    diff = copy.deepcopy(pkg)
    diff["anchors"][0]["utc_ms"] += 999
    status, rejected = call(base, "POST", "/packages", diff)
    print("\n3a) 编号复用内容不同:", status, rejected["error"]["code"])
    assert status == 409 and rejected["error"]["code"] == "package_conflict"

    # 3b) 引用缺失（锚点引用未登记、且不在同包登记的设备）
    status, rejected = call(base, "POST", "/packages", {
        "package_id": "FIELD-20261006-02",
        "anchors": [{"device_id": "FIELD-GHOST", "local_ms": 0, "utc_ms": 0}]})
    print("3b) 引用缺失:", status, rejected["error"]["code"])
    assert status == 404 and rejected["error"]["code"] == "package_reference_missing"

    # 3c) 合并库内锚点后最终序列非法：
    #     FIELD-D2 现有 (1000,1700000001000)-(20000,1700000020000)，
    #     在 local=10000 插入 utc=1700000999999 会破坏 UTC 严格递增
    status, rejected = call(base, "POST", "/packages", {
        "package_id": "FIELD-20261006-03",
        "anchors": [{"device_id": "FIELD-D2", "local_ms": 10000,
                     "utc_ms": 1700000999999}]})
    print("3c) 最终锚点序列非法:", status, rejected["error"]["code"])
    assert status == 400 and rejected["error"]["code"] == "anchors_not_increasing"

    # 3d) 封存后试图通过采集包变更该周期
    _, detail = call(base, "GET", "/devices/FIELD-D2/cycles/default")
    fp = detail["cycle"]["fingerprint"]
    call(base, "POST", "/devices/FIELD-D2/cycles/default/seal",
         {"fingerprint": fp})
    status, rejected = call(base, "POST", "/packages", {
        "package_id": "FIELD-20261006-04",
        "segments": [{"capture_id": "FIELD-CAP-SEALED", "device_id": "FIELD-D2",
                      "local_start_ms": 2000, "local_end_ms": 3000}]})
    print("3d) 变更已封存周期:", status, rejected["error"]["code"])
    assert status == 409 and rejected["error"]["code"] == "cycle_sealed"

    # 4) 版本：旧版快照不变；新发布版本反映整包入库结果
    _, vnew = call(base, "POST", "/versions", {"note": "现场采集包入库后"})
    added = {s["capture_id"] for s in vnew["changes"]["added"]}
    print("\n4) 新版本 changes.added =", sorted(added))
    assert added == {"FIELD-CAP-A", "FIELD-CAP-B"}

    status, lst = call(base, "GET", "/packages")
    print("\n已接收采集包数量:", lst["count"],
          "（全部拒绝的包均未落盘，不留半包/垃圾记录）")
    print("\n完成：可执行 GET /timeline/current 查看当前时间轴，"
          "GET /packages/FIELD-20261006-01 查看首次导入结果。")


if __name__ == "__main__":
    main()
