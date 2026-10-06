"""端到端测试：现场断网采集包整包导入（packages，v1.8）。

设备断网后把多台设备的登记信息、时钟周期、同步锚点与声段汇成采集包补传。
覆盖：

- 成功路径：包内顺序不限（锚点乱序、声段前向引用同包设备/周期）、同内容
  记录去重、各类记录 added/reused 计数、线性漂移定位、隐式 default 周期；
- 幂等：相同包编号相同记录集合重传返回首次结果（200，idempotent_replay）
  且不重复写入；记录顺序/重复项不影响内容指纹；
- 整包拒绝且不留部分数据：编号复用内容不同 / 同标识记录与库中冲突
  （device/cycle/anchor/capture）/ 包内同标识异内容 / 引用缺失 /
  锚点最终序列非法 / 起止倒置 / 非法字段 / 试图变更封存周期；
- 封存周期：同内容复用锚点/声段允许通过（沿用既有幂等规则），新增拒绝；
- 当前时间轴反映入库结果；已发布版本仍是原快照；
- 并发：导入与发布互相串行，观察不到半包状态；同包编号并发重传一致；
- 既有单条录入、周期封存、版本发布行为不受影响。

运行：python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db, server  # noqa: E402


def request(base, method, path, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


class PackagesApiTestCase(unittest.TestCase):
    server = None
    base = None
    tmpdir = None

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.server = server.make_server("127.0.0.1", 0, ":memory:")
        cls.port = cls.server.server_address[1]
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmpdir.cleanup()

    def setUp(self):
        self.db_path = os.path.join(self.tmpdir.name, f"{self.id().split('.')[-1]}.db")
        db.init_db(self.db_path)
        self.server.ctx.db_path = self.db_path

    def req(self, method, path, body=None):
        return request(self.base, method, path, body)

    # ------------------------------------------------------------ 工具

    def make_device(self, device_id="D1", name=None):
        payload = {"device_id": device_id}
        if name is not None:
            payload["name"] = name
        return self.req("POST", "/devices", payload)

    def reg_cycle(self, device_id, cycle_id, note=None):
        payload = {"cycle_id": cycle_id}
        if note is not None:
            payload["note"] = note
        return self.req("POST", f"/devices/{device_id}/cycles", payload)

    def add_anchor(self, device_id, local_ms, utc_ms, cycle_id=None):
        payload = {"local_ms": local_ms, "utc_ms": utc_ms}
        if cycle_id is not None:
            payload["cycle_id"] = cycle_id
        return self.req("POST", f"/devices/{device_id}/anchors", payload)

    def submit(self, capture_id, device_id, start, end, cycle_id=None, note=None):
        payload = {"capture_id": capture_id, "device_id": device_id,
                   "local_start_ms": start, "local_end_ms": end}
        if cycle_id is not None:
            payload["cycle_id"] = cycle_id
        if note is not None:
            payload["note"] = note
        return self.req("POST", "/segments", payload)

    def fingerprint(self, device_id, cycle_id="default"):
        _, body = self.req("GET", f"/devices/{device_id}/cycles/{cycle_id}")
        return body["cycle"]["fingerprint"]

    def seal(self, device_id, cycle_id="default"):
        fp = self.fingerprint(device_id, cycle_id)
        status, body = self.req("POST", f"/devices/{device_id}/cycles/{cycle_id}/seal",
                                {"fingerprint": fp})
        self.assertIn(status, (200, 201))
        return body

    def sample_package(self, package_id="PKG-1"):
        """两台设备（D1/D2），各 default + boot-2 周期，共 4 锚点 2 声段。

        故意把 sections 顺序排成语义依赖倒置：segments 在最前、devices 在最后，
        anchors 乱序，验证"包内顺序不限、可前向引用同包登记"。
        """
        return {
            "package_id": package_id,
            "segments": [
                {"capture_id": "CAP-A", "device_id": "D1", "cycle_id": "boot-2",
                 "local_start_ms": 1000, "local_end_ms": 61000, "note": "甲"},
                {"capture_id": "CAP-B", "device_id": "D2",
                 "local_start_ms": 1000, "local_end_ms": 2000},
            ],
            "cycles": [
                {"device_id": "D1", "cycle_id": "boot-2", "note": "甲第二次开机"},
                {"device_id": "D2", "cycle_id": "boot-2", "note": "乙第二次开机"},
            ],
            "anchors": [
                {"device_id": "D1", "cycle_id": "boot-2", "local_ms": 61000,
                 "utc_ms": 1700010060500},
                {"device_id": "D1", "cycle_id": "boot-2", "local_ms": 1000,
                 "utc_ms": 1700010000000},
                {"device_id": "D2", "local_ms": 2000, "utc_ms": 1700000002000},
                {"device_id": "D2", "local_ms": 1000, "utc_ms": 1700000001000},
            ],
            "devices": [
                {"device_id": "D1", "name": "录音笔-甲"},
                {"device_id": "D2", "name": "录音笔-乙"},
            ],
        }

    def import_pkg(self, package=None):
        return self.req("POST", "/packages", package or self.sample_package())

    # ------------------------------------------------------------ 成功路径

    def test_import_success_order_independent_and_counts(self):
        status, body = self.import_pkg()
        self.assertEqual(status, 201, body)
        self.assertFalse(body["idempotent_replay"])
        self.assertEqual(body["counts"], {
            "devices": {"added": 2, "reused": 0},
            # 每台新设备隐式 default（2）+ 两个显式 boot-2（2）
            "cycles": {"added": 4, "reused": 0},
            "anchors": {"added": 4, "reused": 0},
            "segments": {"added": 2, "reused": 0}})
        projs = {s["capture_id"]: s for s in body["segments"]}
        # 线性漂移映射：D2 速率 1.0；D1 boot-2 速率 1.008333…
        self.assertEqual(projs["CAP-B"]["status"], "located")
        self.assertEqual(projs["CAP-B"]["utc_start_ms"], 1700000001000)
        self.assertEqual(projs["CAP-B"]["utc_end_ms"], 1700000002000)
        self.assertEqual(projs["CAP-A"]["cycle_id"], "boot-2")
        self.assertEqual(projs["CAP-A"]["utc_start_ms"], 1700010000000)
        self.assertEqual(projs["CAP-A"]["utc_end_ms"], 1700010060500)
        # 排序：可定位声段按 (UTC 起点, 设备, 采集编号)
        self.assertEqual([s["capture_id"] for s in body["segments"]],
                         ["CAP-B", "CAP-A"])
        # 单条查询/列表同样可见
        st, b = self.req("GET", "/segments/CAP-A")
        self.assertEqual(st, 200)
        self.assertEqual(b["segment"]["device_id"], "D1")
        st, b = self.req("GET", "/devices/D1/cycles")
        self.assertEqual(st, 200)
        self.assertEqual({c["cycle_id"] for c in b["cycles"]},
                         {"default", "boot-2"})

    def test_import_reuse_existing_records(self):
        """库中已有的同标识同内容记录计为复用，不重复写入。"""
        self.make_device("D1", "录音笔-甲")
        self.reg_cycle("D1", "boot-2", "甲第二次开机")
        self.add_anchor("D1", 61000, 1700010060500, "boot-2")
        self.add_anchor("D1", 1000, 1700010000000, "boot-2")
        status, body = self.import_pkg()
        self.assertEqual(status, 201, body)
        self.assertEqual(body["counts"]["devices"], {"added": 1, "reused": 1})
        self.assertEqual(body["counts"]["cycles"], {"added": 2, "reused": 1})
        self.assertEqual(body["counts"]["anchors"], {"added": 2, "reused": 2})
        self.assertEqual(body["counts"]["segments"], {"added": 2, "reused": 0})

    def test_import_anchor_interleaved_with_existing_chain(self):
        """包内锚点插入库内现有锚点之间，按合并后的最终序列校验，顺序合法即通过。"""
        self.make_device("D3")
        self.add_anchor("D3", 1000, 10000)
        self.add_anchor("D3", 3000, 30000)
        status, body = self.req("POST", "/packages", {
            "package_id": "PKG-X",
            "anchors": [
                # 乱序提交：2000 插在既有 1000/3000 之间，UTC 10000<20000<30000 合法
                {"device_id": "D3", "local_ms": 3000, "utc_ms": 30000},
                {"device_id": "D3", "local_ms": 2000, "utc_ms": 20000},
                {"device_id": "D3", "local_ms": 1000, "utc_ms": 10000}]})
        self.assertEqual(status, 201, body)
        self.assertEqual(body["counts"]["anchors"], {"added": 1, "reused": 2})

    def test_import_unlocatable_segment_allowed(self):
        """锚点覆盖不足的声段入库即为 unlocatable（不阻塞整包）。"""
        status, body = self.req("POST", "/packages", {
            "package_id": "PKG-U",
            "devices": [{"device_id": "DU"}],
            "anchors": [{"device_id": "DU", "local_ms": 0, "utc_ms": 0}],
            "segments": [{"capture_id": "CAP-U", "device_id": "DU",
                          "local_start_ms": 5000, "local_end_ms": 6000}]})
        self.assertEqual(status, 201, body)
        (seg,) = body["segments"]
        self.assertEqual(seg["status"], "unlocatable")
        self.assertIsNone(seg["utc_start_ms"])

    def test_import_forward_reference_to_package_device(self):
        """锚点/声段可引用同包登记的设备与其隐式 default 周期（devices 段在最后）。"""
        status, body = self.req("POST", "/packages", {
            "package_id": "PKG-F",
            "segments": [{"capture_id": "CAP-F", "device_id": "DF",
                          "local_start_ms": 0, "local_end_ms": 10}],
            "anchors": [{"device_id": "DF", "local_ms": 0, "utc_ms": 1000},
                        {"device_id": "DF", "local_ms": 10, "utc_ms": 1010}],
            "devices": [{"device_id": "DF"}]})
        self.assertEqual(status, 201, body)
        self.assertEqual(body["counts"]["devices"]["added"], 1)
        self.assertEqual(body["counts"]["cycles"]["added"], 1)  # 仅隐式 default

    # ------------------------------------------------------------ 幂等

    def test_replay_same_records_returns_first_result(self):
        st1, b1 = self.import_pkg()
        self.assertEqual(st1, 201)
        # 重传：打乱 sections 与列表顺序、追加完全相同的重复记录
        pkg = self.sample_package()
        pkg["segments"] = list(reversed(pkg["segments"]))
        pkg["anchors"] = [pkg["anchors"][2], pkg["anchors"][3],
                          pkg["anchors"][0], pkg["anchors"][1]]
        pkg["devices"].append({"device_id": "D1", "name": "录音笔-甲"})
        st2, b2 = self.req("POST", "/packages", pkg)
        self.assertEqual(st2, 200)
        self.assertTrue(b2["idempotent_replay"])
        self.assertEqual(b2["counts"], b1["counts"])
        self.assertEqual(b2["created_at"], b1["created_at"])
        self.assertEqual(b2["content_hash"], b1["content_hash"])
        # 库里没有重复数据
        _, tl = self.req("GET", "/timeline/current")
        self.assertEqual(tl["segment_count"], 2)
        _, ancs = self.req("GET", "/devices/D1/anchors?cycle_id=boot-2")
        self.assertEqual(len(ancs["anchors"]), 2)
        _, lst = self.req("GET", "/packages")
        self.assertEqual(lst["count"], 1)

    def test_concurrent_replay_same_package(self):
        """同一包编号并发提交相同集合：只有一个 201，其余 200 且结果一致。"""
        results = []

        def fire():
            results.append(self.req("POST", "/packages", self.sample_package("PKG-C")))

        threads = [threading.Thread(target=fire) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        statuses = sorted(s for s, _ in results)
        self.assertEqual(statuses.count(201), 1)
        self.assertEqual(statuses.count(200), 5)
        bodies = [b for _, b in results]
        self.assertEqual(len({b["content_hash"] for b in bodies}), 1)
        _, tl = self.req("GET", "/timeline/current")
        self.assertEqual(tl["segment_count"], 2)

    # ------------------------------------------------------------ 整包拒绝

    def _assert_db_unchanged(self, device_ids=()):
        _, tl = self.req("GET", "/timeline/current")
        self.assertEqual(tl["segment_count"], 0)
        for d in device_ids:
            st, _ = self.req("GET", f"/devices/{d}")
            self.assertEqual(st, 404)

    def test_reject_package_id_reuse_different_content(self):
        st1, _ = self.import_pkg()
        self.assertEqual(st1, 201)
        pkg = self.sample_package()
        pkg["anchors"][0]["utc_ms"] = 1700010060999
        st2, b2 = self.req("POST", "/packages", pkg)
        self.assertEqual(st2, 409)
        self.assertEqual(b2["error"]["code"], "package_conflict")
        # 首次结果不变
        _, gp = self.req("GET", "/packages/PKG-1")
        self.assertEqual(gp["package"]["counts"]["anchors"]["added"], 4)

    def test_reject_device_cycle_anchor_capture_conflicts_with_db(self):
        # 设备 name 冲突
        self.make_device("D1", "完全不同的名字")
        st, b = self.import_pkg()
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "device_conflict")

        # 周期 note 冲突
        self.make_device("DX")
        self.reg_cycle("DX", "boot-2", "别的备注")
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-E1",
            "devices": [{"device_id": "DZ"}],
            "cycles": [{"device_id": "DX", "cycle_id": "boot-2", "note": "新备注"}]})
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "cycle_conflict")

        # 锚点 utc_ms 冲突
        self.make_device("DA")
        self.add_anchor("DA", 1000, 11111)
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-E2",
            "anchors": [{"device_id": "DA", "local_ms": 1000, "utc_ms": 22222}]})
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "anchor_conflict")
        self.assertEqual(b["error"]["details"]["anchors"][0]["existing_utc_ms"], 11111)

        # 声段异内容冲突
        self.make_device("DB")
        self.submit("CAP-Z", "DB", 1, 2)
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-E3",
            "anchors": [{"device_id": "DB", "local_ms": 0, "utc_ms": 0},
                        {"device_id": "DB", "local_ms": 9, "utc_ms": 9}],
            "segments": [{"capture_id": "CAP-Z", "device_id": "DB",
                          "local_start_ms": 3, "local_end_ms": 4}]})
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "capture_conflict")

    def test_reject_intra_package_duplicate_conflicts(self):
        """包内同标识异内容记录 -> 整包拒绝（设备/周期/锚点/声段各一种）。"""
        cases = [
            ("PKG-I1", {"devices": [{"device_id": "DI", "name": "甲"},
                                    {"device_id": "DI", "name": "乙"}]},
             "device_conflict"),
            ("PKG-I2", {"devices": [{"device_id": "DI"}],
                        "cycles": [{"device_id": "DI", "cycle_id": "c", "note": "n1"},
                                   {"device_id": "DI", "cycle_id": "c", "note": "n2"}]},
             "cycle_conflict"),
            ("PKG-I3", {"devices": [{"device_id": "DI"}],
                        "anchors": [{"device_id": "DI", "local_ms": 1, "utc_ms": 10},
                                    {"device_id": "DI", "local_ms": 1, "utc_ms": 20}]},
             "anchor_conflict"),
            ("PKG-I4", {"devices": [{"device_id": "DI"}],
                        "segments": [{"capture_id": "CI", "device_id": "DI",
                                      "local_start_ms": 1, "local_end_ms": 2},
                                     {"capture_id": "CI", "device_id": "DI",
                                      "local_start_ms": 3, "local_end_ms": 4,
                                      "note": "不同"}]},
             "capture_conflict"),
        ]
        for pid, pkg, code in cases:
            pkg["package_id"] = pid
            st, b = self.req("POST", "/packages", pkg)
            self.assertEqual(st, 409, (pid, b))
            self.assertEqual(b["error"]["code"], code, pid)
        # 任何一个被拒包都没有留下数据
        st, _ = self.req("GET", "/devices/DI")
        self.assertEqual(st, 404)
        _, lst = self.req("GET", "/packages")
        self.assertEqual(lst["count"], 0)

    def test_reject_missing_references(self):
        # 引用不存在的设备
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-M1",
            "anchors": [{"device_id": "GHOST", "local_ms": 0, "utc_ms": 0}]})
        self.assertEqual(st, 404)
        self.assertEqual(b["error"]["code"], "package_reference_missing")
        # 引用不存在的周期
        self.make_device("DM")
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-M2",
            "segments": [{"capture_id": "CM", "device_id": "DM", "cycle_id": "nope",
                          "local_start_ms": 0, "local_end_ms": 1}]})
        self.assertEqual(st, 404)
        self.assertEqual(b["error"]["code"], "package_reference_missing")
        self.assertTrue(any(x["kind"] == "cycle" for x in b["error"]["details"]["missing"]))
        self._assert_db_unchanged()

    def test_reject_illegal_final_anchor_sequence(self):
        """合并库内锚点后最终序列非法（UTC 非严格递增）-> 400，整包回滚。"""
        self.make_device("DN")
        self.add_anchor("DN", 1000, 10000)
        self.add_anchor("DN", 3000, 30000)
        # 2000 插在中间但 utc=40000 > 后锚点 30000 -> 最终序列非单调
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-N",
            "anchors": [{"device_id": "DN", "local_ms": 2000, "utc_ms": 40000}]})
        self.assertEqual(st, 400)
        self.assertEqual(b["error"]["code"], "anchors_not_increasing")
        # 库内仍只有原来的两个锚点
        _, ancs = self.req("GET", "/devices/DN/anchors")
        self.assertEqual([a["local_ms"] for a in ancs["anchors"]], [1000, 3000])

    def test_reject_segment_inverted(self):
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-INV",
            "devices": [{"device_id": "DV"}],
            "segments": [{"capture_id": "CV", "device_id": "DV",
                          "local_start_ms": 50, "local_end_ms": 10}]})
        self.assertEqual(st, 400)
        self.assertEqual(b["error"]["code"], "segment_inverted")
        self._assert_db_unchanged(device_ids=("DV",))

    def test_reject_invalid_fields_and_shapes(self):
        bad = [
            ("PKG-B1", {}, "validation_error"),                       # 缺 package_id
            ("PKG-B2", {"package_id": "PKG-B2"}, "validation_error"), # 空包
            ("PKG-B3", {"package_id": "PKG-B3",
                        "devices": "x"}, "validation_error"),         # 非数组
            ("PKG-B4", {"package_id": "PKG-B4",
                        "devices": [{"name": "x"}]}, "validation_error"),  # 缺字段
            ("PKG-B5", {"package_id": "PKG-B5",
                        "devices": [{"device_id": "bad id!"}]}, "validation_error"),
            ("PKG-B6", {"package_id": "PKG-B6",
                        "anchors": [{"device_id": "D", "local_ms": -1,
                                     "utc_ms": 1}]}, "invalid_time"),
            ("PKG-B7", {"package_id": "PKG-B7",
                        "anchors": [{"device_id": "D", "local_ms": 1.5,
                                     "utc_ms": 1}]}, "invalid_time"),
            ("PKG-B8", {"package_id": "PKG-B8",
                        "segments": [{"capture_id": "C", "device_id": "D",
                                      "local_start_ms": 0,
                                      "local_end_ms": 2 ** 63}]}, "invalid_time"),
        ]
        for pid, pkg, code in bad:
            pkg.setdefault("package_id", pid)
            st, b = self.req("POST", "/packages", pkg)
            self.assertEqual(st, 400, (pid, b))
            self.assertEqual(b["error"]["code"], code, pid)

    def test_reject_changes_to_sealed_cycle(self):
        """已封存周期：新增锚点/声段 -> 409 cycle_sealed；同内容复用允许。"""
        self.make_device("DS")
        self.add_anchor("DS", 0, 0)
        self.add_anchor("DS", 100, 100)
        self.submit("CAP-S", "DS", 10, 20)
        self.seal("DS", "default")

        # 新增锚点 -> 拒绝
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-S1",
            "anchors": [{"device_id": "DS", "local_ms": 200, "utc_ms": 200}]})
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "cycle_sealed")
        # 新增声段 -> 拒绝
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-S2",
            "segments": [{"capture_id": "CAP-S2", "device_id": "DS",
                          "local_start_ms": 30, "local_end_ms": 40}]})
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "cycle_sealed")

        # 同内容锚点/声段复用：允许通过且不改动封存内容
        fp_before = self.fingerprint("DS")
        st, body = self.req("POST", "/packages", {
            "package_id": "PKG-S3",
            "anchors": [{"device_id": "DS", "local_ms": 0, "utc_ms": 0},
                        {"device_id": "DS", "local_ms": 100, "utc_ms": 100}],
            "segments": [{"capture_id": "CAP-S", "device_id": "DS",
                          "local_start_ms": 10, "local_end_ms": 20}]})
        self.assertEqual(st, 201, body)
        self.assertEqual(body["counts"]["anchors"]["reused"], 2)
        self.assertEqual(body["counts"]["segments"]["reused"], 1)
        self.assertEqual(self.fingerprint("DS"), fp_before)

    def test_reject_new_device_default_cycle_note_conflict(self):
        """新设备隐式 default 的 note 固定为 null；显式 default 带非空 note 即冲突。"""
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-DC",
            "devices": [{"device_id": "DD"}],
            "cycles": [{"device_id": "DD", "cycle_id": "default", "note": "想改名"}]})
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "cycle_conflict")
        self._assert_db_unchanged(device_ids=("DD",))

    def test_failed_package_leaves_no_partial_data(self):
        """包内含一台新设备与一个引用缺失项：失败后新设备也不得留下。"""
        self.make_device("OK1")
        st, b = self.req("POST", "/packages", {
            "package_id": "PKG-P",
            "devices": [{"device_id": "NEW1"}, {"device_id": "NEW2"}],
            "cycles": [{"device_id": "NEW1", "cycle_id": "b"}],
            "anchors": [{"device_id": "NEW1", "cycle_id": "b",
                         "local_ms": 0, "utc_ms": 0},
                        {"device_id": "GHOST", "local_ms": 0, "utc_ms": 0}]})
        self.assertEqual(st, 404)
        self.assertEqual(b["error"]["code"], "package_reference_missing")
        for d in ("NEW1", "NEW2"):
            st, _ = self.req("GET", f"/devices/{d}")
            self.assertEqual(st, 404)

    # ------------------------------------------------------------ 版本与并发

    def test_current_timeline_reflects_import_published_versions_unchanged(self):
        # 先发布一个空版本
        st, v1 = self.req("POST", "/versions")
        self.assertEqual(st, 201)
        self.assertEqual(v1["version_no"], 1)
        # 导入采集包
        st, body = self.import_pkg()
        self.assertEqual(st, 201)
        # 当前时间轴已反映入库结果
        _, tl = self.req("GET", "/timeline/current")
        self.assertEqual(tl["segment_count"], 2)
        # 已发布 v1 仍是原快照（空）
        _, gv1 = self.req("GET", "/versions/1")
        self.assertEqual(gv1["segments"], [])
        self.assertNotIn("PKG-1", json.dumps(gv1))
        # 新发布版本反映导入结果
        st, v2 = self.req("POST", "/versions")
        self.assertEqual(st, 201)
        self.assertEqual(v2["version_no"], 2)
        self.assertEqual(len(v2["segments"]), 2)
        self.assertEqual({s["capture_id"] for s in v2["changes"]["added"]},
                         {"CAP-A", "CAP-B"})
        # 之后再发一版：无变化
        _, v3 = self.req("POST", "/versions")
        self.assertEqual(v3["changes"]["added"], [])
        self.assertEqual(v3["changes"]["removed"], [])
        self.assertEqual(v3["changes"]["displaced"], [])

    def test_concurrent_import_and_publish_no_half_package(self):
        """多包导入与版本发布并发：每个版本要么不含、要么完整包含某包的全部声段。"""
        def make_pkg(i):
            return {
                "package_id": f"PKG-T{i}",
                "devices": [{"device_id": f"T{i}"}],
                "anchors": [{"device_id": f"T{i}", "local_ms": 0, "utc_ms": 1000 * i},
                            {"device_id": f"T{i}", "local_ms": 100,
                             "utc_ms": 1000 * i + 100}],
                "segments": [{"capture_id": f"CT{i}a", "device_id": f"T{i}",
                              "local_start_ms": 0, "local_end_ms": 50},
                             {"capture_id": f"CT{i}b", "device_id": f"T{i}",
                              "local_start_ms": 50, "local_end_ms": 100}]}

        errors = []

        def import_many():
            for i in range(8):
                st, b = self.req("POST", "/packages", make_pkg(i))
                if st != 201:
                    errors.append((i, st, b))

        def publish_many():
            for _ in range(8):
                st, v = self.req("POST", "/versions")
                if st != 201:
                    errors.append(("v", st, v))

        t1 = threading.Thread(target=import_many)
        t2 = threading.Thread(target=publish_many)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(errors, [])

        # 导入全部完成后再发布一个收尾版本：它必须完整包含全部 8 包 × 2 段；
        # 并发期间每个版本都不允许出现同一包"只进了一段"的半包状态。
        st, final_v = self.req("POST", "/versions")
        self.assertEqual(st, 201)
        _, versions = self.req("GET", "/versions")
        counts = []
        for meta in versions["versions"]:
            _, v = self.req("GET", f"/versions/{meta['version_no']}")
            ids = {s["capture_id"] for s in v["segments"]}
            counts.append(len(ids))
            # 任一版本中，同一包的两段必须同时出现或同时缺席（无半包）
            for i in range(8):
                present = {f"CT{i}a", f"CT{i}b"} & ids
                self.assertIn(len(present), (0, 2),
                              (meta["version_no"], present))
        # 版本声段数单调非降（只增不减），收尾版本为 16
        self.assertEqual(counts, sorted(counts))
        self.assertEqual(counts[-1], 16)
        self.assertEqual(final_v["version_no"], versions["versions"][-1]["version_no"])

    # ------------------------------------------------------------ 既有能力不受影响

    def test_legacy_single_entry_still_works(self):
        """既有单条录入/封存/发布流程在引入采集包后行为不变。"""
        self.assertEqual(self.make_device("L1")[0], 201)
        self.assertEqual(self.make_device("L1")[0], 200)  # 幂等
        self.assertEqual(self.add_anchor("L1", 0, 0)[0], 201)
        self.assertEqual(self.add_anchor("L1", 100, 100)[0], 201)
        # 严格递增仍由单条接口保证
        st, b = self.add_anchor("L1", 50, 999)
        self.assertEqual(st, 400)
        self.assertEqual(b["error"]["code"], "anchors_not_increasing")
        st, _ = self.submit("CL1", "L1", 10, 20)
        self.assertEqual(st, 201)
        st, _ = self.submit("CL1", "L1", 10, 20)
        self.assertEqual(st, 200)  # 同内容幂等
        self.seal("L1")
        st, b = self.add_anchor("L1", 200, 200)
        self.assertEqual(st, 409)
        self.assertEqual(b["error"]["code"], "cycle_sealed")
        st, v = self.req("POST", "/versions")
        self.assertEqual(st, 201)
        self.assertEqual(len(v["segments"]), 1)

    def test_get_package_not_found_and_list(self):
        st, b = self.req("GET", "/packages/NOPE")
        self.assertEqual(st, 404)
        self.assertEqual(b["error"]["code"], "package_not_found")
        _, lst = self.req("GET", "/packages")
        self.assertEqual(lst, {"packages": [], "count": 0})
        self.import_pkg()
        st, lst = self.req("GET", "/packages")
        self.assertEqual(st, 200)
        self.assertEqual(lst["count"], 1)
        self.assertEqual(lst["packages"][0]["package_id"], "PKG-1")
        self.assertEqual(lst["packages"][0]["segment_count"], 2)


if __name__ == "__main__":
    unittest.main()
