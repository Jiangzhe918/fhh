# -*- coding: utf-8 -*-
"""
TVBox / FongMi / 影视仓 工业级全功能苹果CMS (MacCMS v10) 聚合采集 Python Spider (type: 3)

核心升级特性 (v3.0)：
1. 【15站全量池 + 短剧聚合专区模式 (mode="duanju")】：
   - 扩充至 15 个主流综合与短剧强站（新增暴风、iKun、卧龙等高频同步红果/河马/番茄短剧的采集站）；
   - 当 ext 配置 `"mode": "duanju"` 时，自动切换为「🎭 短剧聚合专区」：智能扫描主备站点的全部短剧分类（爽文短剧、现代都市、年代穿越、古装仙侠、脑洞悬疑、女频恋爱、反转爽剧），并内置题材关键词（战神/逆袭/霸总/甜宠/重生/神医等）与更新周期多维筛选，搜索与详情页聚焦 15 站短剧秒播源。
2. 【双协议自适应容灾 (JSON + XML)】：当部分采集站 JSON 接口返回异常或格式错误时，自动无缝回退至 XML 接口解析；首页默认站故障时自动顺延容灾。
3. 【跨站归一化模糊换源与线路合并 (Normalized Cross-Site Aggregation)】：进入任意影片/短剧详情页时，自动并发检索其余 14 个兄弟采集站，支持多季剧集、全半角标点及短剧副标题归一化匹配。
4. 【M3U8 本地代理最高码率优选与防误杀去广 (Smart HLS Ad-Strip Proxy)】：
   - 自动解析 Master Playlist (`#EXT-X-STREAM-INF`) 并优选最高 `BANDWIDTH` / 分辨率画质子流；
   - 自动补全 TS 分片、`#EXT-X-KEY` 加密密钥与 `#EXT-X-MAP` (fMP4) 相对路径为绝对 URL；
   - 结合 `#EXT-X-DISCONTINUITY`、特征广告时长、跨域名检测与分片命名模式精准剥离插播广告，杜绝多目录正片分片误杀。
"""

import re
import json
import time
import threading
import urllib.parse
import xml.etree.ElementTree as ET
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from base.spider import Spider as BaseSpider
except ImportError:
    class BaseSpider:
        def init(self, extend=""): pass
        def getName(self): pass
        def isVideoFormat(self, url): pass
        def manualVideoCheck(self): pass
        def homeContent(self, filter): pass
        def homeVideoContent(self): pass
        def categoryContent(self, tid, pg, filter, extend): pass
        def detailContent(self, ids): pass
        def searchContent(self, key, quick, pg="1"): pass
        def searchContentPage(self, key, quick, pg="1"): pass
        def playerContent(self, flag, id, vipFlags): pass
        def localProxy(self, param): pass


class Spider(BaseSpider):
    # 15个主流综合与短剧采集站配置池（由 auto_update_sources.py 自动维护域名）
    SITES = {
        "lzi": {
            "name": "🎬量子资源",
            "api": "http://cj.lziapi.com/api.php/provide/vod/",
            "flag_keys": ["lzm3u8", "lzi"]
        },
        "ffzy": {
            "name": "🎞️非凡资源",
            "api": "http://ffzy5.tv/api.php/provide/vod/",
            "flag_keys": ["ffm3u8", "ffzy"]
        },
        "suoni": {
            "name": "🎧索尼资源",
            "api": "https://suoniapi.com/api.php/provide/vod/",
            "flag_keys": ["snm3u8", "suoni"]
        },
        "jisu": {
            "name": "🚀极速资源",
            "api": "https://jszyapi.com/api.php/provide/vod/",
            "flag_keys": ["jsm3u8", "jszy"]
        },
        "bfzy": {
            "name": "🌪️暴风资源",
            "api": "https://app.bfzyapi.com/api.php/provide/vod/",
            "flag_keys": ["bfzym3u8", "bfzy"]
        },
        "ikun": {
            "name": "🏀iKun资源",
            "api": "http://ikunzyapi.com/api.php/provide/vod/",
            "flag_keys": ["ikm3u8", "ikun"]
        },
        "wolong": {
            "name": "🐉卧龙资源",
            "api": "https://collect.wolongzyw.com/api.php/provide/vod/",
            "flag_keys": ["wlm3u8", "wolong"]
        },
        "guangsu": {
            "name": "⚡光速资源",
            "api": "https://api.guangsuapi.com/api.php/provide/vod/",
            "flag_keys": ["gsm3u8", "guangsu"]
        },
        "tiankong": {
            "name": "☁️天空资源",
            "api": "http://m3u8.tiankongapi.com/api.php/provide/vod/",
            "flag_keys": ["tkm3u8", "tiankong"]
        },
        "hongniu": {
            "name": "🐂红牛资源",
            "api": "https://www.hongniuzy2.com/api.php/provide/vod/",
            "flag_keys": ["hnm3u8", "hongniu"]
        },
        "wujin": {
            "name": "♾️无尽资源",
            "api": "https://api.wujinapi.me/api.php/provide/vod/",
            "flag_keys": ["wjm3u8", "wujin"]
        },
        "haohua": {
            "name": "💎豪华资源",
            "api": "https://hhzyapi.com/api.php/provide/vod/",
            "flag_keys": ["hhm3u8", "hhzy"]
        },
        "baidu": {
            "name": "🐾百度资源",
            "api": "https://api.apibdzy.com/api.php/provide/vod/",
            "flag_keys": ["bdxm3u8", "bdzy"]
        },
        "360zy": {
            "name": "🛡️360资源",
            "api": "https://360zy.com/api.php/provide/vod/",
            "flag_keys": ["360m3u8", "360zy"]
        },
        "haiwaikan": {
            "name": "🌍海外看",
            "api": "https://haiwaikan.com/api.php/provide/vod/",
            "flag_keys": ["hwkm3u8", "haiwaikan"]
        }
    }

    # 短剧强站优先顺序（用于 mode="duanju" 短剧专区）
    DUANJU_PRIORITY_SITES = [
        "lzi", "suoni", "jisu", "bfzy", "ffzy", "wujin", "haohua", "ikun", "wolong", "360zy", "hongniu", "guangsu"
    ]

    # 识别采集站中“短剧”及其子分类的关键词
    DUANJU_CATEGORY_KEYWORDS = (
        "短剧", "爽剧", "爽文", "女频", "男频", "现代都市", "年代穿越",
        "古装仙侠", "脑洞悬疑", "反转", "都市短剧", "言情短剧", "微短剧"
    )

    # 短剧热门题材虚拟分类（当主站仅有单一“短剧”分类时，自动提供热门爆款题材导航）
    DUANJU_TOPIC_TAGS = [
        ("🔥 全部最新短剧", ""),
        ("⚡ 逆袭爽剧", "逆袭"),
        ("👑 霸总甜宠", "总裁"),
        ("⚔️ 战神归来", "战神"),
        ("⏳ 重生穿越", "重生"),
        ("🏮 古装仙侠", "古装"),
        ("🩺 神医都市", "神医"),
        ("💍 闪婚豪门", "闪婚"),
        ("👶 萌宝亲情", "萌宝")
    ]

    # 敏感及杂乱分类黑名单
    BLOCKED_CATEGORIES = [
        "伦理", "论理", "福利", "写真", "美女", "热舞", "街拍", "三级",
        "情色", "成人", "里番", "同性", "传媒", "主播", "解说", "预告", "资讯"
    ]

    # 已知常见采集站插播广告单分片时长特征（秒）
    AD_DURATIONS = {
        "6.433333", "6.666667", "15.1666", "15.2666", "15.166667",
        "15.266667", "10.000000", "5.333333", "8.333333"
    }

    _CN_NUM_MAP = {
        "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
        "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
        "十一": "11", "十二": "12"
    }

    def getName(self):
        return "🎭短剧聚合专区Pro" if getattr(self, "mode", "all") == "duanju" else "🐍全聚合CMS采集引擎Pro"

    def init(self, extend=""):
        self.default_site = "ffzy"
        self.mode = "all"             # "all" (全量综合) 或 "duanju" (短剧聚合专区)
        self.remove_ad = True
        self.aggregate_detail = True  # 是否在详情页自动并发聚合其余站点的同名播放源
        self.use_local_proxy = True   # 是否启用本地代理深度清洗 M3U8 广告
        self.timeout = 7
        self._cache = OrderedDict()
        self._cache_lock = threading.Lock()
        self._cache_ttl = 300         # 5分钟缓存
        self._cache_max = 256         # 最大缓存条目数（LRU 淘汰）

        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=24, pool_maxsize=24, max_retries=1)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, application/xml, text/xml, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
        })

        if extend:
            try:
                if isinstance(extend, dict):
                    ext_cfg = extend
                elif isinstance(extend, str) and extend.strip().startswith("{"):
                    ext_cfg = json.loads(extend)
                else:
                    ext_cfg = {"default_site": str(extend).strip()}

                if ext_cfg.get("default_site") in self.SITES:
                    self.default_site = ext_cfg["default_site"]
                if ext_cfg.get("mode") in ("all", "duanju"):
                    self.mode = ext_cfg["mode"]
                if "remove_ad" in ext_cfg:
                    self.remove_ad = bool(ext_cfg["remove_ad"])
                if "aggregate_detail" in ext_cfg:
                    self.aggregate_detail = bool(ext_cfg["aggregate_detail"])
                if "use_local_proxy" in ext_cfg:
                    self.use_local_proxy = bool(ext_cfg["use_local_proxy"])
                if "api" in ext_cfg and str(ext_cfg["api"]).startswith("http"):
                    self.SITES["custom"] = {
                        "name": ext_cfg.get("name", "✨自定义站"),
                        "api": ext_cfg["api"],
                        "flag_keys": ["m3u8"]
                    }
                    self.default_site = "custom"
            except Exception:
                pass

    def isVideoFormat(self, url):
        if not url:
            return False
        return bool(re.search(r"\.(m3u8|mp4|flv|ts|mkv|mov)(\?|$)", url.split("#")[0], re.IGNORECASE))

    def manualVideoCheck(self):
        return False

    # ==================== 线程安全 LRU 缓存与 JSON/XML 双协议自适应层 ====================

    def _get_cache(self, key):
        now = time.time()
        with self._cache_lock:
            item = self._cache.get(key)
            if item is not None:
                if now - item["ts"] < self._cache_ttl:
                    self._cache.move_to_end(key)
                    return item["data"]
                else:
                    del self._cache[key]
        return None

    def _set_cache(self, key, data):
        with self._cache_lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = {"ts": time.time(), "data": data}
            while len(self._cache) > self._cache_max:
                self._cache.popitem(last=False)

    def _fetch_cms(self, api_url, params=None, timeout=None):
        """
        统一请求 MacCMS 接口：优先解析 JSON，若站点强制返回 XML 或 JSON 损坏则自动回退 XML 解析
        """
        params = params or {}
        req_timeout = timeout or self.timeout
        cache_key = f"{api_url}?{urllib.parse.urlencode(sorted(params.items()))}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        try:
            resp = self.session.get(api_url, params=params, timeout=req_timeout, verify=False)
            resp.encoding = "utf-8"
            text = resp.text.strip()
            if not text:
                return {}

            if text.startswith("{") or text.startswith("["):
                try:
                    data = json.loads(text)
                    self._set_cache(cache_key, data)
                    return data
                except Exception:
                    pass

            if text.startswith("<?xml") or "<rss" in text[:100]:
                data = self._parse_xml_response(text)
                self._set_cache(cache_key, data)
                return data
        except Exception:
            pass
        return {}

    def _parse_xml_response(self, xml_text):
        """将苹果CMS标准 XML 响应转换为与 JSON 一致的字典结构"""
        result = {"class": [], "list": [], "page": 1, "pagecount": 1, "limit": 20, "total": 0}
        try:
            clean_xml = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]", "", xml_text)
            root = ET.fromstring(clean_xml)
            list_node = root.find("list")
            if list_node is not None:
                result["page"] = int(list_node.attrib.get("page", 1))
                result["pagecount"] = int(list_node.attrib.get("pagecount", 1))
                result["limit"] = int(list_node.attrib.get("pagesize", 20))
                result["total"] = int(list_node.attrib.get("recordcount", 0))

                for video in list_node.findall("video"):
                    dl_node = video.find("dl")
                    play_from, play_url = [], []
                    if dl_node is not None:
                        for dd in dl_node.findall("dd"):
                            flag = dd.attrib.get("flag", "m3u8")
                            url_str = (dd.text or "").strip()
                            if url_str:
                                play_from.append(flag)
                                play_url.append(url_str)

                    result["list"].append({
                        "vod_id": self._xml_text(video, "id"),
                        "vod_name": self._xml_text(video, "name"),
                        "type_id": self._xml_text(video, "tid"),
                        "type_name": self._xml_text(video, "type"),
                        "vod_pic": self._xml_text(video, "pic"),
                        "vod_year": self._xml_text(video, "year"),
                        "vod_area": self._xml_text(video, "area"),
                        "vod_remarks": self._xml_text(video, "note"),
                        "vod_actor": self._xml_text(video, "actor"),
                        "vod_director": self._xml_text(video, "director"),
                        "vod_content": self._xml_text(video, "des"),
                        "vod_play_from": "$$$".join(play_from),
                        "vod_play_url": "$$$".join(play_url)
                    })

            class_node = root.find("class")
            if class_node is not None:
                for ty in class_node.findall("ty"):
                    result["class"].append({
                        "type_id": ty.attrib.get("id", ""),
                        "type_name": (ty.text or "").strip()
                    })
        except Exception:
            pass
        return result

    @staticmethod
    def _xml_text(parent, tag):
        node = parent.find(tag)
        return (node.text or "").strip() if node is not None else ""

    def _is_blocked_category(self, type_name):
        if not type_name:
            return True
        return any(word in type_name for word in self.BLOCKED_CATEGORIES)

    @classmethod
    def _is_duanju_category(cls, type_name):
        if not type_name:
            return False
        return any(kw in type_name for kw in cls.DUANJU_CATEGORY_KEYWORDS)

    # ==================== 片名与季数归一化模糊匹配算法 ====================

    @classmethod
    def _normalize_title(cls, raw_title):
        if not raw_title:
            return "", "1"
        s = str(raw_title).strip().lower()
        s = re.sub(r"[\(\[（【][^)\]）】]*(?:4k|1080p|720p|hd|bd|国语|粤语|中字|双语|未删减|版|更新|全集)[^)\]）】]*[\)\]）】]", "", s)
        s = re.sub(r"(?:国语|粤语|日语|韩语|英语|中字|高清|蓝光|4k|1080p|hd|bd)版?$", "", s).strip()

        season = "1"
        m_cn = re.search(r"第\s*([0-9]+|[一二三四五六七八九十]+)\s*[季部]", s)
        if m_cn:
            val = m_cn.group(1)
            season = cls._CN_NUM_MAP.get(val, val)
            s = s[:m_cn.start()] + s[m_cn.end():]
        else:
            m_en = re.search(r"(?:season|(?<![a-z0-9])s)\s*0*([1-9][0-9]?)(?![a-z0-9])", s)
            if m_en:
                season = m_en.group(1)
                s = s[:m_en.start()] + s[m_en.end():]
            else:
                m_tail = re.search(r"^(.{2,}?)\s*0*([2-9]|1[0-5])$", s)
                if m_tail:
                    s = m_tail.group(1)
                    season = m_tail.group(2)

        base = re.sub(r"[^\w\u4e00-\u9fa5]+", "", s)
        return base, season

    @classmethod
    def _is_title_match(cls, target_title, target_year, cand_title, cand_year):
        if not target_title or not cand_title:
            return False
        if target_title.strip() == cand_title.strip():
            return True

        base_t, season_t = cls._normalize_title(target_title)
        base_c, season_c = cls._normalize_title(cand_title)
        if not base_t or not base_c or season_t != season_c:
            return False

        year_ok = True
        exact_year = False
        if target_year and cand_year and str(target_year).isdigit() and str(cand_year).isdigit():
            yt, yc = int(target_year), int(cand_year)
            if 1900 <= yt <= 2100 and 1900 <= yc <= 2100:
                year_ok = abs(yt - yc) <= 1
                exact_year = (yt == yc)

        if not year_ok:
            return False

        if base_t == base_c:
            return True

        if exact_year and min(len(base_t), len(base_c)) >= 3:
            if base_c.startswith(base_t) or base_t.startswith(base_c):
                return True

        return False

    # ==================== 首页、分类、详情、搜索核心实现 ====================

    def _select_active_default_site(self):
        """优先使用 default_site，若其暂时不可达则自动顺延至首个返回有效分类的站点"""
        order = [self.default_site] + [k for k in self.SITES if k != self.default_site]
        for s_key in order:
            site_info = self.SITES.get(s_key)
            if not site_info:
                continue
            data = self._fetch_cms(site_info["api"], params={"ac": "list"}, timeout=5)
            if data.get("class"):
                self.default_site = s_key
                return s_key, data
        fallback_key = self.default_site if self.default_site in self.SITES else "lzi"
        return fallback_key, {}

    def _find_duanju_classes(self, site_key):
        """从指定采集站提取所有短剧相关分类（含短剧大类下的二级子分类）"""
        site_info = self.SITES.get(site_key)
        if not site_info:
            return []
        data = self._fetch_cms(site_info["api"], params={"ac": "list"}, timeout=5)
        raw_classes = data.get("class", [])
        if not raw_classes:
            return []

        duanju_root_ids = {
            str(item.get("type_id", ""))
            for item in raw_classes
            if self._is_duanju_category(item.get("type_name", ""))
        }

        matched = []
        for item in raw_classes:
            t_id = str(item.get("type_id", ""))
            t_pid = str(item.get("type_pid", ""))
            t_name = item.get("type_name", "").strip()
            if self._is_blocked_category(t_name):
                continue
            if t_id in duanju_root_ids or t_pid in duanju_root_ids or self._is_duanju_category(t_name):
                matched.append({"type_id": t_id, "type_name": t_name, "type_pid": t_pid})

        # 如果存在二级细分短剧分类，则剔除空壳父级“短剧”大类
        sub_pids = {m["type_pid"] for m in matched if m["type_pid"] in duanju_root_ids}
        if sub_pids:
            matched = [m for m in matched if m["type_id"] not in sub_pids]

        return matched

    def _home_content_duanju(self):
        """短剧聚合专区首页分类与筛选器构建"""
        classes = []
        filters = {}
        active_site = self.default_site
        duanju_cls = self._find_duanju_classes(active_site)
        if not duanju_cls:
            for s_key in self.DUANJU_PRIORITY_SITES:
                duanju_cls = self._find_duanju_classes(s_key)
                if duanju_cls:
                    active_site = s_key
                    self.default_site = s_key
                    break

        site_filter_values = [
            {"n": self.SITES[k]["name"], "v": k}
            for k in self.DUANJU_PRIORITY_SITES if k in self.SITES
        ]
        hours_filter_values = [
            {"n": "全部时间", "v": ""},
            {"n": "今日上新 (24h)", "v": "24"},
            {"n": "近三天 (72h)", "v": "72"},
            {"n": "本周热播 (168h)", "v": "168"},
            {"n": "本月爆款 (720h)", "v": "720"}
        ]
        topic_filter_values = [{"n": label, "v": kw} for label, kw in self.DUANJU_TOPIC_TAGS]

        # 1. 若采集站本身提供了多个短剧子分类（如 现代都市、年代穿越、古装仙侠 等）
        if len(duanju_cls) >= 2:
            for item in duanju_cls:
                cid = f"dj_cat${active_site}${item['type_id']}"
                classes.append({"type_id": cid, "type_name": f"🎭 {item['type_name']}"})
                filters[cid] = [
                    {"key": "site", "name": "切换短剧源", "value": site_filter_values},
                    {"key": "h", "name": "更新周期", "value": hours_filter_values}
                ]
        elif len(duanju_cls) == 1:
            base_tid = duanju_cls[0]["type_id"]
            cid = f"dj_cat${active_site}${base_tid}"
            classes.append({"type_id": cid, "type_name": f"🎭 {duanju_cls[0]['type_name']}全部"})
            filters[cid] = [
                {"key": "site", "name": "切换短剧源", "value": site_filter_values},
                {"key": "topic", "name": "热门题材", "value": topic_filter_values},
                {"key": "h", "name": "更新周期", "value": hours_filter_values}
            ]

        # 2. 追加热门短剧爆款题材直达分类（基于全站短剧关键词检索，跨站通用）
        for label, kw in self.DUANJU_TOPIC_TAGS[1:]:
            cid = f"dj_wd${active_site}${kw}"
            classes.append({"type_id": cid, "type_name": label})
            filters[cid] = [
                {"key": "site", "name": "切换短剧源", "value": site_filter_values},
                {"key": "h", "name": "更新周期", "value": hours_filter_values}
            ]

        return {"class": classes, "filters": filters}

    def homeContent(self, filter):
        if self.mode == "duanju":
            return self._home_content_duanju()

        active_site, data = self._select_active_default_site()
        classes = []
        filters = {}

        raw_classes = data.get("class", [])
        parent_ids = {
            str(item.get("type_pid", ""))
            for item in raw_classes
            if item.get("type_pid") and str(item.get("type_pid")) != "0"
        }

        for item in raw_classes:
            type_name = item.get("type_name", "").strip()
            type_id = str(item.get("type_id", ""))
            if self._is_blocked_category(type_name):
                continue
            if type_id in parent_ids and type_name in ("电影", "电视剧", "连续剧", "综艺", "动漫"):
                continue
            classes.append({
                "type_id": f"{active_site}${type_id}",
                "type_name": type_name
            })

        site_filter_values = [{"n": info["name"], "v": key} for key, info in self.SITES.items()]
        hours_filter_values = [
            {"n": "全部时间", "v": ""},
            {"n": "今日更新 (24h)", "v": "24"},
            {"n": "近三天 (72h)", "v": "72"},
            {"n": "本周更新 (168h)", "v": "168"},
            {"n": "本月更新 (720h)", "v": "720"}
        ]

        for c in classes:
            filters[c["type_id"]] = [
                {"key": "site", "name": "切换站点", "value": site_filter_values},
                {"key": "h", "name": "更新周期", "value": hours_filter_values}
            ]

        return {"class": classes, "filters": filters}

    def homeVideoContent(self):
        if self.mode == "duanju":
            duanju_cls = self._find_duanju_classes(self.default_site)
            if duanju_cls:
                site_info = self.SITES.get(self.default_site, self.SITES["lzi"])
                tid = duanju_cls[0]["type_id"]
                data = self._fetch_cms(site_info["api"], params={"ac": "detail", "t": tid, "pg": 1})
                videos = self._parse_vod_list(data.get("list", []), self.default_site)
                if videos:
                    return {"list": videos}

        site_info = self.SITES.get(self.default_site, self.SITES["lzi"])
        data = self._fetch_cms(site_info["api"], params={"ac": "detail", "pg": 1, "h": 24})
        raw_list = data.get("list", [])
        if not raw_list:
            data = self._fetch_cms(site_info["api"], params={"ac": "detail", "pg": 1})
            raw_list = data.get("list", [])
        videos = self._parse_vod_list(raw_list, self.default_site)
        return {"list": videos}

    def categoryContent(self, tid, pg, filter, extend):
        extend = extend or {}
        tid_str = str(tid)

        # 处理短剧专区分类 (dj_cat$site$tid 或 dj_wd$site$keyword)
        if tid_str.startswith("dj_wd$"):
            _, site_key, kw = tid_str.split("$", 2)
            target_site = extend.get("site", site_key)
            if target_site in self.SITES:
                site_key = target_site
            site_info = self.SITES.get(site_key, self.SITES["lzi"])
            params = {"ac": "detail", "wd": kw, "pg": pg}
            if extend.get("h"):
                params["h"] = extend["h"]
            data = self._fetch_cms(site_info["api"], params=params)
            videos = self._parse_vod_list(data.get("list", []), site_key)
            return {
                "list": videos,
                "page": int(data.get("page", pg)),
                "pagecount": int(data.get("pagecount", 1)),
                "limit": int(data.get("limit", 20)),
                "total": int(data.get("total", len(videos)))
            }

        if tid_str.startswith("dj_cat$"):
            _, site_key, real_tid = tid_str.split("$", 2)
            target_site = extend.get("site", site_key)
            if target_site in self.SITES and target_site != site_key:
                mapped_tid = self._map_category_id(site_key, real_tid, target_site, duanju_only=True)
                if mapped_tid:
                    real_tid = mapped_tid
                site_key = target_site

            site_info = self.SITES.get(site_key, self.SITES["lzi"])
            topic_kw = extend.get("topic", "").strip()
            if topic_kw:
                params = {"ac": "detail", "wd": topic_kw, "pg": pg}
            else:
                params = {"ac": "detail", "t": real_tid, "pg": pg}
            if extend.get("h"):
                params["h"] = extend["h"]
            data = self._fetch_cms(site_info["api"], params=params)
            videos = self._parse_vod_list(data.get("list", []), site_key)
            return {
                "list": videos,
                "page": int(data.get("page", pg)),
                "pagecount": int(data.get("pagecount", 1)),
                "limit": int(data.get("limit", 20)),
                "total": int(data.get("total", len(videos)))
            }

        if "$" in tid_str:
            site_key, real_tid = tid_str.split("$", 1)
        else:
            site_key, real_tid = self.default_site, tid_str

        target_site = extend.get("site", site_key)
        if target_site in self.SITES and target_site != site_key:
            real_tid = self._map_category_id(site_key, real_tid, target_site)
            site_key = target_site

        site_info = self.SITES.get(site_key, self.SITES["lzi"])
        params = {"ac": "detail", "pg": pg}
        if real_tid:
            params["t"] = real_tid
        if extend.get("h"):
            params["h"] = extend["h"]

        data = self._fetch_cms(site_info["api"], params=params)
        videos = self._parse_vod_list(data.get("list", []), site_key)

        return {
            "list": videos,
            "page": int(data.get("page", pg)),
            "pagecount": int(data.get("pagecount", 1)),
            "limit": int(data.get("limit", 20)),
            "total": int(data.get("total", len(videos)))
        }

    def _map_category_id(self, src_site, src_tid, dst_site, duanju_only=False):
        """跨站切换时，根据分类名称智能匹配目标站点的 type_id"""
        src_data = self._fetch_cms(self.SITES[src_site]["api"], params={"ac": "list"})
        src_name = ""
        for c in src_data.get("class", []):
            if str(c.get("type_id")) == str(src_tid):
                src_name = c.get("type_name", "").strip()
                break

        dst_data = self._fetch_cms(self.SITES[dst_site]["api"], params={"ac": "list"})
        dst_classes = dst_data.get("class", [])
        if src_name:
            for c in dst_classes:
                dst_name = c.get("type_name", "").strip()
                if dst_name == src_name or (len(src_name) >= 2 and src_name[:2] in dst_name):
                    return str(c.get("type_id"))

        if duanju_only:
            dj_cls = self._find_duanju_classes(dst_site)
            if dj_cls:
                return str(dj_cls[0]["type_id"])
        return ""

    def detailContent(self, ids):
        if not ids:
            return {"list": []}
        raw_id = str(ids[0])
        if "$" in raw_id:
            site_key, vod_id = raw_id.split("$", 1)
        else:
            site_key, vod_id = self.default_site, raw_id

        site_info = self.SITES.get(site_key, self.SITES["lzi"])
        data = self._fetch_cms(site_info["api"], params={"ac": "detail", "ids": vod_id})
        vod_list = data.get("list", [])
        if not vod_list:
            return {"list": []}

        item = vod_list[0]
        vod_name = item.get("vod_name", "").strip()
        vod_year = str(item.get("vod_year", "")).strip()

        clean_from, clean_url = self._extract_m3u8_streams(item, site_info["name"])

        if self.aggregate_detail and vod_name:
            extra_from, extra_url = self._aggregate_Backup_streams(
                exclude_site=site_key,
                vod_name=vod_name,
                vod_year=vod_year
            )
            clean_from.extend(extra_from)
            clean_url.extend(extra_url)

        vod = {
            "vod_id": f"{site_key}${item.get('vod_id')}",
            "vod_name": vod_name,
            "vod_pic": item.get("vod_pic", ""),
            "type_name": item.get("type_name", ""),
            "vod_year": vod_year,
            "vod_area": item.get("vod_area", ""),
            "vod_remarks": item.get("vod_remarks", ""),
            "vod_actor": item.get("vod_actor", ""),
            "vod_director": item.get("vod_director", ""),
            "vod_content": self._clean_html(item.get("vod_content", "")),
            "vod_play_from": "$$$".join(clean_from),
            "vod_play_url": "$$$".join(clean_url)
        }
        return {"list": [vod]}

    def _extract_m3u8_streams(self, item, site_label):
        play_from_list = item.get("vod_play_from", "").split("$$$")
        play_url_list = item.get("vod_play_url", "").split("$$$")

        m3u8_from, m3u8_url = [], []
        fallback_from, fallback_url = [], []

        for idx, flag in enumerate(play_from_list):
            if idx >= len(play_url_list):
                break
            flag_clean = flag.strip()
            urls_str = play_url_list[idx].strip()
            if not urls_str:
                continue

            episodes = urls_str.split("#")
            valid_m3u8_eps = []
            for ep in episodes:
                if "$" in ep:
                    ep_name, ep_link = ep.split("$", 1)
                else:
                    ep_name, ep_link = "正片", ep
                if ".m3u8" in ep_link.lower():
                    valid_m3u8_eps.append(f"{ep_name.strip()}${ep_link.strip()}")

            if valid_m3u8_eps or "m3u8" in flag_clean.lower():
                m3u8_from.append(f"{site_label}({flag_clean})")
                m3u8_url.append("#".join(valid_m3u8_eps) if valid_m3u8_eps else urls_str)
            else:
                fallback_from.append(f"{site_label}({flag_clean})")
                fallback_url.append(urls_str)

        return (m3u8_from, m3u8_url) if m3u8_from else (fallback_from, fallback_url)

    def _aggregate_Backup_streams(self, exclude_site, vod_name, vod_year):
        extra_from, extra_url = [], []
        base_query = re.sub(r"第[一二三四五六七八九十0-9]+[季部]", "", vod_name).strip() or vod_name

        def query_backup(s_key, s_info):
            data = self._fetch_cms(
                s_info["api"],
                params={"ac": "detail", "wd": vod_name},
                timeout=4
            )
            cand_list = data.get("list", [])
            if not cand_list and base_query != vod_name:
                data = self._fetch_cms(
                    s_info["api"],
                    params={"ac": "detail", "wd": base_query},
                    timeout=4
                )
                cand_list = data.get("list", [])

            for cand in cand_list:
                if self._is_blocked_category(cand.get("type_name", "")):
                    continue
                c_name = cand.get("vod_name", "").strip()
                c_year = str(cand.get("vod_year", "")).strip()
                if self._is_title_match(vod_name, vod_year, c_name, c_year):
                    return s_key, self._extract_m3u8_streams(cand, s_info["name"])
            return s_key, ([], [])

        other_sites = {k: v for k, v in self.SITES.items() if k != exclude_site}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(query_backup, k, v) for k, v in other_sites.items()]
            results_map = {}
            for fut in as_completed(futures):
                try:
                    s_key, (f_list, u_list) = fut.result()
                    if f_list:
                        results_map[s_key] = (f_list, u_list)
                except Exception:
                    continue

        for k in self.SITES:
            if k in results_map:
                f_list, u_list = results_map[k]
                extra_from.extend(f_list)
                extra_url.extend(u_list)

        return extra_from, extra_url

    def searchContent(self, key, quick, pg="1"):
        return self.searchContentPage(key, quick, pg)

    def searchContentPage(self, key, quick, pg="1"):
        """15站并发搜索，短剧模式下优先展示短剧分类结果"""
        results = []
        norm_key, _ = self._normalize_title(key)

        def fetch_site_search(site_key, site_info):
            data = self._fetch_cms(
                site_info["api"],
                params={"ac": "detail", "wd": key, "pg": pg},
                timeout=5
            )
            raw_items = data.get("list", [])
            if self.mode == "duanju":
                dj_items = [it for it in raw_items if self._is_duanju_category(it.get("type_name", ""))]
                if dj_items:
                    raw_items = dj_items
            return self._parse_vod_list(raw_items, site_key, prefix_site_name=True)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(fetch_site_search, k, v): k
                for k, v in self.SITES.items()
            }
            for future in as_completed(futures):
                try:
                    site_videos = future.result()
                    if site_videos:
                        results.extend(site_videos)
                except Exception:
                    continue

        def sort_rank(item):
            vname = item.get("vod_name", "")
            if vname == key:
                return 0
            b_v, _ = self._normalize_title(vname)
            if norm_key and b_v == norm_key:
                return 1
            return 2

        results.sort(key=sort_rank)
        return {"list": results}

    # ==================== 播放与本地代理 M3U8 深度去广引擎 ====================

    def playerContent(self, flag, id, vipFlags):
        url = id.strip()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
        if self.remove_ad and self.use_local_proxy and ".m3u8" in url.lower():
            proxy_url = f"proxy://do=m3u8&url={urllib.parse.quote(url)}"
            return {
                "parse": 0,
                "playUrl": "",
                "url": proxy_url,
                "header": headers
            }

        return {
            "parse": 0 if self.isVideoFormat(url) else 1,
            "playUrl": "",
            "url": url,
            "header": headers
        }

    def localProxy(self, param):
        url = urllib.parse.unquote(param.get("url", ""))
        if not url:
            return [404, "text/plain", "Missing URL"]

        try:
            content, final_url = self._fetch_and_resolve_m3u8(url)
            if not content or "#EXTM3U" not in content:
                return [302, "text/plain", "", {"Location": url}]

            cleaned_m3u8 = self._strip_m3u8_ads(content, final_url)
            return [200, "application/vnd.apple.mpegurl", cleaned_m3u8]
        except Exception:
            return [302, "text/plain", "", {"Location": url}]

    def _fetch_and_resolve_m3u8(self, url, depth=0):
        if depth > 3:
            return "", url
        resp = self.session.get(url, timeout=6, verify=False)
        resp.encoding = "utf-8"
        text = resp.text
        final_url = resp.url

        if "#EXT-X-STREAM-INF" in text:
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            candidates = []
            for i, line in enumerate(lines):
                if line.startswith("#EXT-X-STREAM-INF"):
                    bw_match = re.search(r"(?:AVERAGE-)?BANDWIDTH=(\d+)", line)
                    bw = int(bw_match.group(1)) if bw_match else 0
                    res_match = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
                    pixels = (int(res_match.group(1)) * int(res_match.group(2))) if res_match else 0
                    for j in range(i + 1, len(lines)):
                        if not lines[j].startswith("#"):
                            sub_url = urllib.parse.urljoin(final_url, lines[j])
                            candidates.append((bw, pixels, sub_url))
                            break
                        elif lines[j].startswith("#EXT-X-STREAM-INF"):
                            break
            if candidates:
                candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
                return self._fetch_and_resolve_m3u8(candidates[0][2], depth + 1)

        return text, final_url

    @staticmethod
    def _rewrite_uri_attr(line, base_url):
        if 'URI="' in line:
            return re.sub(
                r'URI="([^"]+)"',
                lambda m: f'URI="{urllib.parse.urljoin(base_url, m.group(1))}"',
                line
            )
        return line

    @staticmethod
    def _ts_name_style(ts_url):
        path = ts_url.split("?", 1)[0]
        fname = path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        if fname.isdigit() or re.match(r"^(?:index|seg|clip|ts|out|video)[_-]?\d+$", fname, re.IGNORECASE):
            return "seq"
        if len(fname) >= 12 and re.match(r"^[a-f0-9]+$", fname, re.IGNORECASE):
            return "hash"
        return f"len_{len(fname)}"

    def _strip_m3u8_ads(self, m3u8_text, base_url):
        lines = m3u8_text.splitlines()
        header_lines = []
        segments = []
        current_seg = []

        in_body = False
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#EXT-X-DISCONTINUITY"):
                in_body = True
                if current_seg:
                    segments.append(current_seg)
                    current_seg = []
                continue
            if not in_body and not stripped.startswith("#EXTINF"):
                if stripped.startswith(("#EXT-X-KEY", "#EXT-X-MAP")):
                    stripped = self._rewrite_uri_attr(stripped, base_url)
                header_lines.append(stripped)
            else:
                in_body = True
                if stripped.startswith(("#EXT-X-KEY", "#EXT-X-MAP")):
                    stripped = self._rewrite_uri_attr(stripped, base_url)
                elif not stripped.startswith("#"):
                    stripped = urllib.parse.urljoin(base_url, stripped)
                current_seg.append(stripped)

        if current_seg:
            segments.append(current_seg)

        if len(segments) <= 1:
            out_single = header_lines + (segments[0] if segments else [])
            return "\n".join(out_single)

        total_ts = 0
        host_counts = {}
        dir_counts = {}
        style_counts = {}
        for seg in segments:
            for l in seg:
                if not l.startswith("#"):
                    total_ts += 1
                    parsed = urllib.parse.urlsplit(l)
                    host = f"{parsed.scheme}://{parsed.netloc}"
                    dpath = l.split("?", 1)[0].rsplit("/", 1)[0]
                    style = self._ts_name_style(l)
                    host_counts[host] = host_counts.get(host, 0) + 1
                    dir_counts[dpath] = dir_counts.get(dpath, 0) + 1
                    style_counts[style] = style_counts.get(style, 0) + 1

        dominant_host = max(host_counts, key=host_counts.get) if host_counts else ""
        dominant_dir = max(dir_counts, key=dir_counts.get) if dir_counts else ""
        dominant_dir_ratio = (dir_counts.get(dominant_dir, 0) / float(total_ts)) if total_ts else 0.0
        dominant_style = max(style_counts, key=style_counts.get) if style_counts else ""

        clean_body = []
        for seg in segments:
            ts_urls = [l for l in seg if not l.startswith("#")]
            extinfs = [l for l in seg if l.startswith("#EXTINF:")]
            seg_ts_count = len(ts_urls)

            seg_durations = []
            for inf in extinfs:
                m = re.search(r"#EXTINF:([0-9.]+)", inf)
                if m:
                    seg_durations.append(m.group(1))
            seg_total_sec = sum(float(d) for d in seg_durations if d.replace(".", "", 1).isdigit())

            is_ad_segment = False
            if 0 < seg_ts_count <= 10:
                if seg_total_sec <= 45.0 and any(d in self.AD_DURATIONS for d in seg_durations):
                    is_ad_segment = True

                if not is_ad_segment and ts_urls:
                    first_u = ts_urls[0]
                    p = urllib.parse.urlsplit(first_u)
                    seg_host = f"{p.scheme}://{p.netloc}"
                    seg_dir = first_u.split("?", 1)[0].rsplit("/", 1)[0]
                    seg_style = self._ts_name_style(first_u)

                    if dominant_host and seg_host != dominant_host:
                        is_ad_segment = True
                    elif (
                        dominant_dir
                        and seg_dir != dominant_dir
                        and seg_ts_count <= 6
                        and seg_total_sec <= 30.0
                        and (dominant_dir_ratio >= 0.80 or seg_style != dominant_style)
                    ):
                        is_ad_segment = True

                if not is_ad_segment:
                    if any(re.search(r"(1o\.ts|/ad[_/]|adjump|gg\.ts|tuiguang)", u, re.IGNORECASE) for u in ts_urls):
                        is_ad_segment = True

            if not is_ad_segment:
                if clean_body:
                    clean_body.append("#EXT-X-DISCONTINUITY")
                clean_body.extend(seg)

        out_lines = header_lines + clean_body
        if out_lines and not out_lines[-1].startswith("#EXT-X-ENDLIST"):
            out_lines.append("#EXT-X-ENDLIST")
        return "\n".join(out_lines)

    # ==================== 辅助工具函数 ====================

    @staticmethod
    def _clean_html(raw_html):
        if not raw_html:
            return ""
        text = re.sub(r"<[^>]+>", "", str(raw_html))
        text = re.sub(r"&nbsp;|&#160;", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _parse_vod_list(self, raw_list, site_key, prefix_site_name=False):
        videos = []
        site_name = self.SITES.get(site_key, {}).get("name", site_key)
        for item in raw_list:
            type_name = item.get("type_name", "")
            if self._is_blocked_category(type_name):
                continue
            remarks = item.get("vod_remarks", "")
            if prefix_site_name:
                remarks = f"{site_name} | {remarks}" if remarks else site_name
            videos.append({
                "vod_id": f"{site_key}${item.get('vod_id')}",
                "vod_name": item.get("vod_name", ""),
                "vod_pic": item.get("vod_pic", ""),
                "vod_remarks": remarks,
                "vod_year": str(item.get("vod_year", ""))
            })
        return videos
