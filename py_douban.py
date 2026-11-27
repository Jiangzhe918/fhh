# -*- coding: utf-8 -*-
"""
豆瓣推荐 Python Spider (type: 3) —— 自包含，无外部 jar/js 依赖 (v3.0)

核心升级特性：
1. 【vod_id 内嵌片名/年份/封面容错】：列表与搜索阶段直接将 `id$title$year$cover` 编码进 `vod_id`，进入详情页时无需依赖豆瓣 `subject_abstract` 接口即可瞬间启动 15 站并发搜源，彻底杜绝豆瓣接口 403/限流导致的详情页白屏。
2. 【移动端 Rexxar 接口双路兜底 & 海报防盗链修复】：豆瓣 PC 详情接口受限时自动回退移动端接口，并为豆瓣海报图片自动附加 Referer/UA 标记，防止电视端裂图。
3. 【跨站片名与季数归一化匹配】：支持“庆余年 第二季”与“庆余年2”等跨站异名剧集自动对齐，大幅提高多季剧与番剧搜源命中率。
4. 【内置本地代理 M3U8 最高码率优选与防误杀去广告】：与 `py_cms_spider.py` 共享同款本地代理 HLS 深度去广引擎。
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
        def playerContent(self, flag, id, vipFlags): pass
        def localProxy(self, param): pass


class Spider(BaseSpider):
    # 与 py_cms_spider.py 保持同步的 15 站采集池（由 auto_update_sources.py 自动更新域名）
    CMS_SITES = {
        "lzi": {
            "name": "🎬量子资源",
            "api": "http://cj.lziapi.com/api.php/provide/vod/"
        },
        "ffzy": {
            "name": "🎞️非凡资源",
            "api": "http://ffzy1.tv/api.php/provide/vod/"
        },
        "suoni": {
            "name": "🎧索尼资源",
            "api": "http://suoniapi.com/api.php/provide/vod/"
        },
        "jisu": {
            "name": "🚀极速资源",
            "api": "https://jszyapi.com/api.php/provide/vod/"
        },
        "bfzy": {
            "name": "🌪️暴风资源",
            "api": "https://app.bfzyapi.com/api.php/provide/vod/"
        },
        "ikun": {
            "name": "🏀iKun资源",
            "api": "http://ikunzyapi.com/api.php/provide/vod/"
        },
        "wolong": {
            "name": "🐉卧龙资源",
            "api": "https://collect.wolongzyw.com/api.php/provide/vod/"
        },
        "guangsu": {
            "name": "⚡光速资源",
            "api": "https://api.guangsuapi.com/api.php/provide/vod/"
        },
        "tiankong": {
            "name": "☁️天空资源",
            "api": "http://m3u8.tiankongapi.com/api.php/provide/vod/"
        },
        "hongniu": {
            "name": "🐂红牛资源",
            "api": "https://www.hongniuzy2.com/api.php/provide/vod/"
        },
        "wujin": {
            "name": "♾️无尽资源",
            "api": "https://api.wujinapi.me/api.php/provide/vod/"
        },
        "haohua": {
            "name": "💎豪华资源",
            "api": "https://hhzyapi.com/api.php/provide/vod/"
        },
        "baidu": {
            "name": "🐾百度资源",
            "api": "http://api.apibdzy.com/api.php/provide/vod/"
        },
        "360zy": {
            "name": "🛡️360资源",
            "api": "https://360zy5.com/api.php/provide/vod/"
        },
        "haiwaikan": {
            "name": "🌍海外看",
            "api": "https://haiwaikan.com/api.php/provide/vod/"
        }
    }

    TAGS = [
        ("热门电影", "movie", "热门"),
        ("热门剧集", "tv", "热门"),
        ("热门综艺", "tv", "综艺"),
        ("热门动漫", "tv", "动漫"),
        ("华语电影", "movie", "华语"),
        ("欧美电影", "movie", "欧美"),
        ("日韩电影", "movie", "日本"),
        ("高分电影", "movie", "豆瓣高分"),
        ("高分剧集", "tv", "豆瓣高分"),
        ("最新电影", "movie", "最新"),
        ("最新剧集", "tv", "最新")
    ]

    BLOCKED_WORDS = [
        "伦理", "论理", "福利", "写真", "美女", "热舞", "街拍", "三级",
        "情色", "成人", "里番", "同性", "传媒", "主播"
    ]

    AD_DURATIONS = {
        "6.433333", "6.666667", "15.1666", "15.2666", "15.166667",
        "15.266667", "10.000000", "5.333333", "8.333333"
    }

    _CN_NUM_MAP = {
        "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
        "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
        "十一": "11", "十二": "12"
    }

    DOUBAN_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://movie.douban.com/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
    }

    def getName(self):
        return "🌟豆瓣推荐"

    def init(self, extend=""):
        self.timeout = 6
        self.remove_ad = True
        self.use_local_proxy = True
        self._cache = OrderedDict()
        self._cache_lock = threading.Lock()
        self._cache_ttl = 600
        self._cache_max = 256

        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=24, pool_maxsize=24, max_retries=1)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if extend and isinstance(extend, str) and extend.strip().startswith("{"):
            try:
                ext_cfg = json.loads(extend)
                if "remove_ad" in ext_cfg:
                    self.remove_ad = bool(ext_cfg["remove_ad"])
                if "use_local_proxy" in ext_cfg:
                    self.use_local_proxy = bool(ext_cfg["use_local_proxy"])
            except Exception:
                pass

    def isVideoFormat(self, url):
        if not url:
            return False
        return bool(re.search(r"\.(m3u8|mp4|flv|ts|mkv|mov)(\?|$)", url.split("#")[0], re.IGNORECASE))

    def manualVideoCheck(self):
        return False

    # ==================== 线程安全缓存与网络请求 ====================

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

    def _fetch_json(self, url, params=None, headers=None, timeout=None):
        params = params or {}
        cache_key = f"{url}?{urllib.parse.urlencode(sorted(params.items()))}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached
        try:
            resp = self.session.get(
                url, params=params,
                headers=headers or self.DOUBAN_HEADERS,
                timeout=timeout or self.timeout,
                verify=False
            )
            resp.encoding = "utf-8"
            text = resp.text.strip()
            if text.startswith("{") or text.startswith("["):
                data = json.loads(text)
                self._set_cache(cache_key, data)
                return data
        except Exception:
            pass
        return {}

    def _fetch_cms(self, api_url, params, timeout=5):
        cache_key = f"{api_url}?{urllib.parse.urlencode(sorted(params.items()))}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached
        try:
            resp = self.session.get(api_url, params=params, timeout=timeout, verify=False)
            resp.encoding = "utf-8"
            text = resp.text.strip()
            if text.startswith("{"):
                data = json.loads(text)
                self._set_cache(cache_key, data)
                return data
            if text.startswith("<?xml") or "<rss" in text[:100]:
                data = self._parse_xml(text)
                self._set_cache(cache_key, data)
                return data
        except Exception:
            pass
        return {}

    @staticmethod
    def _parse_xml(xml_text):
        result = {"list": []}
        try:
            clean = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]", "", xml_text)
            root = ET.fromstring(clean)
            list_node = root.find("list")
            if list_node is None:
                return result
            for video in list_node.findall("video"):
                def txt(tag):
                    node = video.find(tag)
                    return (node.text or "").strip() if node is not None else ""
                play_from, play_url = [], []
                dl = video.find("dl")
                if dl is not None:
                    for dd in dl.findall("dd"):
                        s = (dd.text or "").strip()
                        if s:
                            play_from.append(dd.attrib.get("flag", "m3u8"))
                            play_url.append(s)
                result["list"].append({
                    "vod_id": txt("id"),
                    "vod_name": txt("name"),
                    "type_name": txt("type"),
                    "vod_pic": txt("pic"),
                    "vod_year": txt("year"),
                    "vod_remarks": txt("note"),
                    "vod_actor": txt("actor"),
                    "vod_director": txt("director"),
                    "vod_content": txt("des"),
                    "vod_play_from": "$$$".join(play_from),
                    "vod_play_url": "$$$".join(play_url)
                })
        except Exception:
            pass
        return result

    def _blocked(self, name):
        return any(w in (name or "") for w in self.BLOCKED_WORDS)

    @staticmethod
    def _decorate_douban_pic(pic_url):
        if not pic_url:
            return ""
        if "doubanio.com" in pic_url and "@Referer=" not in pic_url:
            return f"{pic_url}@Referer=https://movie.douban.com/@User-Agent=Mozilla/5.0"
        return pic_url

    @staticmethod
    def _encode_vod_id(subject_id, title="", year="", cover=""):
        safe_title = str(title or "").replace("$", " ").strip()
        safe_year = str(year or "").replace("$", "").strip()
        safe_cover = str(cover or "").replace("$", "").strip()
        return f"{subject_id}${safe_title}${safe_year}${safe_cover}"

    @staticmethod
    def _decode_vod_id(raw_id):
        parts = str(raw_id or "").split("$")
        if len(parts) >= 4:
            return parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()
        elif len(parts) == 2:
            return parts[1].strip(), "", "", ""
        return parts[0].strip(), "", "", ""

    # ==================== 片名与季数归一化模糊匹配 ====================

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

    # ==================== 豆瓣推荐流 ====================

    def _subjects(self, dtype, tag, pg=1, sort="recommend"):
        data = self._fetch_json(
            "https://movie.douban.com/j/search_subjects",
            params={
                "type": dtype,
                "tag": tag,
                "sort": sort,
                "page_limit": 20,
                "page_start": (int(pg) - 1) * 20
            }
        )
        videos = []
        for s in data.get("subjects", []):
            title = s.get("title", "").strip()
            if not title or self._blocked(title):
                continue
            sid = str(s.get("id", ""))
            cover = s.get("cover", "")
            rate = s.get("rate", "")
            remarks = f"⭐{rate}" if rate else ""
            ep = s.get("episodes_info", "")
            if ep:
                remarks = f"{remarks} {ep}".strip()
            videos.append({
                "vod_id": self._encode_vod_id(sid, title, "", cover),
                "vod_name": title,
                "vod_pic": self._decorate_douban_pic(cover),
                "vod_remarks": remarks
            })
        return videos

    def homeContent(self, filter):
        classes = []
        filters = {}
        sort_values = [
            {"n": "综合推荐", "v": "recommend"},
            {"n": "最新上架", "v": "time"},
            {"n": "评分优先", "v": "rank"}
        ]
        for label, dtype, tag in self.TAGS:
            tid = f"{dtype}|{tag}"
            classes.append({"type_id": tid, "type_name": label})
            filters[tid] = [{"key": "sort", "name": "排序", "value": sort_values}]
        return {"class": classes, "filters": filters}

    def homeVideoContent(self):
        videos = self._subjects("movie", "热门")
        if not videos:
            videos = self._subjects("tv", "热门")
        return {"list": videos[:20]}

    def categoryContent(self, tid, pg, filter, extend):
        extend = extend or {}
        dtype, tag = str(tid).split("|", 1) if "|" in str(tid) else ("movie", "热门")
        sort = extend.get("sort", "recommend")
        videos = self._subjects(dtype, tag, pg, sort)
        page = int(pg)
        return {
            "list": videos,
            "page": page,
            "pagecount": page + 1 if len(videos) >= 20 else page,
            "limit": 20,
            "total": 9999
        }

    # ==================== 详情：内嵌元数据秒级兜底 + 15站 CMS 聚合取源 ====================

    def _fetch_douban_detail(self, subject_id):
        abstract = self._fetch_json(
            "https://movie.douban.com/j/subject_abstract",
            params={"subject_id": subject_id},
            timeout=3.5
        ).get("subject", {})
        if abstract and abstract.get("title"):
            return abstract

        mobile_headers = dict(self.DOUBAN_HEADERS)
        mobile_headers["Referer"] = f"https://m.douban.com/movie/subject/{subject_id}/"
        rexxar = self._fetch_json(
            f"https://m.douban.com/rexxar/api/v2/movie/{subject_id}",
            headers=mobile_headers,
            timeout=3.5
        )
        if rexxar and rexxar.get("title"):
            rating_val = (rexxar.get("rating") or {}).get("value", "")
            return {
                "title": rexxar.get("title", ""),
                "year": str(rexxar.get("year", "")),
                "rate": str(rating_val) if rating_val else "",
                "region": " / ".join(rexxar.get("countries", [])),
                "types": rexxar.get("genres", []),
                "cover": (rexxar.get("pic") or {}).get("normal", ""),
                "intro": rexxar.get("intro", "")
            }
        return {}

    def detailContent(self, ids):
        if not ids:
            return {"list": []}
        subject_id, emb_title, emb_year, emb_cover = self._decode_vod_id(ids[0])

        abstract = self._fetch_douban_detail(subject_id)
        title = (abstract.get("title") or emb_title or "").strip()
        year = str(abstract.get("year") or emb_year or "").strip()
        if not title:
            return {"list": []}

        play_from, play_url, meta = self._aggregate_cms(title, year)

        rate = abstract.get("rate", "")
        remarks = f"⭐豆瓣评分 {rate}" if rate else (meta.get("vod_remarks") or "")
        content_parts = []
        if abstract.get("region"):
            content_parts.append(f"地区: {abstract['region']}")
        if abstract.get("types"):
            content_parts.append(f"类型: {' / '.join(abstract['types'])}")
        if meta.get("vod_actor"):
            content_parts.append(f"演员: {meta['vod_actor']}")
        if meta.get("vod_director"):
            content_parts.append(f"导演: {meta['vod_director']}")
        raw_desc = meta.get("vod_content") or abstract.get("intro") or ""
        desc = re.sub(r"<[^>]+>", "", str(raw_desc))
        desc = re.sub(r"\s+", " ", desc).strip()
        if desc:
            content_parts.append(desc)

        cover_url = meta.get("vod_pic") or self._decorate_douban_pic(abstract.get("cover") or emb_cover)
        vod = {
            "vod_id": self._encode_vod_id(subject_id, title, year, emb_cover),
            "vod_name": title,
            "vod_pic": cover_url,
            "type_name": " / ".join(abstract.get("types", [])) or meta.get("type_name", ""),
            "vod_year": year or meta.get("vod_year", ""),
            "vod_area": abstract.get("region", "") or meta.get("vod_area", ""),
            "vod_remarks": remarks,
            "vod_actor": meta.get("vod_actor", ""),
            "vod_director": meta.get("vod_director", ""),
            "vod_content": "\n".join(content_parts) or "豆瓣推荐影片，播放源由聚合采集站自动提供。",
            "vod_play_from": "$$$".join(play_from),
            "vod_play_url": "$$$".join(play_url)
        }
        return {"list": [vod]}

    def _aggregate_cms(self, title, year):
        found = {}
        meta = {}
        base_query = re.sub(r"第[一二三四五六七八九十0-9]+[季部]", "", title).strip() or title

        def query(site_key, info):
            data = self._fetch_cms(info["api"], {"ac": "detail", "wd": title}, timeout=5)
            cand_list = data.get("list", [])
            if not cand_list and base_query != title:
                data = self._fetch_cms(info["api"], {"ac": "detail", "wd": base_query}, timeout=4)
                cand_list = data.get("list", [])

            for cand in cand_list:
                c_name = (cand.get("vod_name") or "").strip()
                c_year = str(cand.get("vod_year") or "").strip()
                if self._blocked(cand.get("type_name", "")):
                    continue
                if self._is_title_match(title, year, c_name, c_year):
                    return site_key, cand
            return site_key, None

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(query, k, v) for k, v in self.CMS_SITES.items()]
            for fut in as_completed(futures):
                try:
                    site_key, cand = fut.result()
                    if cand:
                        found[site_key] = cand
                except Exception:
                    continue

        play_from, play_url = [], []
        for site_key in self.CMS_SITES:
            cand = found.get(site_key)
            if not cand:
                continue
            if not meta:
                meta = cand
            label = self.CMS_SITES[site_key]["name"]
            from_list = str(cand.get("vod_play_from", "")).split("$$$")
            url_list = str(cand.get("vod_play_url", "")).split("$$$")
            for idx, flag in enumerate(from_list):
                if idx >= len(url_list):
                    break
                urls_str = url_list[idx].strip()
                if not urls_str:
                    continue
                m3u8_eps = []
                for ep in urls_str.split("#"):
                    if "$" in ep:
                        ep_name, ep_link = ep.split("$", 1)
                    else:
                        ep_name, ep_link = "正片", ep
                    if ".m3u8" in ep_link.lower():
                        m3u8_eps.append(f"{ep_name.strip()}${ep_link.strip()}")
                if m3u8_eps:
                    play_from.append(f"{label}({flag.strip()})")
                    play_url.append("#".join(m3u8_eps))
                elif "m3u8" in flag.lower():
                    play_from.append(f"{label}({flag.strip()})")
                    play_url.append(urls_str)

        if not play_from and meta:
            from_list = str(meta.get("vod_play_from", "")).split("$$$")
            url_list = str(meta.get("vod_play_url", "")).split("$$$")
            for idx, flag in enumerate(from_list):
                if idx < len(url_list) and url_list[idx].strip():
                    play_from.append(f"聚合({flag.strip()})")
                    play_url.append(url_list[idx].strip())

        return play_from, play_url, meta

    # ==================== 搜索 ====================

    def searchContent(self, key, quick, pg="1"):
        data = self._fetch_json(
            "https://movie.douban.com/j/subject_suggest",
            params={"q": key}
        )
        videos = []
        if isinstance(data, list):
            for s in data:
                title = (s.get("title") or "").strip()
                if not title or self._blocked(title):
                    continue
                sid = str(s.get("id", ""))
                year = str(s.get("year", "") or "")
                sub = s.get("sub_title", "")
                cover = s.get("img", "")
                remarks = " ".join(x for x in [year, sub] if x)
                videos.append({
                    "vod_id": self._encode_vod_id(sid, title, year, cover),
                    "vod_name": title,
                    "vod_pic": self._decorate_douban_pic(cover),
                    "vod_remarks": remarks,
                    "vod_year": year
                })
        return {"list": videos}

    # ==================== 播放与本地代理 M3U8 去广告引擎 ====================

    def playerContent(self, flag, id, vipFlags):
        url = id.strip()
        headers = {"User-Agent": self.DOUBAN_HEADERS["User-Agent"]}
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
