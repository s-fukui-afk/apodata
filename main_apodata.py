# ===== 置き換え用・完全版: main_apodata.py =====
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
import httplib2
from datetime import datetime, timedelta
import pytz, time, random, socket, re

from apodata_pipeline.config_apodata import (
    # 既存
    TARGET_ID, TARGET_SHEET, MEIBO_SHEET,
    COLUMNS, BATCH_SIZE_COPY_INIT, BATCH_SIZE_COPY_MIN, WRITE_START,
    # 追加（config_apodata.py 側に定義が必要）
    META_SHEET, SOURCES
)

# ==== 固定設定 ====
NUM_CALC_COLS = 12          # AM〜 の計算列数（入社年月まで）
HTTP_TIMEOUT_SEC = 120
MAX_RETRY = 5

SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/cloud-platform",
)

def get_sa_creds():
    """
    環境変数 GOOGLE_APPLICATION_CREDENTIALS (=SA鍵のパス) を読み、
    サービスアカウント資格情報を生成して返す。
    """
    key_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  # 例: /Users/shota/keys/flowx-test-run-sa.json
    return service_account.Credentials.from_service_account_file(key_path).with_scopes(list(SCOPES))

# ==== 列ユーティリティ ====
def _col_to_a1(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def a1_range(sheet: str, row_start: int, col_start: int, num_rows: int, num_cols: int) -> str:
    start_cell = f"{_col_to_a1(col_start)}{row_start}"
    end_cell   = f"{_col_to_a1(col_start + num_cols - 1)}{row_start + num_rows - 1}"
    return f"{sheet}!{start_cell}:{end_cell}"

# === コピー対象：詰め込み設定 ===
COLS_PACKED = COLUMNS[:]                             # 38列を A から詰める
PACKED_INDEX = {col: i for i, col in enumerate(COLS_PACKED, start=1)}  # 1始まり
PACKED_LAST_COL = len(COLS_PACKED)                   # 38
SRC_MIN_COL = min(COLUMNS)                           # 2
SRC_MAX_COL = max(COLUMNS)                           # 49
SRC_WINDOW_NUM_COLS = SRC_MAX_COL - SRC_MIN_COL + 1  # 48列（B..AW）

print(f"[CONF] WRITE_START={WRITE_START} ({_col_to_a1(WRITE_START)}), "
      f"PACKED_LAST_COL={PACKED_LAST_COL} (期待=38), "
      f"SRC_WINDOW=B..{_col_to_a1(SRC_MAX_COL)}")

# ==== リトライ・HTTPヘルパ ====
def _with_retry(callable_fn, *args, **kwargs):
    for attempt in range(1, MAX_RETRY + 1):
        try:
            return callable_fn(*args, **kwargs)
        except (TimeoutError, socket.timeout) as e:
            wait = (2 ** attempt) + random.uniform(0, 1.0)
            print(f"[RETRY] timeout attempt {attempt}/{MAX_RETRY}; sleep {wait:.2f}s")
            time.sleep(wait)
        except HttpError as he:
            status = getattr(he, "status_code", None) or (he.resp.status if getattr(he, "resp", None) else None)
            if status in (429, 500, 502, 503, 504):
                wait = (2 ** attempt) + random.uniform(0, 1.0)
                print(f"[RETRY] HttpError {status} attempt {attempt}/{MAX_RETRY}; sleep {wait:.2f}s")
                time.sleep(wait)
            else:
                raise
        except Exception:
            raise
    raise TimeoutError("max retry exceeded")

# ==== Sheets ユーティリティ ====
def get_last_row_by_col(sheets, spreadsheet_id, sheet_name, col_index: int):
    """指定列（必ず非空な列。ここでは 案件ID = B列=2 を想定）の最終非空行"""
    col_letter = _col_to_a1(col_index)
    resp = _with_retry(
        sheets.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!{col_letter}:{col_letter}",
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute
    )
    return len(resp.get("values", []))

def get_sheet_meta(sheets, spreadsheet_id, sheet_name):
    meta = _with_retry(sheets.get(spreadsheetId=spreadsheet_id).execute)
    for s in meta.get("sheets", []):
        p = s.get("properties", {})
        if p.get("title") == sheet_name:
            g = p.get("gridProperties", {})
            return p.get("sheetId"), g.get("rowCount", 0), g.get("columnCount", 0)
    raise RuntimeError(f"sheet not found: {sheet_name}")

def ensure_target_columns(sheets, min_columns: int):
    sheet_id, row_count, col_count = get_sheet_meta(sheets, TARGET_ID, TARGET_SHEET)
    need_cols = max(col_count or 0, min_columns)
    if need_cols > (col_count or 0):
        _with_retry(
            sheets.batchUpdate(
                spreadsheetId=TARGET_ID,
                body={"requests": [{
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "gridProperties": {"columnCount": need_cols}},
                        "fields": "gridProperties.columnCount"
                    }
                }]}
            ).execute
        )
        print(f"[GRID] expanded columns {col_count}->{need_cols}")
    else:
        print(f"[GRID] ok columns={col_count}")
    return need_cols

def ensure_target_rows(sheets, min_rows: int):
    """必要行数 min_rows までシートの行数を拡張する"""
    sheet_id, row_count, col_count = get_sheet_meta(sheets, TARGET_ID, TARGET_SHEET)
    need_rows = max(row_count or 0, min_rows)
    if need_rows > (row_count or 0):
        _with_retry(
            sheets.batchUpdate(
                spreadsheetId=TARGET_ID,
                body={"requests": [{
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "gridProperties": {"rowCount": need_rows}},
                        "fields": "gridProperties.rowCount"
                    }
                }]}
            ).execute
        )
        print(f"[GRID] expanded rows {row_count}->{need_rows}")
    return need_rows

# ==== メタ（1回限りソース & 固定行管理） ====
def _ensure_meta_sheet(sheets):
    meta = _with_retry(sheets.get(spreadsheetId=TARGET_ID).execute)
    names = {s['properties']['title'] for s in meta.get('sheets', [])}
    if META_SHEET not in names:
        _with_retry(sheets.batchUpdate(
            spreadsheetId=TARGET_ID,
            body={"requests":[{"addSheet":{"properties":{"title": META_SHEET}}}]}
        ).execute)
        _with_retry(sheets.values().update(
            spreadsheetId=TARGET_ID,
            range=f"{META_SHEET}!A1:C1",
            valueInputOption="RAW",
            body={"values":[["key","value","updated_at"]]}
        ).execute)

def _meta_get(values_service, key: str, default=""):
    rows = values_service.get(
        spreadsheetId=TARGET_ID, range=f"{META_SHEET}!A2:C"
    ).execute().get("values", []) or []
    for r in rows:
        if len(r) > 0 and r[0] == key:
            return r[1] if len(r) > 1 else default
    return default

def _meta_set(values_service, key: str, value: str):
    rows = values_service.get(
        spreadsheetId=TARGET_ID, range=f"{META_SHEET}!A2:C"
    ).execute().get("values", []) or []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    idx = None
    for i, r in enumerate(rows, start=2):
        if len(r) > 0 and r[0] == key:
            idx = i; break
    if idx:
        _with_retry(values_service.update(
            spreadsheetId=TARGET_ID,
            range=f"{META_SHEET}!A{idx}:C{idx}",
            valueInputOption="RAW",
            body={"values":[[key, value, now]]}
        ).execute)
    else:
        _with_retry(values_service.append(
            spreadsheetId=TARGET_ID,
            range=f"{META_SHEET}!A:C",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values":[[key, value, now]]}
        ).execute)

def _done_key(src_id: str, sheet: str) -> str:
    return f"DONE::{src_id}:{sheet}"

def _frozen_rows_key() -> str:
    return "FROZEN_TOP_ROWS"

def _get_frozen_rows(values_service) -> int:
    v = _meta_get(values_service, _frozen_rows_key(), "0")
    try:
        return max(0, int(v))
    except:
        return 0

def _add_frozen_rows(values_service, add: int):
    cur = _get_frozen_rows(values_service)
    _meta_set(values_service, _frozen_rows_key(), str(cur + max(0, int(add))))

# ==== クリア処理（固定行を残す対応） ====
def clear_target_before_run(sheets, grid_columns: int, preserve_top_rows: int = 0):
    """
    preserve_top_rows 行（ヘッダの下）を残し、以降をクリア。
    ヘッダは1行目想定。固定行が 0 の場合は従来の全クリア。
    """
    if preserve_top_rows <= 0:
        copy_end = _col_to_a1(PACKED_LAST_COL)  # A..AL
        _with_retry(sheets.values().clear(
            spreadsheetId=TARGET_ID, range=f"{TARGET_SHEET}!A2:{copy_end}"
        ).execute)
        start_letter = _col_to_a1(WRITE_START)  # AM..
        end_letter   = _col_to_a1(grid_columns)
        _with_retry(sheets.values().clear(
            spreadsheetId=TARGET_ID, range=f"{TARGET_SHEET}!{start_letter}2:{end_letter}"
        ).execute)
        print(f"[CLEAR] A2:{copy_end}, {start_letter}2:{end_letter}")
        return

    start_row = 2 + preserve_top_rows
    end_letter = _col_to_a1(grid_columns)
    copy_end   = _col_to_a1(PACKED_LAST_COL)
    _with_retry(sheets.values().clear(
        spreadsheetId=TARGET_ID, range=f"{TARGET_SHEET}!A{start_row}:{copy_end}"
    ).execute)
    _with_retry(sheets.values().clear(
        spreadsheetId=TARGET_ID, range=f"{TARGET_SHEET}!{_col_to_a1(WRITE_START)}{start_row}:{end_letter}"
    ).execute)
    print(f"[CLEAR(partial)] kept top {preserve_top_rows} rows; cleared from row {start_row} downward")

# ==== 日付/計算ヘルパ ====
def parse_date(v):
    if v in (None, ""):
        return None
    try:
        base = datetime(1899, 12, 30)
        return base + timedelta(days=float(v))
    except Exception:
        pass
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d",
                "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def format_getsudo(dt, tzname="Asia/Tokyo"):
    if not dt:
        return ""
    jst = pytz.timezone(tzname)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC).astimezone(jst)
    else:
        dt = dt.astimezone(jst)
    return dt.strftime("%Y年%m月度")

def gv_from_packed(row, src_col, default=""):
    idx1 = PACKED_INDEX.get(src_col)
    if not idx1:
        return default
    idx0 = idx1 - 1
    return row[idx0] if idx0 < len(row) else default

# ==== 名簿ロード（旧：互換用。必要なら残す） ====
def load_joining_map(sheets):
    resp = _with_retry(
        sheets.values().get(
            spreadsheetId=TARGET_ID,
            range=f"{MEIBO_SHEET}!A2:B",
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute
    )
    joining_map = {}
    for r in resp.get("values", []) or []:
        uid = (str(r[0]).strip() if len(r) > 0 else "")
        d   = parse_date(r[1]) if len(r) > 1 else None
        if uid:
            joining_map[uid] = d
    return joining_map

# ==== 入社“年月”の正規化（O列→YYYY年MM月 / Oが空ならB列で補完） ====
def normalize_join_month_text(value, date_fallback):
    def _try_dt_from_any(x):
        try:
            return parse_date(x)
        except Exception:
            return None

    if value is None or (isinstance(value, str) and value.strip() == ""):
        return date_fallback.strftime("%Y年%m月") if date_fallback else ""

    dt = _try_dt_from_any(value)
    if dt:
        return dt.strftime("%Y年%m月")

    s = str(value).strip()

    m = re.search(r"^\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*$", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        return f"{y}年{max(1,min(12,mo)):02d}月"

    m = re.search(r"^\s*(\d{4})\s*[-/\.]\s*(\d{1,2})\s*$", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        return f"{y}年{max(1,min(12,mo)):02d}月"

    m = re.search(r"^\s*(\d{4})(\d{2})\s*$", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        return f"{y}年{max(1,min(12,mo)):02d}月"

    return date_fallback.strftime("%Y年%m月") if date_fallback else ""

# ==== 名簿：入社日/入社年月 まとめ読み ====
def load_join_maps(sheets):
    """
    戻り値:
      joining_map   {user_id(str): 入社日(datetime)}
      joinmonth_map {user_id(str): 'YYYY年MM月'}
    """
    resp = _with_retry(
        sheets.values().get(
            spreadsheetId=TARGET_ID,
            range=f"{MEIBO_SHEET}!A2:O",  # A=TN_ID, B=入社日, ... O=入社年月
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute
    )
    joining_map = {}
    joinmonth_map = {}
    for r in (resp.get("values", []) or []):
        uid = (str(r[0]).strip() if len(r) > 0 else "")
        jd  = parse_date(r[1]) if len(r) > 1 else None
        ymv = r[14] if len(r) > 14 else ""
        if uid:
            joining_map[uid]   = jd
            joinmonth_map[uid] = normalize_join_month_text(ymv, jd)
    return joining_map, joinmonth_map

# ==== コピー + 計算（複数ソース / 1回限りソースは固定化） ====
def copy_and_calc(sheets, joining_map, joinmonth_map, start_row_offset: int = 0):
    values = sheets.values()
    _ensure_meta_sheet(sheets)

    # 既存の処理済みキー（one-time のフラグ）を読む
    done_keys = set()
    rows = values.get(spreadsheetId=TARGET_ID, range=f"{META_SHEET}!A2:B").execute().get("values", []) or []
    for r in rows:
        if len(r) > 0 and r[0].startswith("DONE::"):
            done_keys.add(r[0])

    # 書き込み開始行（ヘッダ行の次=2 + 固定行ぶん）
    next_dst = 2 + max(0, int(start_row_offset))
    total = 0
    seen_anken = set()  # 案件ID(B列=2)で重複排除

    shukyaku = {'CMS','EC','CMS/SEO','EC/SEO','SEO','Easier'}
    kyujin   = {'イツザイ','イツザイライト','イツザイ/助成金','エージェント','イツザイエージェント',
                'JOBY','イツザイ/JOBY','イツザイエージェント/JOBY','イツザイ/イツザイエージェント'}

    src_start_col   = SRC_MIN_COL
    src_window_cols = SRC_WINDOW_NUM_COLS

    # --- 追加: 文字正規化（全角→半角、空白除去、全角スラッシュ対応） ---
    import unicodedata
    def _norm(s: str) -> str:
        if s is None:
            return ""
        s = str(s)
        # 全角→半角
        s = unicodedata.normalize("NFKC", s)
        # 全角スラッシュが残っても半角に寄せる（normalizeで大半は直るが保険）
        s = s.replace("／", "/")
        # 前後空白
        return s.strip()

    for src in SOURCES:
        src_id = src["id"]; src_sheet = src["sheet"]; one_time = bool(src.get("one_time"))
        key = _done_key(src_id, src_sheet)
        if one_time and key in done_keys:
            print(f"[SKIP] one-time source already imported: {src_id}/{src_sheet}")
            continue

        last_row = get_last_row_by_col(sheets, src_id, src_sheet, 2)
        print(f"[SRC] {src_id}/{src_sheet} last_row(by B)={last_row}")
        if last_row <= 1:
            if one_time and key not in done_keys:
                _meta_set(values, key, "done-empty")
            continue

        chunk = BATCH_SIZE_COPY_INIT
        next_src = 2
        wrote_from_this_source = 0

        while next_src <= last_row:
            num = min(chunk, last_row - next_src + 1)
            src_range = a1_range(src_sheet, next_src, src_start_col, num, src_window_cols)

            try:
                resp = _with_retry(values.get(
                    spreadsheetId=src_id, range=src_range,
                    majorDimension="ROWS", valueRenderOption="UNFORMATTED_VALUE"
                ).execute)
                rows_wide = resp.get("values", [])

                out_copy, out_calc = [], []
                rel_indices = [c - src_start_col for c in COLS_PACKED]

                for r in range(num):
                    src_row = rows_wide[r] if r < len(rows_wide) else []
                    packed = []
                    for idx in rel_indices:
                        v = src_row[idx] if idx < len(src_row) else ""
                        packed.append("" if v is None else v)

                    # 重複除外（案件ID）
                    anken_id = str(gv_from_packed(packed, 2, "")).strip()
                    if anken_id:
                        if anken_id in seen_anken:
                            continue
                        seen_anken.add(anken_id)

                    out_copy.append(packed)

                    # 計算列
                    user_id            = str(gv_from_packed(packed, 12, "")).strip()
                    proposal           = _norm(gv_from_packed(packed, 25, ""))
                    meeting_format     = _norm(gv_from_packed(packed, 27, ""))
                    apo_date           = parse_date(gv_from_packed(packed, 29, ""))
                    meeting_sched_date = parse_date(gv_from_packed(packed, 30, ""))
                    first_meet_post    = parse_date(gv_from_packed(packed, 35, ""))
                    order_post         = parse_date(gv_from_packed(packed, 46, ""))
                    status             = str(gv_from_packed(packed, 49, "")).strip()

                    # ▼ AX（フック）：S(19)・R(18)（原シート列番号で参照）＋正規化
                    s_val = _norm(gv_from_packed(packed, 26, ""))
                    r_val = _norm(gv_from_packed(packed, 25, ""))

                    def hook_from_sr(s, r):
                        """
                        S=フック列(Z=26), R=提案内容(Y=25)
                        """
                        # 両方イツザイエージェント → イツザイエージェント
                        if s == "イツザイエージェント" and r == "イツザイエージェント":
                            return "イツザイエージェント"

                        # 両方Easier → Easier
                        if s == "Easier" and r == "Easier":
                            return "Easier"

                        # 提案内容=Easier × フック=W → ウェルEasier
                        if r == "Easier" and s == "W":
                            return "ウェルEasier"

                        # 従来ルール
                        if s == "W" and r in ("CMS", "CMS/SEO"):
                            return "ウェルCMS"
                        if s == "W" and r == "EC":
                            return "ウェルEC"
                        if s == "イツザイ" or r == "イツザイ":
                            return "イツザイ"
                        if s == "なし" or r == "CMS":
                            return "WEB直販"

                        return ""


                    hook_ax = hook_from_sr(s_val, r_val)

                    getsudoApo = format_getsudo(apo_date)
                    getsudoSei = format_getsudo(first_meet_post)
                    getsudoOrd = format_getsudo(order_post)

                    flagSeiritsu = "成立" if status in ["失注", "追客中", "受注済"] else ""
                    if proposal in shukyaku:
                        flagShozai = "集客"
                    elif proposal in kyujin:
                        flagShozai = "求人"
                    else:
                        flagShozai = ""

                    leadTime, leadTimeJudge = "", ""
                    if apo_date and meeting_sched_date:
                        diff = (meeting_sched_date - apo_date).days
                        leadTime = max(0, diff)
                        leadTimeJudge = "直近" if leadTime <= 3 else "先"

                    if meeting_format == "オンライン":
                        formatJudge = "オンライン"
                    elif meeting_format in ["訪問", "カフェ", "来社"]:
                        formatJudge = "対面"
                    else:
                        formatJudge = ""

                    tenure, tenureFlag = "", ""
                    if user_id and apo_date:
                        joining = joining_map.get(user_id)
                        if joining and joining <= apo_date:
                            months = (apo_date - joining).days // 30
                            tenure = months
                            if months <= 3:   tenureFlag = "0〜3ヶ月目"
                            elif months <= 6: tenureFlag = "4〜6ヶ月目"
                            elif months <= 9: tenureFlag = "7〜9ヶ月目"
                            elif months <= 12: tenureFlag = "10〜12ヶ月目"
                            elif months <= 24: tenureFlag = "1〜2年"
                            elif months <= 36: tenureFlag = "2〜3年"
                            elif months <= 48: tenureFlag = "3〜4年"
                            else:             tenureFlag = "4年以上"

                    join_month = joinmonth_map.get(user_id, "")

                    # ★ AM..AW(11列) + AX(フック)=12列
                    #   未該当時も "-" を入れて“AX まで必ず書く”。確認後は "" に戻してOK。
                    out_calc.append([
                        getsudoApo, getsudoSei, getsudoOrd, flagSeiritsu, flagShozai,
                        leadTime, leadTimeJudge, formatJudge, tenure, tenureFlag,
                        join_month, (hook_ax if hook_ax != "" else "-")
                    ])

                if out_copy:
                    copy_range = a1_range(TARGET_SHEET, next_dst, 1, len(out_copy), PACKED_LAST_COL)
                    calc_range = a1_range(TARGET_SHEET, next_dst, WRITE_START, len(out_calc), NUM_CALC_COLS)

                    end_row_needed = next_dst + len(out_copy) - 1
                    ensure_target_rows(sheets, end_row_needed)

                    _with_retry(values.batchUpdate(
                        spreadsheetId=TARGET_ID,
                        body={"valueInputOption":"RAW","data":[
                            {"range": copy_range, "values": out_copy},
                            {"range": calc_range, "values": out_calc},
                        ]}
                    ).execute)
                    wrote_from_this_source += len(out_copy)
                    total += len(out_copy)
                    print(f"[COPY+CALC] {src_id}/{src_sheet} src {next_src}-{next_src+len(out_copy)-1} -> dst {next_dst}-{next_dst+len(out_copy)-1}")
                    next_dst += len(out_copy)

                next_src += num
                if chunk < BATCH_SIZE_COPY_INIT:
                    chunk = min(BATCH_SIZE_COPY_INIT, chunk + 500)

            except (TimeoutError, socket.timeout, HttpError) as e:
                if isinstance(e, HttpError):
                    status = e.resp.status if getattr(e, "resp", None) else None
                    if status not in (429,500,502,503,504):
                        raise
                new_chunk = max(BATCH_SIZE_COPY_MIN, chunk // 2)
                if new_chunk == chunk:
                    raise
                print(f"[ADAPT] chunk {chunk} -> {new_chunk} due to {type(e).__name__}")
                chunk = new_chunk

        # one-time は初回だけ：処理済みフラグ＆固定行に加算（start_row_offset=0 の時）
        if one_time and key not in done_keys:
            _meta_set(values, key, "done")
            if wrote_from_this_source > 0 and start_row_offset == 0:
                _add_frozen_rows(values, wrote_from_this_source)
            print(f"[ONCE] marked done: {key} (wrote {wrote_from_this_source} rows)")

    print(f"[DONE] total_written_rows={total}")
    return total



# ==== main ====
def main():
    # ★ サービスアカウント鍵で認証
    creds = get_sa_creds()

    # タイムアウト付き HTTP（既存の常識的な構成）
    base_http = httplib2.Http(timeout=HTTP_TIMEOUT_SEC)
    authed_http = AuthorizedHttp(creds, http=base_http)

    # build は「http=authed_http」か「credentials=creds」のどちらか一方だけ
    service = build("sheets", "v4", http=authed_http)
    sheets = service.spreadsheets()

    # 列数の確保（AM から 12 列分も入るように＝AXまで）
    min_cols = max(PACKED_LAST_COL, WRITE_START + NUM_CALC_COLS - 1)  # 39+12-1=50（AX）
    grid_cols = ensure_target_columns(sheets, min_columns=min_cols)

    # メタ初期化 & 固定化行数の取得
    _ensure_meta_sheet(sheets)
    values_service = sheets.values()
    frozen = _get_frozen_rows(values_service)
    print(f"[META] frozen top rows = {frozen}")

    # クリア：固定行は残し、それ以降だけ消す
    clear_target_before_run(sheets, grid_columns=grid_cols, preserve_top_rows=frozen)

    # 名簿ロード（入社日 & 入社年月）
    joining_map, joinmonth_map = load_join_maps(sheets)

    # コピー + 計算（複数ソース・固定行の下に積む）
    print("Step: コピー + 計算 開始")
    total = copy_and_calc(sheets, joining_map, joinmonth_map, start_row_offset=frozen)
    print("Step: コピー + 計算 完了")


if __name__ == "__main__":
    main()
# ===== ここまで =====
