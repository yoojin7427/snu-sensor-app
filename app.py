import os
import io
import csv
import zipfile
import shutil
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from datetime import datetime, date, time, timedelta, timezone

import streamlit as st
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound

from avro.datafile import DataFileReader
from avro.io import DatumReader


# =========================
# ✅ 고정 설정
# =========================
BUCKET = "empatica-us-east-1-prod-data"
REGION = "us-east-1"
DEFAULT_PROG = "1"
DEFAULT_SITE = "1"

APP_DIR = os.path.abspath(os.path.dirname(__file__))
# Streamlit Cloud 등 서버 환경을 고려해 임시 폴더 경로를 안전하게 설정
CACHE_DIR = os.path.join(APP_DIR, "_cache_avro")
OUT_DIR = os.path.join(APP_DIR, "_output_csv")

KST = timezone(timedelta(hours=9))

# ✅ 기기(색상) ↔ org/profile 매핑
DEVICE_MAP = {
    "🔵 Blue": {"org": "2244", "profile": "deviceA"},
    "🟢 Green": {"org": "2602", "profile": "deviceB"},
}

# Blue 기본 participant(있으면 자동 선택)
BLUE_DEFAULT_PARTICIPANT = "VRPILOT001-3YKC51P1C9"


# =========================
# 데이터 클래스
# =========================
@dataclass
class Window:
    start_kst: datetime
    end_kst: datetime

    @property
    def start_us(self) -> int:
        return int(self.start_kst.astimezone(timezone.utc).timestamp() * 1_000_000)

    @property
    def end_us(self) -> int:
        return int(self.end_kst.astimezone(timezone.utc).timestamp() * 1_000_000)


# =========================
# 유틸
# =========================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def kst_datetime(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=KST)


def us_to_kst_str(us: int) -> str:
    """epoch microseconds(UTC) -> KST string"""
    dt_utc = datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc)
    return dt_utc.astimezone(KST).strftime("%Y.%m.%d %H:%M:%S")


def compute_timestamp_us(start_us: Optional[int], freq_hz: Optional[float], count: int) -> List[int]:
    """freq_hz == 0/None 방어 + 깨진 값 방어"""
    try:
        if start_us is None or count is None or count <= 0:
            return []
        if freq_hz is None:
            return []
        f = float(freq_hz)
        if f <= 0:
            return []
        step = 1e6 / f
        return [int(round(start_us + i * step)) for i in range(int(count))]
    except Exception:
        return []


def clip_rows_by_us(rows: List[List], idx_ts: int, start_us: int, end_us: int) -> List[List]:
    return [r for r in rows if start_us <= int(r[idx_ts]) < end_us]


# =========================
# S3 경로/탐색/다운로드
# =========================
def s3_prefix(org: str, prog: str, site: str, date_str: str, participant: str) -> str:
    return f"v2/{org}/{prog}/{site}/participant_data/{date_str}/{participant}/raw_data/v6/"


def participant_root_prefix(org: str, prog: str, site: str, date_str: str) -> str:
    return f"v2/{org}/{prog}/{site}/participant_data/{date_str}/"


def list_participant_folders(s3, org: str, prog: str, site: str, date_str: str) -> List[str]:
    """
    participant_data/{DATE}/ 아래의 폴더(=participant) 목록을 가져옴
    list_objects_v2 + Delimiter="/" 방식
    """
    prefix = participant_root_prefix(org, prog, site, date_str)
    folders = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            p = cp.get("Prefix", "")
            part = p.rstrip("/").split("/")[-1]
            if part:
                folders.append(part)

    return sorted(set(folders))


def list_avro_keys(s3, prefix: str) -> List[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj.get("Key", "")
            if k.lower().endswith(".avro"):
                keys.append(k)
    return keys


def download_avro_files(s3, prefix: str, local_dir: str) -> List[str]:
    ensure_dir(local_dir)
    keys = list_avro_keys(s3, prefix)
    if not keys:
        return []

    local_paths = []
    for key in keys:
        fname = os.path.basename(key)
        dst = os.path.join(local_dir, fname)

        # 캐시: 이미 있으면 재다운로드 생략
        if not os.path.exists(dst) or os.path.getsize(dst) == 0:
            s3.download_file(BUCKET, key, dst)
        local_paths.append(dst)

    return sorted(local_paths)


# =========================
# AVRO → 센서 적재
# =========================
def load_sensor_data_from_avros(avro_files: List[str]) -> Tuple[Dict[str, List[List]], List[Tuple[str, str, str]]]:
    sensor_data = defaultdict(list)
    skipped_notes = []  # (file, sensor, reason)

    for avro_file in avro_files:
        try:
            reader = DataFileReader(open(avro_file, "rb"), DatumReader())
            data = next(reader)
            reader.close()
        except Exception as e:
            skipped_notes.append((os.path.basename(avro_file), "FILE", f"read_fail: {e}"))
            continue

        raw = (data or {}).get("rawData", {})

        # Accelerometer (digital->physical)
        if "accelerometer" in raw:
            try:
                acc = raw["accelerometer"]
                xs = acc.get("x", [])
                ys = acc.get("y", [])
                zs = acc.get("z", [])
                ts = compute_timestamp_us(acc.get("timestampStart"), acc.get("samplingFrequency"), len(xs))
                if not ts:
                    skipped_notes.append((os.path.basename(avro_file), "accelerometer", "freq<=0 or empty"))
                else:
                    p = acc.get("imuParams", {})
                    dp = (p.get("physicalMax", 0) - p.get("physicalMin", 0))
                    dd = (p.get("digitalMax", 0) - p.get("digitalMin", 0))
                    if dd == 0:
                        skipped_notes.append((os.path.basename(avro_file), "accelerometer", "digital_range=0"))
                    else:
                        x_g = [(v * dp / dd) for v in xs]
                        y_g = [(v * dp / dd) for v in ys]
                        z_g = [(v * dp / dd) for v in zs]
                        sensor_data["accelerometer"].extend([[t, x, y, z] for t, x, y, z in zip(ts, x_g, y_g, z_g)])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "accelerometer", f"parse_fail: {e}"))

        # Gyroscope
        if "gyroscope" in raw:
            try:
                gyro = raw["gyroscope"]
                xs = gyro.get("x", [])
                ys = gyro.get("y", [])
                zs = gyro.get("z", [])
                ts = compute_timestamp_us(gyro.get("timestampStart"), gyro.get("samplingFrequency"), len(xs))
                if not ts:
                    skipped_notes.append((os.path.basename(avro_file), "gyroscope", "freq<=0 or empty"))
                else:
                    sensor_data["gyroscope"].extend([[t, x, y, z] for t, x, y, z in zip(ts, xs, ys, zs)])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "gyroscope", f"parse_fail: {e}"))

        # EDA
        if "eda" in raw:
            try:
                eda = raw["eda"]
                vals = eda.get("values", [])
                ts = compute_timestamp_us(eda.get("timestampStart"), eda.get("samplingFrequency"), len(vals))
                if not ts:
                    skipped_notes.append((os.path.basename(avro_file), "eda", "freq<=0 or empty"))
                else:
                    sensor_data["eda"].extend([[t, v] for t, v in zip(ts, vals)])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "eda", f"parse_fail: {e}"))

        # Temperature
        if "temperature" in raw:
            try:
                tmp = raw["temperature"]
                vals = tmp.get("values", [])
                ts = compute_timestamp_us(tmp.get("timestampStart"), tmp.get("samplingFrequency"), len(vals))
                if not ts:
                    skipped_notes.append((os.path.basename(avro_file), "temperature", "freq<=0 or empty"))
                else:
                    sensor_data["temperature"].extend([[t, v] for t, v in zip(ts, vals)])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "temperature", f"parse_fail: {e}"))

        # BVP
        if "bvp" in raw:
            try:
                bvp = raw["bvp"]
                vals = bvp.get("values", [])
                ts = compute_timestamp_us(bvp.get("timestampStart"), bvp.get("samplingFrequency"), len(vals))
                if not ts:
                    skipped_notes.append((os.path.basename(avro_file), "bvp", "freq<=0 or empty"))
                else:
                    sensor_data["bvp"].extend([[t, v] for t, v in zip(ts, vals)])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "bvp", f"parse_fail: {e}"))

        # Steps
        if "steps" in raw:
            try:
                steps = raw["steps"]
                vals = steps.get("values", [])
                ts = compute_timestamp_us(steps.get("timestampStart"), steps.get("samplingFrequency"), len(vals))
                if not ts:
                    skipped_notes.append((os.path.basename(avro_file), "steps", "freq<=0 or empty"))
                else:
                    sensor_data["steps"].extend([[t, v] for t, v in zip(ts, vals)])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "steps", f"parse_fail: {e}"))

        # Tags (micros)
        if "tags" in raw:
            try:
                tags = raw["tags"]
                for tag_us in tags.get("tagsTimeMicros", []):
                    sensor_data["tags"].append([int(tag_us)])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "tags", f"parse_fail: {e}"))

        # Systolic Peaks (nanos -> micros)
        if "systolicPeaks" in raw:
            try:
                sps = raw["systolicPeaks"]
                for ns in sps.get("peaksTimeNanos", []):
                    sensor_data["systolic_peaks"].append([int(ns) // 1000])
            except Exception as e:
                skipped_notes.append((os.path.basename(avro_file), "systolic_peaks", f"parse_fail: {e}"))

    for k in sensor_data.keys():
        sensor_data[k].sort(key=lambda r: r[0])

    return sensor_data, skipped_notes


# =========================
# CSV 저장(time_kst 컬럼 추가)
# =========================
def save_csv(path: str, header: List[str], rows: List[List]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def export_window_to_csv(
    out_root: str,
    participant: str,
    date_str: str,
    window: Window,
    sensor_data: Dict[str, List[List]],
) -> str:
    out_dir = os.path.join(out_root, participant, date_str)
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    ensure_dir(out_dir)

    s_us, e_us = window.start_us, window.end_us

    def with_time_kst(rows_raw: List[List], ts_index: int = 0) -> List[List]:
        out = []
        for r in rows_raw:
            ts = int(r[ts_index])
            out.append([us_to_kst_str(ts), ts] + [c for i, c in enumerate(r) if i != ts_index])
        return out

    if "accelerometer" in sensor_data:
        rows = clip_rows_by_us(sensor_data["accelerometer"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "accelerometer.csv"),
                     ["time_kst", "ts_us", "x", "y", "z"], with_time_kst(rows, 0))

    if "gyroscope" in sensor_data:
        rows = clip_rows_by_us(sensor_data["gyroscope"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "gyroscope.csv"),
                     ["time_kst", "ts_us", "x", "y", "z"], with_time_kst(rows, 0))

    if "eda" in sensor_data:
        rows = clip_rows_by_us(sensor_data["eda"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "eda.csv"),
                     ["time_kst", "ts_us", "eda"], with_time_kst(rows, 0))

    if "temperature" in sensor_data:
        rows = clip_rows_by_us(sensor_data["temperature"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "temperature.csv"),
                     ["time_kst", "ts_us", "temperature"], with_time_kst(rows, 0))

    if "bvp" in sensor_data:
        rows = clip_rows_by_us(sensor_data["bvp"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "bvp.csv"),
                     ["time_kst", "ts_us", "bvp"], with_time_kst(rows, 0))

    if "steps" in sensor_data:
        rows = clip_rows_by_us(sensor_data["steps"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "steps.csv"),
                     ["time_kst", "ts_us", "steps"], with_time_kst(rows, 0))

    if "tags" in sensor_data:
        rows = clip_rows_by_us(sensor_data["tags"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "tags.csv"),
                     ["time_kst", "ts_us"], with_time_kst(rows, 0))

    if "systolic_peaks" in sensor_data:
        rows = clip_rows_by_us(sensor_data["systolic_peaks"], 0, s_us, e_us)
        if rows:
            save_csv(os.path.join(out_dir, "systolic_peaks.csv"),
                     ["time_kst", "ts_us"], with_time_kst(rows, 0))

    return out_dir


def make_zip_from_dir(root_dir: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for folder, _, files in os.walk(root_dir):
            for f in files:
                abs_path = os.path.join(folder, f)
                rel_path = os.path.relpath(abs_path, root_dir)
                z.write(abs_path, rel_path)
    buf.seek(0)
    return buf.read()


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="EmbracePlus Downloader", layout="wide")
st.title("EmbracePlus 데이터 다운로드 + CSV 변환 (단일 시간범위, KST 표기)")

# 🚨 수정된 부분: 사전 준비 설명 제거 후 경고 문구 추가
st.warning("⚠️ **주의!** 출발 및 종료 시간을 정확하게 기입해서 다운로드해 주세요.")

# --- Device(색상) 선택 ---
device_choice = st.selectbox("Device 선택", list(DEVICE_MAP.keys()), index=0)
org = DEVICE_MAP[device_choice]["org"]
aws_profile = DEVICE_MAP[device_choice]["profile"]

# --- ORG/PROG/SITE/DATE ---
c1, c2, c3 = st.columns(3)
with c1:
    st.text_input("ORG (자동)", value=org, disabled=True)
    prog = st.text_input("PROG", value=DEFAULT_PROG)
with c2:
    site = st.text_input("SITE", value=DEFAULT_SITE)
with c3:
    d = st.date_input("DATE (KST 기준)", value=date.today())  # ✅ 기본은 오늘
    date_str = d.strftime("%Y-%m-%d")

# --- S3 client 준비 ---
s3_client = None
s3_err = None

# 현재 선택된 기기의 프로필 이름 (deviceA 또는 deviceB)
target_secret_name = DEVICE_MAP[device_choice]["profile"]

try:
    # 1. 서버의 비밀노트(Secrets)에서 키를 가져옴
    if target_secret_name in st.secrets:
        creds = st.secrets[target_secret_name]
        
        # 2. 가져온 키로 S3 연결 (boto3.client 직접 생성)
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            region_name="us-east-1"
        )
    else:
        s3_err = f"Secrets에 '{target_secret_name}' 정보가 없습니다."

except Exception as e:
    s3_err = f"AWS 연결 실패: {e}"

# --- participant 목록 자동 탐색 ---
participants = []
if s3_client and not s3_err:
    try:
        participants = list_participant_folders(s3_client, org, prog, site, date_str)
    except ClientError as e:
        s3_err = f"S3 목록 조회 실패: {e}"
    except Exception as e:
        s3_err = f"참가자 목록 로드 실패: {e}"

st.subheader("참가자 선택 (S3 자동 탐색)")

if s3_err:
    st.warning(s3_err)

if participants:
    # Blue면 기본 participant가 목록에 있으면 그걸 기본 선택
    default_idx = 0
    if device_choice.startswith("🔵") and BLUE_DEFAULT_PARTICIPANT in participants:
        default_idx = participants.index(BLUE_DEFAULT_PARTICIPANT)

    participant = st.selectbox(
        "PARTICIPANT (S3 폴더명)",
        participants,
        index=default_idx
    )
else:
    # 목록이 없거나 못 불러온 경우 수동 입력 fallback
    fallback_default = BLUE_DEFAULT_PARTICIPANT if device_choice.startswith("🔵") else ""
    participant = st.text_input("PARTICIPANT (수동 입력, S3 폴더명 그대로)", value=fallback_default)

# --- 시간 범위 ---
st.subheader("시간 범위 (KST, 한국시간)")

# 체크박스와 복잡한 if/else 로직을 제거하고, 심플하게 배치
tc1, tc2 = st.columns(2)

with tc1:
    # time_input은 기본적으로 키보드 입력과 클릭 선택 모두 지원합니다.
    t_start = st.time_input("시작", value=time(10, 0), key="t_start_picker")

with tc2:
    t_end = st.time_input("종료", value=time(11, 0), key="t_end_picker")

st.caption("※ 내부적으로는 UTC epoch(마이크로초)로 변환해서 정확히 자릅니다. CSV에는 time_kst 컬럼을 추가합니다.")

run = st.button("🚀 다운로드 + 변환 실행", type="primary")

if run:
    if s3_err or not s3_client:
        st.error("S3 연결이 준비되지 않았어요. 위 경고를 해결해 주세요.")
        st.stop()

    if not participant.strip():
        st.error("PARTICIPANT가 비어있어요.")
        st.stop()

    start_dt = kst_datetime(d, t_start)
    end_dt = kst_datetime(d, t_end)
    if end_dt <= start_dt:
        st.error("종료 시간이 시작 시간보다 빠르거나 같아요.")
        st.stop()

    window = Window(start_kst=start_dt, end_kst=end_dt)
    prefix = s3_prefix(org, prog, site, date_str, participant)

    st.write(f"🎨 선택: **{device_choice}** |  🔐 profile: `{aws_profile}`")
    st.write(f"📌 S3 Prefix: `{prefix}`")

    local_cache = os.path.join(CACHE_DIR, device_choice.replace(" ", "_"), participant, date_str)
    ensure_dir(local_cache)

    with st.status("S3에서 AVRO 다운로드 중...", expanded=True) as status:
        try:
            avro_files = download_avro_files(s3_client, prefix, local_cache)
            if not avro_files:
                status.update(label="AVRO 파일을 찾지 못했습니다.", state="error")
                st.error("해당 경로에 .avro가 없어요. participant/date 경로가 맞는지 확인해 주세요.")
                st.stop()

            st.write(f"✅ AVRO {len(avro_files)}개 준비됨 (캐시: `{local_cache}`)")
            status.update(label="다운로드 완료", state="complete")
        except ClientError as e:
            st.error(f"S3 접근 실패: {e}")
            st.stop()

    with st.status("AVRO 읽는 중...", expanded=True) as status:
        sensor_data, skipped = load_sensor_data_from_avros(avro_files)
        status.update(label=f"AVRO 로드 완료 (센서 {len(sensor_data)}종)", state="complete")

    with st.status("시간 범위로 자르고 CSV 생성 중...", expanded=True) as status:
        out_dir = export_window_to_csv(OUT_DIR, participant, date_str, window, sensor_data)
        status.update(label="CSV 생성 완료", state="complete")

    st.subheader("결과 요약")
    st.write(f"📂 저장 위치(로컬): `{out_dir}`")
    st.write(
        f"⏱️ 범위(KST): {start_dt.strftime('%Y.%m.%d %H:%M:%S')} ~ {end_dt.strftime('%Y.%m.%d %H:%M:%S')}"
        f"  (epoch_us {window.start_us} ~ {window.end_us})"
    )

    if skipped:
        with st.expander(f"⚠️ 일부 센서/파일 스킵됨 ({len(skipped)}건) — 그래도 전체 변환은 완료", expanded=False):
            st.caption("samplingFrequency=0, 디지털 범위=0, 파일 손상 등으로 일부 센서가 스킵될 수 있어요.")
            st.dataframe(
                [{"file": a, "sensor": b, "reason": c} for a, b, c in skipped],
                use_container_width=True
            )

    zip_bytes = make_zip_from_dir(out_dir)
    zip_name = f"{participant}_{date_str}_{t_start.strftime('%H%M')}-{t_end.strftime('%H%M')}_KST_csv.zip"
    st.download_button(
        label="⬇️ CSV ZIP 다운로드",
        data=zip_bytes,
        file_name=zip_name,
        mime="application/zip",
    )

st.divider()
with st.expander("🧯 자주 나는 에러 / 해결"):
    st.markdown(
        f"""
- **profile 없음**: `aws configure --profile deviceA` 또는 `aws configure --profile deviceB`  
- **참가자 목록이 비어 있음**: (1) DATE가 틀림 (2) PROG/SITE가 다름 (3) 해당 날짜에 업로드 아직 안 됨  
- **AVRO 파일 0개**: participant/date 경로가 실제 S3 폴더명과 정확히 일치해야 함  
- **time_kst가 이상함**: tags.csv로 KST 변환이 맞는지 확인  
        """
    )
