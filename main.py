import asyncio, aiohttp, json, os, re, subprocess
from datetime import datetime, timezone
from io import BytesIO
from PIL import Image
import piexif
from tqdm import tqdm

OUTPUT_DIR = "downloads"

EXT_BY_CT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
    "image/webp": ".webp", "video/mp4": ".mp4", "video/quicktime": ".mov",
}

def parse_dt_utc_str(s: str):
    dt = datetime.strptime(s.replace(" UTC",""), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    exif_dt = dt.strftime("%Y:%m:%d %H:%M:%S")
    iso_z = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return exif_dt, iso_z

def parse_lat_lon(loc: str):
    m = re.search(r"Latitude,\s*Longitude:\s*([+\-]?\d+(?:\.\d+)?)\s*,\s*([+\-]?\d+(?:\.\d+)?)", loc or "")
    if not m: return None
    lat = float(m.group(1)); lon = float(m.group(2))
    if lat == 0.0 and lon == 0.0: return None
    return lat, lon

def to_dms_rationals(deg_float):
    sign = 1 if deg_float >= 0 else -1
    deg = abs(deg_float)
    d = int(deg)
    m_float = (deg - d) * 60
    m = int(m_float)
    s = round((m_float - m) * 60 * 10000)
    return sign, ((d,1), (m,1), (s,10000))

async def download_and_tag(session, item):
    url = item["Media Download Url"]
    date_str = item.get("Date","").strip()
    media_type = item.get("Media Type","").strip()
    loc = item.get("Location","") or ""

    exif_dt, iso_z = parse_dt_utc_str(date_str) if date_str else (None, None)
    latlon = parse_lat_lon(loc)

    async with session.get(url) as resp:
        if resp.status != 200:
            tqdm.write(f"Failed: {resp.status} {url}")
            return
        data = await resp.read()
        ct = resp.headers.get("Content-Type","").split(";")[0].strip().lower()
        ext = EXT_BY_CT.get(ct)
        if not ext:
            if data[:3] == b"\xff\xd8\xff": ext = ".jpg"
            elif data[:4] == b"\x89PNG": ext = ".png"
            elif data[:4] == b"RIFF" and data[8:12] == b"WEBP": ext = ".webp"
            elif data[4:8] == b"ftyp": ext = ".mp4"
            else: ext = ".bin"

        base = date_str.replace(":","-").replace(" ","_") if date_str else "file"
        filepath = os.path.join(OUTPUT_DIR, base + ext)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(filepath, "wb") as f:
            f.write(data)

    if ext == ".jpg":
        try:
            im = Image.open(filepath)
            exif_dict = {"0th":{}, "Exif":{}, "GPS":{}, "1st":{}, "thumbnail":None}
            if exif_dt:
                exif_dict["0th"][piexif.ImageIFD.DateTime] = exif_dt
                exif_dict["Exif"][piexif.ExifIFD.DateTimeOriginal] = exif_dt
                exif_dict["Exif"][piexif.ExifIFD.DateTimeDigitized] = exif_dt
            if latlon:
                lat, lon = latlon
                lat_sign, lat_dms = to_dms_rationals(lat)
                lon_sign, lon_dms = to_dms_rationals(lon)
                exif_dict["GPS"][piexif.GPSIFD.GPSLatitudeRef] = b"N" if lat_sign >= 0 else b"S"
                exif_dict["GPS"][piexif.GPSIFD.GPSLatitude] = lat_dms
                exif_dict["GPS"][piexif.GPSIFD.GPSLongitudeRef] = b"E" if lon_sign >= 0 else b"W"
                exif_dict["GPS"][piexif.GPSIFD.GPSLongitude] = lon_dms
                exif_dict["GPS"][piexif.GPSIFD.GPSVersionID] = (2,3,0,0)
            exif_bytes = piexif.dump(exif_dict)
            im.save(filepath, "jpeg", exif=exif_bytes)
        except Exception as e:
            tqdm.write(f"EXIF failed for {os.path.basename(filepath)}: {e}")

    elif ext in (".mp4", ".mov"):
        try:
            iso6709 = None
            if latlon:
                lat, lon = latlon
                iso6709 = f"{lat:+.6f}{lon:+.6f}/"
            tmp = filepath + ".tmp.mp4"
            cmd = ["ffmpeg", "-y", "-i", filepath, "-codec", "copy"]
            if iso_z:
                cmd += ["-metadata", f"creation_time={iso_z}"]
            if date_str or latlon:
                cmd += ["-metadata", f"description=Date: {date_str}; Location: {loc}"]
            if iso6709:
                cmd += ["-metadata", f"com.apple.quicktime.location.ISO6709={iso6709}",
                        "-metadata", f"location={iso6709}"]
            cmd += [tmp]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            os.replace(tmp, filepath)
        except Exception as e:
            tqdm.write(f"MP4 metadata failed for {os.path.basename(filepath)}: {e}")
    else:
        try:
            with open(filepath + ".json", "w", encoding="utf-8") as s:
                json.dump({"date": date_str, "location": loc}, s, ensure_ascii=False, indent=2)
        except Exception as e:
            tqdm.write(f"Sidecar failed for {os.path.basename(filepath)}: {e}")

async def main():
    with open("memories_history.json","r",encoding="utf-8") as f:
        items = json.load(f)["Saved Media"]

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=60, sock_read=300)
    conn = aiohttp.TCPConnector(limit=12)
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        pbar = tqdm(total=len(items), desc="Downloading & tagging", unit="file")
        sem = asyncio.Semaphore(12)

        async def wrapped(it):
            async with sem:
                await download_and_tag(session, it)
                pbar.update(1)

        try:
            await asyncio.gather(*[wrapped(it) for it in items])
        finally:
            pbar.close()

if __name__ == "__main__":
    asyncio.run(main())

