from pathlib import Path
from typing import Tuple
import boto3
from botocore.exceptions import NoCredentialsError, ClientError


def s3_key_for(file_path: Path, base_dir: Path, prefix: str) -> str:
    rel = file_path.relative_to(base_dir).as_posix()
    prefix = prefix.strip("/")
    return f"{prefix}/{rel}" if prefix else rel


def upload_dir_to_s3(
    bucket: str,
    local_dir: str = "data/gold",
    prefix: str = "dwh/gold",
    profile: str = "",
) -> Tuple[int, int]:

    base_dir = Path(local_dir).resolve()

    if not base_dir.exists():
        raise ValueError(f"Folder not found: {base_dir}")

    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile

    session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
    s3 = session.client("s3")

    files = [p for p in base_dir.rglob("*") if p.is_file()]
    if not files:
        return (0, 0)

    ok, fail = 0, 0

    for file_path in files:
        key = s3_key_for(file_path, base_dir, prefix)

        try:
            s3.upload_file(str(file_path), bucket, key)
            ok += 1
        except NoCredentialsError:
            raise RuntimeError("AWS credentials not found.")
        except ClientError:
            fail += 1

    return ok, fail