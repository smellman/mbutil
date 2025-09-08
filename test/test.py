import os
import shutil
import json
import pytest
import boto3
from moto import mock_aws
from mbutil.util import mbtiles_to_disk, disk_to_mbtiles, mbtiles_to_s3

@pytest.fixture(autouse=True)
def clear_data_each_test():
    yield
    try:
        shutil.rmtree('test/output')
    except Exception:
        pass

BUCKET = "test-bucket"

@pytest.fixture(autouse=True)
def aws_dummy_creds(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-1")

def test_mbtiles_to_disk():
    mbtiles_to_disk('test/data/one_tile.mbtiles', 'test/output')
    assert os.path.exists('test/output/0/0/0.png')
    assert os.path.exists('test/output/metadata.json')

def test_mbtiles_to_disk_and_back():
    mbtiles_to_disk('test/data/one_tile.mbtiles', 'test/output')
    assert os.path.exists('test/output/0/0/0.png')
    disk_to_mbtiles('test/output/', 'test/output/one.mbtiles')
    assert os.path.exists('test/output/one.mbtiles')

def test_utf8grid_mbtiles_to_disk():
    mbtiles_to_disk('test/data/utf8grid.mbtiles', 'test/output')
    assert os.path.exists('test/output/0/0/0.grid.json')
    assert os.path.exists('test/output/0/0/0.png')
    assert os.path.exists('test/output/metadata.json')

def test_utf8grid_disk_to_mbtiles():
    os.mkdir('test/output')
    mbtiles_to_disk('test/data/utf8grid.mbtiles', 'test/output/original', callback=None)
    disk_to_mbtiles('test/output/original/', 'test/output/imported.mbtiles')
    mbtiles_to_disk('test/output/imported.mbtiles', 'test/output/imported', callback=None)
    assert os.path.exists('test/output/imported/0/0/0.grid.json')
    original = json.load(open('test/output/original/0/0/0.grid.json'))
    imported = json.load(open('test/output/imported/0/0/0.grid.json'))
    assert original['data']['77'] == imported['data']['77'] == {'ISO_A2': 'FR'}

def test_mbtiles_to_disk_utfgrid_callback():
    os.mkdir('test/output')
    callback = {}
    for c in ['null', 'foo']:
        mbtiles_to_disk('test/data/utf8grid.mbtiles', 'test/output/%s' % c, callback=c)
        f = open('test/output/%s/0/0/0.grid.json' % c)
        callback[c] = f.read().split('{')[0]
        f.close()
    assert callback['foo'] == 'foo('
    assert callback['null'] == ''

def test_disk_to_mbtiles_zyx():
    os.mkdir('test/output')
    disk_to_mbtiles('test/data/tiles/zyx', 'test/output/zyx.mbtiles', scheme='zyx', format='png')
    mbtiles_to_disk('test/output/zyx.mbtiles', 'test/output/tiles', callback=None)
    assert os.path.exists('test/output/tiles/3/1/5.png')

@mock_aws
def test_mbtiles_to_s3_uploads_objects():
    region = "ap-northeast-1"
    s3 = boto3.client("s3", region_name=region)
    create_params = {"Bucket": BUCKET}
    if region != "us-east-1":
        create_params["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**create_params)

    mbtiles = os.path.join("test", "data", "one_tile.mbtiles")
    mbtiles_to_s3(
        mbtiles, BUCKET,
        prefix="tiles",
        scheme="xyz",
        format="png",
        cache_control="max-age=60",
        content_encoding="gzip",
    )

    meta = s3.get_object(Bucket=BUCKET, Key="tiles/metadata.json")
    assert meta["ContentType"].startswith("application/json")

    head = s3.head_object(Bucket=BUCKET, Key="tiles/0/0/0.png")
    assert head["ContentType"] == "image/png"
    assert head["CacheControl"] == "max-age=60"

    body = s3.get_object(Bucket=BUCKET, Key="tiles/0/0/0.png")["Body"].read()
    assert isinstance(body, (bytes, bytearray))
    assert len(body) > 0
