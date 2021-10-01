import base64

def test_put_image():
    import odakb.board
    import odakb.datalake

    fn = "image.png"
    data = "test!"
    with open(fn, "w") as f:
        f.write(data)

    bucket_object = odakb.board.upload_image(fn)

    restored_data = odakb.datalake.restore(bucket_object)

    assert data == base64.b64decode(restored_data['image']).decode()