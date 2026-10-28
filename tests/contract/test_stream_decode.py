from backend.adapters.gateway import visible_markdown


def test_json_stream_decodes_only_received_visible_content():
    assert visible_markdown('{"content_markdown":"你好\\nwor') == "你好\nwor"
    assert visible_markdown('{"content_markdown":"hello\\u4f') == "hello"
    assert visible_markdown('{"content_markdown":"hello\\u4f60') == "hello你"
    assert visible_markdown('{"content_markdown":"a\\ud83d') == "a"
    assert visible_markdown('{"content_markdown":"a\\ud83d\\ude00') == "a😀"
    assert visible_markdown('{"content_markdown":"done","candidate":{"text":"hidden"}}') == "done"
    assert visible_markdown('{"candidate":') == ""
    assert visible_markdown("A real unstructured assistant sentence") == "A real unstructured assistant sentence"
