# Fetch web content

The agent's `web_fetch` tool retrieves HTTP resources from the runner host and
converts HTML to readable Markdown. Use it for ordinary page and API requests
instead of starting `curl` through a shell.

## Fetch a page

Example tool arguments:

```json
{"url":"https://example.com/docs","max_bytes":32000}
```

HTML conversion preserves headings, links, lists, code, and tables. By default it
prefers the page's main content and removes navigation and hidden elements. It
does not run JavaScript. Plain text and JSON remain unchanged. Complete supported
images return image content; partial images and other binary formats use base64.
PDFs are not extracted as text.

## Continue reading

Use the returned `next_offset`, not an assumed chunk size:

```json
{"url":"https://example.com/docs","max_bytes":32000,"offset":32000}
```

This example assumes the first result actually returned `next_offset=32000`.
UTF-8 boundaries can make a chunk shorter. Each call refetches the resource rather
than using a cache or HTTP Range request. Compare `content_sha256`; if it changes,
discard the combined content and restart.

Repeating a non-GET request repeats its method and body. Do not paginate requests
with side effects without considering what the repeated request will do.

## Options and limits

| Argument | Default | Meaning |
| --- | --- | --- |
| `method` | `GET` | HTTP method |
| `timeout_s` | 30 | Overall deadline, up to 120 seconds |
| `max_bytes` | 65536 | Returned bytes after conversion, 1 to 1048576 |
| `offset` | 0 | Byte offset in the complete representation |
| `format` | `markdown` | HTML output: `markdown`, `text`, or `raw` |
| `main_content_only` | `true` | Prefer main/article content; ignored for raw HTML |
| `follow_redirects` | `true` | Follow up to 10 redirects |

A separate 16 MiB decompressed download cap rejects oversized resources. The
deadline covers redirects, download, and conversion. Arbitrary offsets inside a
UTF-8 character fail, as does a limit too small to fit the next character. An
offset beyond the end returns empty content and `eof=true`.

Results report `status`, `content_type`, `content_format`, downloaded `bytes_read`,
returned `output_bytes`, `total_bytes`, `offset`, `next_offset`, `truncated`, `eof`,
and the complete representation's `content_sha256`. The final `url`, original
`requested_url`, and `redirects` describe the request path. HTTP 400 and higher
set `is_error`.

## Redirects and security

Approval covers the redirect chain. HTTPS downgrades and cross-origin redirects
that would forward a request body are rejected. Cross-origin requests drop caller
headers except `Accept`, `Accept-Language`, and `User-Agent`; response cookies are
not retained. Standard redirect method rules apply.

Requests can reach private addresses on the runner host's network. This is not a
network sandbox and is not routed through the execution target. Arguments enter
history, so do not put secrets in URLs, headers, or bodies. Treat fetched content
as untrusted data, not instructions that grant additional permissions.

Completed calls replay their recorded results without network access. Older
recorded calls can contain earlier response formats; see
[compatibility](../reference/compatibility.md).

## Related

- [Tool collections](../config/tool-collections.md)
- [Tool approval](../config/tool-approval.md)
