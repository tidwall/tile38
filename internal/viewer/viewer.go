package viewer

import (
	"bytes"
	"embed"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

//go:embed *
var files embed.FS

func HandleHTTP(wr io.Writer, url string, devMode bool) error {
	if (!strings.HasPrefix(url, "/viewer/") && url != "/viewer") ||
		strings.Contains(url, "..") {
		return writeHTTPResponse(wr, "404 Not Found", "text/html",
			nil, []byte("<h1>404 Not Found</h1>\n"))
	}
	if strings.HasSuffix(url, "/") {
		return writeHTTPResponse(wr, "307 Redirect", "text/html",
			[]string{"Location", url[:len(url)-1]},
			[]byte("<h1>307 Redirect</h1>\n"))
	}

	if url == "/viewer" {
		return writeHTTPFile(wr, "/viewer/index.html", devMode)
	}
	return writeHTTPFile(wr, url, devMode)
}

func writeHTTPFile(wr io.Writer, path string, devMode bool) error {
	var data []byte
	err := os.ErrNotExist
	if devMode {
		data, err = os.ReadFile("internal" + path)
	}
	path = path[8:]
	if os.IsNotExist(err) {
		data, err = files.ReadFile(path)
	}
	if os.IsNotExist(err) {
		return writeHTTPResponse(wr, "404 Not Found", "text/html",
			nil, []byte("<h1>404 Not Found</h1>\n"))
	}
	return writeHTTPResponse(wr, "200 OK",
		mime.TypeByExtension(filepath.Ext(path)), nil, data)
}

func writeHTTPResponse(wr io.Writer, status, contentType string,
	headers []string, body []byte,
) error {
	var sheaders string
	if len(headers) > 0 {
		hdrs := http.Header{}
		for i := 0; i < len(headers)-1; i += 2 {
			hdrs.Set(headers[i], headers[i+1])
		}
		var buf bytes.Buffer
		hdrs.Write(&buf)
		sheaders = buf.String()
	}
	payload := append([]byte(nil), fmt.Sprintf(""+
		"HTTP/1.1 %s\r\n"+
		"Connection: close\r\n"+
		"Content-Type: %s\r\n"+
		"Content-Length: %d\r\n"+
		"Access-Control-Allow-Origin: *\r\n"+
		sheaders+
		"\r\n", status, contentType, len(body))...)
	payload = append(payload, body...)
	_, err := wr.Write(payload)
	return err
}
