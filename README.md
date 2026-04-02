# DART Disclosure PDF Builder

`build_disclosure_pdf.py`는 금감원 DART에서 특정 종목의 공시를 조회해 시간 역순으로 하나의 PDF로 합칩니다.

기본 프롬프트 값은 실행 시점 기준으로 다음과 같습니다.

- 대상기업: `삼성전자`
- 대상기간: 실행일이 `2026-03-18`라면 `2026-01-01` ~ `2026-03-18`
- 저장폴더: 실행 환경의 `~/Documents`

기본값은 저장소 루트의 `config.toml`에서 관리합니다. 실행 시 `default` 섹션을 먼저 읽고, 현재 OS에 맞는 섹션(`windows`, `macos`, `linux`)이 있으면 그 값으로 덮어씁니다.

```toml
[default]
company_name = "삼성전자"
# cache_dir = "~/Library/Caches/dart-disclosure-pdf-builder"

[windows]
output_dir = "~/Documents"

[macos]
output_dir = "~/Documents"
```

`cache_dir`는 선택값입니다. 지정하지 않으면 OS 기본 캐시 폴더를 사용하며, 같은 공시(`rcp_no + dcm_no`)를 다시 조회할 때는 이미 만든 PDF 소스를 재사용합니다.
추가로 공시 detail 메타데이터(`main_dcm_no`, 첨부목록)도 캐시하므로, 중간에 차단된 뒤 같은 조건으로 다시 실행하면 앞에서 이미 처리한 공시의 detail 페이지를 다시 열지 않습니다.

## 실행

### macOS / Linux

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python build_disclosure_pdf.py
```

### Windows `cmd`

```cmd
py -3 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe build_disclosure_pdf.py
```

## 웹 실행

### 로컬 실행

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/uvicorn web_app:app --host 0.0.0.0 --port 8080
```

브라우저에서 `http://127.0.0.1:8080`으로 접속하면 폼에서 회사명과 기간을 입력한 뒤 PDF를 바로 다운로드할 수 있습니다.

웹 UI는 작업을 시작한 뒤 같은 페이지에서 진행 로그를 보여줍니다. 예를 들어 아래와 같은 형식으로 로그가 누적됩니다.

```text
공시 159건 조회 완료: 인스코비(006490) 2024-01-01 ~ 2026-03-29
[1/159] 2026.03.24 임원ㆍ주요주주특정증권등소유상황보고서
[2/159] 2026.03.24 주식등의대량보유상황보고서(일반)
...
```

완료되면 페이지 안에서 다운로드 버튼이 활성화됩니다. JavaScript를 끄면 기존처럼 단일 요청으로 PDF를 바로 내려받는 방식으로 동작합니다.

서버 프록시가 설정되어 있으면 로그에 `서버 프록시 사용 중 (...)` 문구가 함께 표시됩니다.
반복 조회 성능을 높이기 위해, 본문/첨부 PDF 소스는 디스크 캐시에 저장됩니다. 같은 공시를 다시 생성하면 DART 다운로드와 ZIP HTML 변환을 대부분 건너뜁니다.
웹 UI의 백그라운드 생성 작업은 별도 Python subprocess에서 실행되므로, 긴 PDF 생성 중에도 첫 화면과 상태 조회가 덜 막히도록 구성했습니다.
안정성을 위해 한 머신에서는 동시에 하나의 백그라운드 PDF 작업만 받습니다.
Fly 같은 서버 환경에서는 ZIP 기반 HTML 공시를 `fitz`로 변환하도록 두고, 로컬 CLI/로컬 웹 실행에서는 기존처럼 브라우저 렌더러를 우선 사용합니다.

### Fly 배포

이 저장소에는 Fly 배포용 [Dockerfile](/Users/neotizen/peta-fss-scrap/Dockerfile)과 [fly.toml](/Users/neotizen/peta-fss-scrap/fly.toml)이 포함되어 있습니다.

1. [fly.toml](/Users/neotizen/peta-fss-scrap/fly.toml)의 `app` 값을 원하는 앱 이름으로 수정합니다.
2. `fly auth login`
3. `fly launch --copy-config --no-deploy`
4. `fly deploy`

현재 구성은 요청이 들어올 때만 머신을 자동으로 켜고, 유휴 시 자동 정지하도록 설정되어 있습니다. 공시 수집과 PDF 병합이 한 요청 안에서 수행되므로 동시 처리 수는 낮게 제한했습니다.

### 서버 프록시 연결

웹 서비스에서 DART 접속이 차단되면, 사용자 브라우저 VPN이 아니라 `서버 outbound 경로`를 바꿔야 합니다. 이 앱은 아래 환경변수를 지원합니다.

- `DART_PROXY_URL`: HTTP/HTTPS에 공통으로 쓸 프록시 URL
- `DART_HTTP_PROXY`: HTTP 전용 프록시 URL
- `DART_HTTPS_PROXY`: HTTPS 전용 프록시 URL
- `DART_NO_PROXY`: 프록시 제외 호스트 목록

`DART_*` 값을 주면 내부적으로 표준 `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` 별칭으로 연결됩니다. 이미 표준 환경변수가 있으면 그 값을 우선 사용합니다.

Fly에서는 보통 secret으로 넣는 것이 가장 안전합니다.

```bash
fly secrets set DART_PROXY_URL=http://USER:PASSWORD@PROXY_HOST:PORT
fly secrets set DART_NO_PROXY=localhost,127.0.0.1
fly deploy
```

프로토콜별로 나누고 싶다면 아래처럼 설정할 수 있습니다.

```bash
fly secrets set DART_HTTP_PROXY=http://USER:PASSWORD@PROXY_HOST:PORT
fly secrets set DART_HTTPS_PROXY=http://USER:PASSWORD@PROXY_HOST:PORT
fly deploy
```

프록시는 `HTTP(S) 프록시` 기준입니다. 일반적인 개인용 VPN 앱을 브라우저에만 켜는 방식으로는 Fly 서버의 DART 접속 경로가 바뀌지 않습니다.

### 캐시 디렉터리 지정

캐시 위치를 바꾸고 싶다면 `config.toml`의 `cache_dir`를 쓰거나, 환경변수 `DART_CACHE_DIR`를 지정할 수 있습니다.

```bash
export DART_CACHE_DIR="$HOME/.cache/dart-disclosure-pdf-builder"
```

Fly에서는 기본 캐시가 머신 로컬에 저장됩니다. 반복 조회는 빨라지지만, 배포나 머신 교체 이후까지 영구 보존하려면 볼륨 경로를 `DART_CACHE_DIR`로 지정하는 구성이 추가로 필요합니다.

### DART 요청 속도 완화

긴 구간 조회에서 DART가 연결을 끊는 경우를 줄이기 위해, 기본적으로 요청 간 짧은 간격과 주기적 쿨다운을 둡니다.

- `DART_REQUEST_INTERVAL_SECONDS`: 각 DART 요청 시작 사이의 최소 간격. 기본값 `0.15`
- `DART_REQUEST_COOLDOWN_EVERY`: 누적 요청 수가 이 값의 배수에 도달할 때 추가 쿨다운. 기본값 `60`
- `DART_REQUEST_COOLDOWN_SECONDS`: 주기적 쿨다운 시간. 기본값 `6`
- `DART_REMOTE_DISCONNECT_RETRY_DELAY_SECONDS`: `RemoteDisconnected` 등 연결 끊김 재시도 시 적용할 최소 backoff. 기본값 `12`

예시:

```bash
export DART_REQUEST_INTERVAL_SECONDS=0.2
export DART_REQUEST_COOLDOWN_EVERY=40
export DART_REQUEST_COOLDOWN_SECONDS=8
```

같은 조건으로 다시 실행할 때는 캐시가 앞부분 detail/PDF 소스를 재사용하므로, 긴 조회에서 중간 차단이 나더라도 재시도 부담이 크게 줄어듭니다.

### HTML -> PDF 렌더러 모드

ZIP 기반 HTML 공시를 PDF로 바꿀 때는 아래 환경변수를 쓸 수 있습니다.

- `DART_HTML_PDF_RENDERER=browser`: 브라우저 렌더러 우선, 실패 시 `fitz` fallback
- `DART_HTML_PDF_RENDERER=fitz`: 브라우저를 띄우지 않고 `fitz`만 사용
- `DART_HTML_PDF_RENDERER=auto`: 현재는 `browser`와 동일하게 동작

기본 동작은 로컬 실행에서 `browser`입니다. Fly 서버 subprocess는 자동으로 `fitz`를 강제해 서버 브라우저 오버헤드를 줄입니다.

실행하면 CLI에서 아래 값을 순서대로 묻습니다.

- 대상 회사명
- 조회 시작일
- 조회 종료일
- 저장 폴더

저장 파일명은 실제로 수집된 공시의 접수일자 범위를 기준으로 `금감원공시-{회사명}-{최초접수일YYMMDD}-{최종접수일YYMMDD}.pdf` 형식으로 자동 생성됩니다.

## 옵션

```bash
.venv/bin/python build_disclosure_pdf.py --dry-run
.venv/bin/python build_disclosure_pdf.py --limit 3
.venv/bin/python build_disclosure_pdf.py --company-name 삼성전자 --start-date 2026-01-01 --end-date 2026-03-18 --output-dir /path/to/folder
.venv/bin/python build_disclosure_pdf.py --company-name 삼성전자 --start-date 2026-01-01 --end-date 2026-03-18 --output /path/to/output.pdf --no-prompt
```

```cmd
.venv\Scripts\python.exe build_disclosure_pdf.py --dry-run
.venv\Scripts\python.exe build_disclosure_pdf.py --limit 3
.venv\Scripts\python.exe build_disclosure_pdf.py --company-name 삼성전자 --start-date 2026-01-01 --end-date 2026-03-18 --output-dir C:\path\to\folder
.venv\Scripts\python.exe build_disclosure_pdf.py --company-name 삼성전자 --start-date 2026-01-01 --end-date 2026-03-18 --output C:\path\to\output.pdf --no-prompt
```

## 동작 방식

1. `detailSearch.ax`로 기간 내 공시 목록을 조회합니다.
2. 기업개황 팝업에서 종목코드를 읽습니다.
3. 각 공시의 본문 PDF와 첨부문서 PDF를 다운로드합니다.
4. 모든 페이지 왼쪽 상단에 `회사명(종목코드)-공시일자(yy.mm.dd)-공시명` 헤더를 넣어 하나의 PDF로 저장합니다.
5. 웹 모드에서는 같은 로직을 서버에서 실행한 뒤 생성된 PDF를 바로 응답으로 내려줍니다.
