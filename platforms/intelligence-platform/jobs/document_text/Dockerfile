# The document text job of root ADR-0477: pdfplumber for the PDF text layer, ocrmypdf with
# Tesseract (kor) for the pages that are scans, LibreOffice with the H2Orestart filter to print
# HWP 5 / HWPX to PDF. The tools run as child processes of `python -m document_text.run`.
# Build from platforms/intelligence-platform: docker build -f jobs/document_text/Dockerfile .
FROM python:3.14.10-slim-trixie@sha256:3cabfd7d7aaf394e6040e79fa1eec6ca1c52b74ed9188650472695d9a9326999

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# H2Orestart v0.8.0 (2026-10-18), the HWP/HWPX import filter LibreOffice loads; checked by hash.
ARG H2ORESTART_URL=https://github.com/ebandal/H2Orestart/releases/download/v0.8.0/H2Orestart.oxt
ARG H2ORESTART_SHA256=f36c19fa6b7dff1cf34114d926bdc088e6bcfa7fdc93d67eaafd3f572be1d62c

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl \
      libreoffice-writer-nogui libreoffice-java-common default-jre-headless \
      ocrmypdf tesseract-ocr tesseract-ocr-kor tesseract-ocr-eng \
      fonts-nanum fonts-noto-cjk \
 && curl -fsSL -o /tmp/H2Orestart.oxt "$H2ORESTART_URL" \
 && echo "$H2ORESTART_SHA256  /tmp/H2Orestart.oxt" | sha256sum -c - \
 && unopkg add --shared --suppress-license /tmp/H2Orestart.oxt \
 && rm /tmp/H2Orestart.oxt \
 && apt-get purge -y curl && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# Wheels only, every one checked against the lock's hashes.
COPY jobs/document_text/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt \
 && useradd --uid 10001 --create-home --shell /usr/sbin/nologin intelligence

COPY config/document-extraction.contract.json /opt/intelligence/config/document-extraction.contract.json
COPY config/ksic-divisions.json /opt/intelligence/config/ksic-divisions.json
COPY jobs/document_text/document_text /opt/intelligence/jobs/document_text/document_text

USER intelligence
WORKDIR /opt/intelligence/jobs/document_text
ENTRYPOINT ["python", "-m", "document_text.run"]
