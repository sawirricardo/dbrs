#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${DBRS_REPO_SLUG:-sawirricardo/dbrs}"
PACKAGE_NAME="${DBRS_PACKAGE_NAME:-dbrs}"
RELEASE_TAG="${DBRS_RELEASE_TAG:-stable}"
BIN_DIR="${DBRS_INSTALL_DIR:-}"
TMP_DIR=""

cleanup() {
  if [[ -n "${TMP_DIR:-}" ]]; then
    rm -rf -- "$TMP_DIR"
  fi
}

trap cleanup EXIT

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

download() {
  local url="$1"
  local output="$2"

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$output"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -qO "$output" "$url"
    return
  fi

  echo "error: curl or wget is required to download $PACKAGE_NAME" >&2
  exit 1
}

detect_target() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)
      case "$arch" in
        x86_64|amd64) echo "x86_64-unknown-linux-gnu tar.gz $PACKAGE_NAME" ;;
        *)
          echo "error: unsupported Linux architecture: $arch" >&2
          exit 1
          ;;
      esac
      ;;
    Darwin)
      case "$arch" in
        x86_64|amd64) echo "x86_64-apple-darwin tar.gz $PACKAGE_NAME" ;;
        arm64|aarch64) echo "aarch64-apple-darwin tar.gz $PACKAGE_NAME" ;;
        *)
          echo "error: unsupported macOS architecture: $arch" >&2
          exit 1
          ;;
      esac
      ;;
    MINGW*|MSYS*|CYGWIN*)
      case "$arch" in
        x86_64|amd64) echo "x86_64-pc-windows-msvc zip ${PACKAGE_NAME}.exe" ;;
        *)
          echo "error: unsupported Windows architecture: $arch" >&2
          exit 1
          ;;
      esac
      ;;
    *)
      echo "error: unsupported operating system: $os" >&2
      exit 1
      ;;
  esac
}

install_binary() {
  local binary_path="$1"
  local install_name="$2"

  mkdir -p "$BIN_DIR"
  install -m 755 "$binary_path" "$BIN_DIR/$install_name"
}

resolve_bin_dir() {
  if [[ -n "$BIN_DIR" ]]; then
    return
  fi

  local candidates=()
  case "$(uname -s)" in
    Darwin)
      candidates=(
        "/usr/local/bin"
        "/opt/homebrew/bin"
        "$HOME/.local/bin"
        "${CARGO_HOME:-$HOME/.cargo}/bin"
      )
      ;;
    Linux)
      candidates=(
        "/usr/local/bin"
        "$HOME/.local/bin"
        "${CARGO_HOME:-$HOME/.cargo}/bin"
      )
      ;;
    MINGW*|MSYS*|CYGWIN*)
      candidates=(
        "$HOME/.local/bin"
        "${CARGO_HOME:-$HOME/.cargo}/bin"
      )
      ;;
    *)
      candidates=("${CARGO_HOME:-$HOME/.cargo}/bin")
      ;;
  esac

  local candidate parent
  for candidate in "${candidates[@]}"; do
    if [[ -d "$candidate" && -w "$candidate" ]]; then
      BIN_DIR="$candidate"
      return
    fi

    parent="$(dirname "$candidate")"
    if [[ -d "$parent" && -w "$parent" ]]; then
      BIN_DIR="$candidate"
      return
    fi
  done

  BIN_DIR="${CARGO_HOME:-$HOME/.cargo}/bin"
}

main() {
  need_cmd uname
  need_cmd mktemp
  need_cmd tar
  resolve_bin_dir

  read -r target archive_ext binary_name < <(detect_target)

  local asset_name="${PACKAGE_NAME}-${target}.${archive_ext}"
  local download_url
  if [[ "$RELEASE_TAG" == "stable" ]]; then
    download_url="https://github.com/${REPO_SLUG}/releases/latest/download/${asset_name}"
  else
    download_url="https://github.com/${REPO_SLUG}/releases/download/${RELEASE_TAG}/${asset_name}"
  fi

  local archive_path binary_path install_name
  TMP_DIR="$(mktemp -d)"
  archive_path="$TMP_DIR/$asset_name"
  install_name="$PACKAGE_NAME"
  binary_path="$TMP_DIR/$binary_name"

  echo "Downloading ${asset_name} from ${download_url}"
  download "$download_url" "$archive_path"

  case "$archive_ext" in
    tar.gz)
      tar -xzf "$archive_path" -C "$TMP_DIR"
      ;;
    zip)
      need_cmd unzip
      unzip -q "$archive_path" -d "$TMP_DIR"
      install_name="${PACKAGE_NAME}.exe"
      binary_path="$TMP_DIR/${PACKAGE_NAME}.exe"
      ;;
    *)
      echo "error: unsupported archive format: $archive_ext" >&2
      exit 1
      ;;
  esac

  if [[ ! -f "$binary_path" ]]; then
    echo "error: downloaded archive did not contain ${binary_name}" >&2
    exit 1
  fi

  install_binary "$binary_path" "$install_name"

  echo "Installed ${install_name} to ${BIN_DIR}"
  if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
    echo "warning: $BIN_DIR is not in PATH" >&2
    echo "add this to your shell profile:" >&2
    echo "  export PATH=\"$BIN_DIR:\$PATH\"" >&2
  fi
}

main "$@"
