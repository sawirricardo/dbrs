#!/usr/bin/env bash
set -euo pipefail

PACKAGE_NAME="${DBRS_PACKAGE_NAME:-dbrs}"
BIN_DIR="${DBRS_INSTALL_DIR:-}"

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

  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -x "$candidate/$PACKAGE_NAME" || -x "$candidate/${PACKAGE_NAME}.exe" ]]; then
      BIN_DIR="$candidate"
      return
    fi
  done

  BIN_DIR="${CARGO_HOME:-$HOME/.cargo}/bin"
}

main() {
  resolve_bin_dir

  local removed=0
  if [[ -f "$BIN_DIR/$PACKAGE_NAME" ]]; then
    rm -f "$BIN_DIR/$PACKAGE_NAME"
    echo "Removed $BIN_DIR/$PACKAGE_NAME"
    removed=1
  fi

  if [[ -f "$BIN_DIR/${PACKAGE_NAME}.exe" ]]; then
    rm -f "$BIN_DIR/${PACKAGE_NAME}.exe"
    echo "Removed $BIN_DIR/${PACKAGE_NAME}.exe"
    removed=1
  fi

  if [[ "$removed" -eq 0 ]]; then
    echo "No installed $PACKAGE_NAME binary found in $BIN_DIR" >&2
    exit 1
  fi
}

main "$@"
