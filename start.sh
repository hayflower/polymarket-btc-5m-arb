#!/usr/bin/env bash
# =============================================================================
# Polymarket Arb Bot -- VPS Startup Script
# =============================================================================
# Usage:
#   chmod +x start.sh
#   ./start.sh              # first run: installs deps, starts bot
#   ./start.sh update       # pull latest code, reinstall deps, restart
#   ./start.sh stop         # stop the bot
#   ./start.sh status       # check if running + tail recent logs
#   ./start.sh logs         # follow live log output
#   ./start.sh latency      # test connection to Polymarket servers
#   ./start.sh scan         # one-shot scan for current opportunities

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="$SCRIPT_DIR/venv"
PID_FILE="$SCRIPT_DIR/bot.pid"
LOG_FILE="$SCRIPT_DIR/bot.log"
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

ensure_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        log "Creating virtual environment..."
        python3 -m venv "$VENV_DIR" 2>/dev/null || {
            # Minimal distros (Debian/Ubuntu) may lack ensurepip
            log "venv creation failed -- installing python3-venv..."
            sudo apt-get update -qq && sudo apt-get install -y -qq python3-venv python3-pip
            python3 -m venv "$VENV_DIR"
        }
    fi
    # Ensure pip exists inside the venv (some distros create venv without it)
    if [ ! -f "$PIP" ]; then
        log "pip missing from venv, bootstrapping..."
        "$PYTHON" -m ensurepip --upgrade 2>/dev/null || {
            log "ensurepip unavailable, using get-pip.py..."
            curl -sS https://bootstrap.pypa.io/get-pip.py | "$PYTHON"
        }
    fi
}

install_deps() {
    ensure_venv
    log "Installing dependencies..."
    "$PIP" install --upgrade pip -q
    "$PIP" install -r requirements.txt -q
    log "Dependencies installed."
}

ensure_env() {
    if [ ! -f "$SCRIPT_DIR/.env" ]; then
        log "ERROR: .env file not found."
        log "Copy the example and fill in your private key:"
        log "  cp .env.example .env"
        log "  nano .env"
        exit 1
    fi
}

is_running() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        else
            rm -f "$PID_FILE"
        fi
    fi
    return 1
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_start() {
    if is_running; then
        log "Bot is already running (PID $(cat "$PID_FILE"))."
        log "Use './start.sh stop' first, or './start.sh update' to pull + restart."
        exit 1
    fi

    ensure_env
    install_deps

    log "Starting bot in background..."
    nohup "$PYTHON" bot.py run >> "$LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"
    log "Bot started (PID $pid). Logs: $LOG_FILE"
    log "Use './start.sh logs' to follow output."
    log "Use './start.sh stop' to stop."

    # Show first few seconds of output
    sleep 2
    tail -20 "$LOG_FILE"
}

cmd_stop() {
    if ! is_running; then
        log "Bot is not running."
        return 0
    fi
    local pid
    pid=$(cat "$PID_FILE")
    log "Stopping bot (PID $pid)..."
    kill "$pid" 2>/dev/null || true
    # Wait up to 10s for graceful shutdown
    for i in $(seq 1 10); do
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    # Force kill if still alive
    if kill -0 "$pid" 2>/dev/null; then
        log "Force killing..."
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    log "Bot stopped."
}

cmd_update() {
    log "Pulling latest code..."
    git pull --ff-only || {
        log "WARNING: git pull failed. Check for local changes."
        log "You can try: git stash && git pull && git stash pop"
        exit 1
    }

    # Reinstall deps in case requirements.txt changed
    install_deps

    # Restart if was running
    if is_running; then
        log "Restarting bot..."
        cmd_stop
        cmd_start
    else
        log "Updated. Run './start.sh' to start the bot."
    fi
}

cmd_status() {
    if is_running; then
        local pid
        pid=$(cat "$PID_FILE")
        log "Bot is RUNNING (PID $pid)"
        log ""
        log "--- Recent log output ---"
        tail -30 "$LOG_FILE" 2>/dev/null || log "(no log file yet)"
    else
        log "Bot is NOT running."
        if [ -f "$LOG_FILE" ]; then
            log ""
            log "--- Last log output ---"
            tail -15 "$LOG_FILE"
        fi
    fi
}

cmd_logs() {
    if [ ! -f "$LOG_FILE" ]; then
        log "No log file yet. Start the bot first."
        exit 1
    fi
    tail -f "$LOG_FILE"
}

cmd_latency() {
    ensure_venv
    install_deps
    "$PYTHON" bot.py latency
}

cmd_scan() {
    ensure_env
    ensure_venv
    install_deps
    "$PYTHON" bot.py scan
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "${1:-start}" in
    start)    cmd_start ;;
    stop)     cmd_stop ;;
    update)   cmd_update ;;
    status)   cmd_status ;;
    logs)     cmd_logs ;;
    latency)  cmd_latency ;;
    scan)     cmd_scan ;;
    *)
        echo "Usage: ./start.sh [start|stop|update|status|logs|latency|scan]"
        echo ""
        echo "  start    Start the bot in background (default)"
        echo "  stop     Stop the bot"
        echo "  update   Git pull + reinstall deps + restart if running"
        echo "  status   Check if running + show recent logs"
        echo "  logs     Follow live log output (Ctrl+C to stop)"
        echo "  latency  Test connection speed to Polymarket"
        echo "  scan     One-shot scan for current arb opportunities"
        ;;
esac
