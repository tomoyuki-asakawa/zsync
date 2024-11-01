#!/bin/sh

# グローバル変数
VERSION="3.1"
LOG_FILE=""
CLEANUP_MODE=0
FULL_INCREMENTAL=0
SCRIPT_NAME="$0"
ORIGINAL_ARGS=""
VERBOSE_MODE=0
TRANSFER_TYPE=""
INTERRUPT_OCCURRED=0

# シグナルハンドラ
handle_interrupt() {
    INTERRUPT_OCCURRED=1
    # 子プロセスにもシグナルを送信
    kill -INT 0
}

# verbose_echo 関数の定義
verbose_echo() {
    if [ "$VERBOSE_MODE" -eq 1 ]; then
        echo "$@" >&2
    fi
}

# 状態ファイルのパス
STATE_FILE="/tmp/zsync_state"

# 状態を保存する関数
save_state() {
    echo "$1" > "$STATE_FILE"
}

# 状態を読み込む関数
load_state() {
    if [ -f "$STATE_FILE" ]; then
        cat "$STATE_FILE"
    else
        echo ""
    fi
}

# ログ記録関数
log_message() {
    if [ -n "$LOG_FILE" ]; then
        echo "$(date): $1" >> "$LOG_FILE"
    fi
    verbose_echo "$1"
}

# エラー報告関数
report_error() {
    local ERROR_MESSAGE="$1"
    local SUGGESTION="$2"
    log_message "エラー: $ERROR_MESSAGE"
    echo "エラー: $ERROR_MESSAGE" >&2
    if [ -n "$SUGGESTION" ]; then
        echo "$SUGGESTION" >&2
        echo "以下のコマンドを使用してください：" >&2
        echo "$SCRIPT_NAME $SUGGESTION $ORIGINAL_ARGS" >&2
    fi
    exit 1
}

# 警告報告関数
report_warning() {
    log_message "警告: $1"
    if [ "$VERBOSE_MODE" -eq 1 ]; then
        echo "警告: $1" >&2
    fi
}

# 中断メッセージを表示する関数
display_interrupt_message() {
    if [ $INTERRUPT_OCCURRED -eq 1 ]; then
        echo ""
        case "$TRANSFER_TYPE" in
            "FULL")
                log_message "フル送信が中断されました。レジュームトークンが保存されています。"
                log_message "次回の実行時に中断したところから再開できます。"
                ;;
            "INCREMENTAL")
                log_message "インクリメンタル送信が中断されました。"
                log_message "次回の実行時に最後に成功したスナップショットから再開されます。"
                ;;
            "RESUME")
                log_message "レジューム転送が中断されました。レジュームトークンが更新されています。"
                log_message "次回の実行時に中断したところから再開できます。"
                ;;
            *)
                log_message "転送が中断されました。"
                ;;
        esac
    fi
}

# バージョン情報を表示する関数
show_version() {
    echo "ZFS Sync Script version $VERSION"
    exit 0
}

# 使用方法を表示する関数
usage() {
    echo "Usage: $0 [-f] [-s snapshot_name] [-V] [-v] [-C] [-I] [-l log_file] <source_dataset> <target_dataset>"
    echo "   or: $0 [-f] [-s snapshot_name] [-V] [-v] [-C] [-I] [-l log_file] <user@host> <localdataset> <remotedataset>"
    echo "Options:"
    echo "  -f                 Force full send when no common snapshots are found"
    echo "  -s snapshot_name   Use a fixed snapshot name prefix (timestamp will be appended)"
    echo "  -V                 Verbose mode: display detailed debug information"
    echo "  -v                 Show version information and exit"
    echo "  -C                 Cleanup mode: remove partially received datasets before transfer"
    echo "  -I                 Full incremental: include all intermediate snapshots in transfer"
    echo "  -l log_file        Specify a log file to record the operations"
    echo "Dataset format: [user@host:]dataset"
    exit 1
}

# user@host 文字列からホスト名を抽出する関数
extract_hostname() {
    echo "$1" | cut -d'@' -f2
}

# データセット文字列を解析する関数
parse_dataset() {
    INPUT="$1"
    USER_HOST=""
    DATASET=""

    if echo "$INPUT" | grep -q ":"; then
        USER_HOST=$(echo "$INPUT" | cut -d':' -f1)
        DATASET=$(echo "$INPUT" | cut -d':' -f2-)
    else
        DATASET="$INPUT"
    fi

    echo "$USER_HOST" "$DATASET"
}

# 名前をサニタイズする関数
sanitize_name() {
    echo "$1" | sed 's/[^A-Za-z0-9._-]/_/g'
}

# IPアドレスを取得する関数
get_ip_address() {
    HOST="$1"
    if [ -z "$HOST" ]; then
        IP=$(ifconfig | awk '/inet /{print $2}' | grep -v '127.0.0.1' | head -n 1)
        echo "${IP:-localhost}"
    else
        IP=$(ssh "$HOST" "ifconfig | awk '/inet /{print \$2}' | grep -v '127.0.0.1' | head -n 1" 2>/dev/null)
        if [ -z "$IP" ]; then
            IP="$HOST"
        fi
        echo "$IP"
    fi
}

# コマンドをローカルまたはリモートで実行する関数
run_cmd() {
    USER_HOST="$1"
    shift
    CMD="$*"
    if [ -z "$USER_HOST" ]; then
        verbose_echo "実行コマンド（ローカル）: $CMD"
        $CMD
    else
        verbose_echo "実行コマンド（リモート）: ssh $USER_HOST 'sudo $CMD'"
        ssh "$USER_HOST" "sudo $CMD"
    fi
}

# スナップショットのリストを取得する関数
list_snapshots() {
    USER_HOST="$1"
    DATASET="$2"
    run_cmd "$USER_HOST" "zfs list -H -o name -t snapshot -s creation -r $DATASET"
}

# 共通のスナップショットを取得する関数
get_common_snapshot() {
    SOURCE_SNAPSHOTS=$(mktemp -t zsync.XXXXXX)
    DEST_SNAPSHOTS=$(mktemp -t zsync.XXXXXX)

    verbose_echo "ソースのスナップショットリストを取得します。"
    list_snapshots "$SOURCE_USER_HOST" "$SOURCE_DATASET" | awk -F'@' '{print $2}' | grep "^${SNAPSHOT_PREFIX}" > "$SOURCE_SNAPSHOTS"

    verbose_echo "デスティネーションのスナップショットリストを取得します。"
    list_snapshots "$DEST_USER_HOST" "$TARGET_DATASET" | awk -F'@' '{print $2}' | grep "^${SNAPSHOT_PREFIX}" > "$DEST_SNAPSHOTS"

    SORTED_SOURCE_SNAPSHOTS=$(mktemp -t zsync.XXXXXX)
    SORTED_DEST_SNAPSHOTS=$(mktemp -t zsync.XXXXXX)
    sort "$SOURCE_SNAPSHOTS" > "$SORTED_SOURCE_SNAPSHOTS"
    sort "$DEST_SNAPSHOTS" > "$SORTED_DEST_SNAPSHOTS"

    COMMON_SNAPSHOT_NAME=$(comm -12 "$SORTED_SOURCE_SNAPSHOTS" "$SORTED_DEST_SNAPSHOTS" | tail -n 1)

    rm -f "$SOURCE_SNAPSHOTS" "$DEST_SNAPSHOTS" "$SORTED_SOURCE_SNAPSHOTS" "$SORTED_DEST_SNAPSHOTS"

    if [ -n "$COMMON_SNAPSHOT_NAME" ]; then
        SOURCE_SNAPSHOT_EXISTS=$(run_cmd "$SOURCE_USER_HOST" "zfs list -t snapshot -o name -H ${SOURCE_DATASET}@${COMMON_SNAPSHOT_NAME}" 2>/dev/null)
        DEST_SNAPSHOT_EXISTS=$(run_cmd "$DEST_USER_HOST" "zfs list -t snapshot -o name -H ${TARGET_DATASET}@${COMMON_SNAPSHOT_NAME}" 2>/dev/null)

        if [ -z "$SOURCE_SNAPSHOT_EXISTS" ] || [ -z "$DEST_SNAPSHOT_EXISTS" ]; then
            verbose_echo "共通スナップショットが見つかりましたが、存在しません。"
            COMMON_SNAPSHOT_NAME=""
        fi
    fi

    echo "$COMMON_SNAPSHOT_NAME"
}

# レジュームトークンを取得する関数
get_resume_token() {
    USER_HOST="$1"
    DATASET="$2"
    TOKEN=$(run_cmd "$USER_HOST" "zfs get -H -o value receive_resume_token $DATASET" 2>/dev/null)
    echo "$TOKEN"
}

# レジュームトークンの有効性を確認する関数
validate_resume_token() {
    local RESUME_TOKEN="$1"
    local SOURCE_DATASET="$2"
    local SOURCE_USER_HOST="$3"

    local TONAME=$(echo "$RESUME_TOKEN" | awk -F'toname = ' '{print $2}' | awk '{print $1}')
    local SNAPSHOT_EXISTS
    if [ -z "$SOURCE_USER_HOST" ]; then
        SNAPSHOT_EXISTS=$(zfs list -t snapshot -o name -H "$TONAME" 2>/dev/null)
    else
        SNAPSHOT_EXISTS=$(ssh "$SOURCE_USER_HOST" "zfs list -t snapshot -o name -H $TONAME" 2>/dev/null)
    fi

    if [ -z "$SNAPSHOT_EXISTS" ]; then
        verbose_echo "レジュームトークンが参照するスナップショット $TONAME が存在しません。"
        return 1
    fi

    return 0
}

# スナップショットを準備する関数
prepare_snapshots() {
    local TIMESTAMP=$(date +%Y%m%d%H%M%S)

    if [ -n "$FIXED_SNAPSHOT_NAME" ]; then
        SNAPSHOT_NAME=$(echo "${FIXED_SNAPSHOT_NAME}" | sed 's/-*$//g')"-${TIMESTAMP}"
    else
        local SOURCE_IDENTIFIER
        local DEST_IDENTIFIER

        if [ "$USE_OLD_FORMAT" = "true" ]; then
            SOURCE_IDENTIFIER=$(extract_hostname "$DEST_USER_HOST")
            DEST_IDENTIFIER=$SOURCE_IDENTIFIER
        else
            SOURCE_IP=$(get_ip_address "${SOURCE_USER_HOST}")
            DEST_IP=$(get_ip_address "${DEST_USER_HOST}")
            SOURCE_IDENTIFIER=$(sanitize_name "${SOURCE_IP:-localhost}")
            DEST_IDENTIFIER=$(sanitize_name "${DEST_IP:-localhost}")
        fi

        SANITIZED_TARGET_DATASET=$(sanitize_name "${TARGET_DATASET}")

        SNAPSHOT_NAME="${SNAPSHOT_PREFIX}-${SOURCE_IDENTIFIER}-${DEST_IDENTIFIER}-${SANITIZED_TARGET_DATASET}-${TIMESTAMP}"
    fi

    SNAPSHOT_NAME=$(echo "$SNAPSHOT_NAME" | sed 's/-\+/-/g')
    FULL_SNAPSHOT_NAME="${SOURCE_DATASET}@${SNAPSHOT_NAME}"

    verbose_echo "ソースデータセットの存在を確認します。"
    DATASET_EXISTS=$(run_cmd "$SOURCE_USER_HOST" "zfs list -H -o name $SOURCE_DATASET" 2>/dev/null)

    if [ -z "$DATASET_EXISTS" ]; then
        report_error "ソースデータセット $SOURCE_DATASET が存在しません。"
    fi

    verbose_echo "ソースデータセットで新しいスナップショットを作成します。"
    verbose_echo "スナップショット名: $SNAPSHOT_NAME"
    run_cmd "$SOURCE_USER_HOST" "zfs snapshot $FULL_SNAPSHOT_NAME"
}

# 部分的に受信されたデータセットを処理する関数
handle_partial_recv() {
    local DATASET="$1"
    local USER_HOST="$2"
    
    RECV_DATASET="${DATASET}/%recv"
    RECV_EXISTS=$(run_cmd "$USER_HOST" "zfs list -H -o name $RECV_DATASET 2>/dev/null")
    
    if [ -n "$RECV_EXISTS" ]; then
        log_message "部分的に受信されたデータセット ${RECV_DATASET} を削除します。"
        run_cmd "$USER_HOST" "zfs destroy -R $RECV_DATASET"
        if [ $? -eq 0 ]; then
            log_message "部分的に受信されたデータセットの削除に成功しました。"
            return 0
        else
            log_message "部分的に受信されたデータセットの削除に失敗しました。"
            return 1
        fi
    else
        log_message "部分的に受信されたデータセットは見つかりませんでした。"
        return 0
    fi
}

# データセットを送信するコマンドを構築する関数
build_send_command() {
    log_message "レジュームトークンを確認します。"
    RESUME_TOKEN=$(get_resume_token "$DEST_USER_HOST" "$TARGET_DATASET")
    log_message "取得されたレジュームトークン: $RESUME_TOKEN"

    if [ "$RESUME_TOKEN" != "-" ] && [ -n "$RESUME_TOKEN" ]; then
        log_message "レジュームトークンが見つかりました。転送を再開します。"
        SEND_CMD="zfs send -v -t $RESUME_TOKEN"
        TRANSFER_TYPE="RESUME"
    else
        log_message "レジュームトークンが見つかりません。部分的に受信されたデータセットをチェックします。"
        handle_partial_recv "$TARGET_DATASET" "$DEST_USER_HOST"
        
        log_message "共通のスナップショットを確認します。"
        COMMON_SNAPSHOT_NAME=$(get_common_snapshot)
        log_message "共通のスナップショット名: $COMMON_SNAPSHOT_NAME"
        if [ -z "$COMMON_SNAPSHOT_NAME" ] || [ "$FORCE_SEND" -eq 1 ]; then
            log_message "フル送信を行います。"
            prepare_snapshots
            SEND_CMD="zfs send -v $FULL_SNAPSHOT_NAME"
            TRANSFER_TYPE="FULL"
        else
            log_message "インクリメンタル送信を行います。"
            prepare_snapshots
            PREVIOUS_SNAPSHOT_NAME="$COMMON_SNAPSHOT_NAME"
            if [ "$FULL_INCREMENTAL" -eq 1 ]; then
                SEND_CMD="zfs send -v -I ${SOURCE_DATASET}@${COMMON_SNAPSHOT_NAME} ${FULL_SNAPSHOT_NAME}"
            else
                SEND_CMD="zfs send -v -i ${SOURCE_DATASET}@${COMMON_SNAPSHOT_NAME} ${FULL_SNAPSHOT_NAME}"
            fi
            TRANSFER_TYPE="INCREMENTAL"
        fi
    fi

    if [ -z "$SEND_CMD" ]; then
        report_error "送信コマンドが構築できませんでした。"
    fi
}

# データセットを受信するコマンドを構築する関数
build_receive_command() {
    RECEIVE_CMD="zfs receive -s -F $TARGET_DATASET"
}

# データ転送を実行する関数
execute_transfer() {
    log_message "送信コマンド: $SEND_CMD"
    log_message "受信コマンド: $RECEIVE_CMD"
    log_message "転送タイプ: $TRANSFER_TYPE"

    if [ "$TRANSFER_TYPE" = "RESUME" ]; then
        log_message "レジューム送信を実行中です。フル送信のように表示されますが、実際には中断した場所から再開しています。"
    fi

    # コマンドの生成
    SEND_CMD_FULL="$SEND_CMD"
    RECEIVE_CMD_FULL="$RECEIVE_CMD"

    # 送信コマンドがリモートの場合
    if [ -n "$SOURCE_USER_HOST" ]; then
        SEND_CMD_FULL="ssh $SOURCE_USER_HOST 'sudo $SEND_CMD'"
    fi

    # 受信コマンドがリモートの場合
    if [ -n "$DEST_USER_HOST" ]; then
        RECEIVE_CMD_FULL="ssh $DEST_USER_HOST 'sudo $RECEIVE_CMD'"
    fi

    # 実行コマンドの表示
    log_message "実行コマンド: $SEND_CMD_FULL | $RECEIVE_CMD_FULL"

    # データ転送の実行
    eval "$SEND_CMD_FULL | $RECEIVE_CMD_FULL"

    TRANSFER_RESULT=$?

    if [ $TRANSFER_RESULT -ne 0 ]; then
        report_error "データ転送中にエラーが発生しました。"
    else
        log_message "データ転送が正常に完了しました。"
    fi
}

# 古いスナップショットを削除する関数
cleanup_snapshots() {
    if [ -n "$PREVIOUS_SNAPSHOT_NAME" ]; then
        verbose_echo "インクリメンタル送信が完了しました。古いスナップショットを削除します。"

        verbose_echo "ソースの古いスナップショットを削除します: ${SOURCE_DATASET}@${PREVIOUS_SNAPSHOT_NAME}"
        run_cmd "$SOURCE_USER_HOST" "zfs destroy ${SOURCE_DATASET}@${PREVIOUS_SNAPSHOT_NAME}"

        verbose_echo "ターゲットの古いスナップショットを削除します: ${TARGET_DATASET}@${PREVIOUS_SNAPSHOT_NAME}"
        run_cmd "$DEST_USER_HOST" "zfs destroy ${TARGET_DATASET}@${PREVIOUS_SNAPSHOT_NAME}" || verbose_echo "警告: リモートのスナップショット削除に失敗しました。手動での確認が必要です。"
    fi
}

# 引数の解析
parse_arguments_and_setup() {
    FORCE_SEND=0
    SNAPSHOT_PREFIX="zsync"
    FIXED_SNAPSHOT_NAME=""
    VERBOSE_MODE=0
    CLEANUP_MODE=0
    FULL_INCREMENTAL=0

    # オリジナルの引数を保存
    ORIGINAL_ARGS="$@"

    while getopts "fs:VvCIl:" opt; do
        case "$opt" in
            f) FORCE_SEND=1 ;;
            s) FIXED_SNAPSHOT_NAME="$OPTARG" ;;
            V) VERBOSE_MODE=1 ;;
            v) show_version ;;
            C) CLEANUP_MODE=1 ;;
            I) FULL_INCREMENTAL=1 ;;
            l) LOG_FILE="$OPTARG" ;;
            *) usage ;;
        esac
    done
    shift $((OPTIND -1))

    # オプションを取り除いた引数を ORIGINAL_ARGS に設定
    ORIGINAL_ARGS="$@"

    # 引数の数に基づいてパラメータ形式を判断
    if [ $# -eq 3 ]; then
        verbose_echo "旧形式のパラメータが検出されました。"
        USE_OLD_FORMAT="true"
        REMOTE_USER_HOST="$1"
        SOURCE_DATASET="$2"
        TARGET_DATASET="$3"
        SOURCE_USER_HOST=""
        DEST_USER_HOST="$REMOTE_USER_HOST"
        
        if [ -z "$FIXED_SNAPSHOT_NAME" ]; then
            REMOTE_HOSTNAME=$(extract_hostname "$REMOTE_USER_HOST")
            FIXED_SNAPSHOT_NAME="${SNAPSHOT_PREFIX}-${REMOTE_HOSTNAME}"
        fi
    elif [ $# -eq 2 ]; then
        verbose_echo "新形式のパラメータが検出されました。"
        SOURCE_PARSED=$(parse_dataset "$1")
        SOURCE_USER_HOST=$(echo "$SOURCE_PARSED" | cut -d' ' -f1)
        SOURCE_DATASET=$(echo "$SOURCE_PARSED" | cut -d' ' -f2)

        TARGET_PARSED=$(parse_dataset "$2")
        DEST_USER_HOST=$(echo "$TARGET_PARSED" | cut -d' ' -f1)
        TARGET_DATASET=$(echo "$TARGET_PARSED" | cut -d' ' -f2)
    else
        usage
    fi

    verbose_echo "Source User@Host: $SOURCE_USER_HOST"
    verbose_echo "Source Dataset: $SOURCE_DATASET"
    verbose_echo "Dest User@Host: $DEST_USER_HOST"
    verbose_echo "Target Dataset: $TARGET_DATASET"
    verbose_echo "Snapshot Prefix: $FIXED_SNAPSHOT_NAME"

    if [ "$CLEANUP_MODE" -eq 1 ]; then
        verbose_echo "クリーンアップモードが有効です。部分的に受信されたデータセットを確認します。"
        handle_partial_recv "$TARGET_DATASET" "$DEST_USER_HOST"
    fi
}

# メイン処理
main() {
    parse_arguments_and_setup "$@"
    build_send_command
    build_receive_command
    execute_transfer
    if [ $? -eq 0 ]; then
        cleanup_snapshots
        log_message "同期処理が正常に完了しました。"
    fi
}

# スクリプトの実行
main "$@"

