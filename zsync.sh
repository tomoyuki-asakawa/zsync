#!/bin/sh

VERSION="2.2"

# グローバル変数の初期化
USE_MBUFFER=0
ZFS_SEND_OPTION="-v"
SOURCE_SSH=""
SOURCE_DATASET=""
DESTINATION_SSH=""
DESTINATION_DATASET=""
RECORD_SIZE=""
BUFFER_SIZE=""
TRANSFER_RATE="-R 20m"
PREFIX=""
MBUFFER_CMD="cat"
IS_REMOTE_SOURCE=0
IS_REMOTE_DESTINATION=0

# 割り込み処理
trap 'echo -e "\n転送が中断されました。"; stty sane; exit 1' INT

# 使用方法の表示
show_usage() {
    echo "$0 version $VERSION"
    echo "Usage:"
    echo "  $0 [-m BUFFER_SIZE] [-R TRANSFER_RATE] [-P PREFIX] [-B] [-s RECORD_SIZE] SOURCE DESTINATION"
    echo "    SOURCE can be: DATASET or user@HOST:DATASET"
    echo "    DESTINATION can be: DATASET, user@HOST:DATASET, or user@HOST"
}

# ホストがローカルかどうかを判断する
is_local_host() {
    local ssh_target="$1"
    local host

    # user@host 形式の場合、host部分を抽出
    host=$(echo "$ssh_target" | awk -F'@' '{print $NF}')
    [ -z "$host" ] || [ "$host" = "$(hostname)" ] || [ "$host" = "localhost" ]
}

# 旧形式のパラメータを新形式に変換する
convert_legacy_format() {
    local first_arg="$1"
    local second_arg="$2"
    local third_arg="$3"

    if echo "$first_arg" | grep -q "@" && [ -n "$second_arg" ] && [ -n "$third_arg" ]; then
        # 旧形式: user@host source_dataset destination_dataset -> 新形式: source_dataset user@host:destination_dataset
        SOURCE_DATASET="$second_arg"
        DESTINATION_SSH="$first_arg"
        DESTINATION_DATASET="$third_arg"
        IS_REMOTE_DESTINATION=1
        echo "Notice: Legacy format detected, converted to new format."
    else
        echo "Error: Invalid legacy format. Please check your arguments."
        exit 1
    fi
}

# ソースとデスティネーションが同じかを確認
check_source_and_destination() {
    if [ "$SOURCE_DATASET" = "$DESTINATION_DATASET" ] && [ "$SOURCE_SSH" = "$DESTINATION_SSH" ]; then
        echo "Error: Source and destination cannot be the same."
        exit 1
    fi
}

# ソースとデスティネーションを解析する
parse_source_and_destination() {
    local source_arg="$1"
    local destination_arg="$2"

    # ソースの解析
    if echo "$source_arg" | grep -q ":"; then
        SOURCE_SSH=$(echo "$source_arg" | cut -d':' -f1)
        SOURCE_DATASET=$(echo "$source_arg" | cut -d':' -f2-)
        IS_REMOTE_SOURCE=1
    else
        SOURCE_DATASET="$source_arg"
    fi

    # デスティネーションの解析
    if echo "$destination_arg" | grep -q ":"; then
        DESTINATION_SSH=$(echo "$destination_arg" | cut -d':' -f1)
        DESTINATION_DATASET=$(echo "$destination_arg" | cut -d':' -f2-)
        IS_REMOTE_DESTINATION=1
    elif echo "$destination_arg" | grep -q "@"; then
        DESTINATION_SSH="$destination_arg"
        DESTINATION_DATASET="$SOURCE_DATASET"
        IS_REMOTE_DESTINATION=1
    else
        DESTINATION_DATASET="$destination_arg"
    fi
}

# リモートホストの判定を行う
check_remote_host() {
    if [ -n "$SOURCE_SSH" ] && ! is_local_host "$SOURCE_SSH"; then
        IS_REMOTE_SOURCE=1
    fi
    if [ -n "$DESTINATION_SSH" ] && ! is_local_host "$DESTINATION_SSH"; then
        IS_REMOTE_DESTINATION=1
    fi
}

# コマンドライン引数を処理する
process_arguments() {
    while [ $# -gt 0 ]; do
        case "$1" in
            -P) shift; PREFIX="$1"; shift ;;
            -B) USE_MBUFFER=1; shift ;;
            -s) shift; RECORD_SIZE="-s $1"; shift ;;
            -m) shift; BUFFER_SIZE="-m $1"; shift ;;
            -R) shift; TRANSFER_RATE="-R $1"; shift ;;
            -V) echo "$0 version $VERSION"; exit 0 ;;
            -h) show_usage; exit 0 ;;
            --) shift; break ;;
            *)  break ;;
        esac
    done

    if [ $# -lt 2 ]; then
        echo "Error: Incorrect number of arguments."
        show_usage
        exit 1
    elif [ $# -eq 3 ]; then
        # 旧形式のパラメータ
        convert_legacy_format "$1" "$2" "$3"
        shift 3
    else
        # 新形式のパラメータ
        parse_source_and_destination "$1" "$2"
        shift 2
    fi

    # ソースとデスティネーションが同じかを確認
    check_source_and_destination

    # リモートホストの判定
    check_remote_host

    # 通知
    if [ "$IS_REMOTE_SOURCE" -eq 0 ] && [ -n "$SOURCE_SSH" ]; then
        echo "Notice: Source is on the local host."
    fi
    if [ "$IS_REMOTE_DESTINATION" -eq 0 ] && [ -n "$DESTINATION_SSH" ]; then
        echo "Notice: Destination is on the local host."
    fi
}

# プール名を取得する関数
get_pool_name() {
    echo "$1" | cut -d'/' -f1
}

# 親データセットが存在するかを確認する関数
parent_exists() {
    local dataset="$1"
    local parent_dataset=$(dirname "$dataset")

    if sudo zfs list "$parent_dataset" > /dev/null 2>&1; then
        return 0
    else
        echo "Error: Parent dataset '$parent_dataset' does not exist."
        return 1
    fi
}

# デスティネーションプールや親ディレクトリが存在しない場合はエラーにする
check_destination_pool() {
    local destination_dataset="$1"
    local pool_name=$(get_pool_name "$destination_dataset")

    if ! sudo zfs list "$pool_name" > /dev/null 2>&1; then
        echo "Error: Destination pool '$pool_name' does not exist. Please create it manually and try again."
        exit 1
    fi

    if ! parent_exists "$destination_dataset"; then
        echo "Error: Parent dataset for destination '$destination_dataset' does not exist."
        exit 1
    fi
}

# クローンとプロモートを実行する関数
clone_and_promote() {
    local latest_snapshot="$1"
    local clone_name="$DESTINATION_DATASET"

    # クローンの作成前にデスティネーションプールが存在するかを確認
    check_destination_pool "$DESTINATION_DATASET"

    sudo zfs clone "$SOURCE_DATASET@$latest_snapshot" "$clone_name"
    if [ $? -eq 0 ]; then
        sudo zfs promote "$clone_name"
        echo "Local clone & promote Success"
    else
        echo "Error: Failed to clone and promote."
        exit 1
    fi
}

# パラメータの初期化と検証
initialize_parameters() {
    if [ "$USE_MBUFFER" -eq 1 ]; then
        echo "mbuffer 使用中: buffer size=$BUFFER_SIZE, transfer rate=$TRANSFER_RATE, record size=$RECORD_SIZE"
        MBUFFER_CMD="mbuffer -q $BUFFER_SIZE $TRANSFER_RATE $RECORD_SIZE"
    else
        echo "mbuffer 無効"
        MBUFFER_CMD="cat"
    fi

    # PREFIXが設定されていない場合、適切なホスト名を使用
    if [ -z "$PREFIX" ]; then
        if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
            PREFIX="zsync-$(ssh "$SOURCE_SSH" hostname)"
        else
            PREFIX="zsync-$(hostname)"
        fi
    fi
}

# ソーススナップショットの作成
create_source_snapshot() {
    local snapshot_name="$PREFIX-$(date +%Y%m%d-%H%M%S)"
    if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
        ssh "$SOURCE_SSH" "sudo zfs snapshot $SOURCE_DATASET@$snapshot_name"
    else
        sudo zfs snapshot "$SOURCE_DATASET@$snapshot_name"
    fi
    echo "$snapshot_name"
}

# 最新のデスティネーションスナップショットの取得
get_latest_destination_snapshot() {
    if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
        ssh "$DESTINATION_SSH" "sudo zfs list -t snapshot -H -o name" 2>/dev/null | grep "^$DESTINATION_DATASET@$PREFIX" | sort | tail -1
    else
        sudo zfs list -t snapshot -H -o name 2>/dev/null | grep "^$DESTINATION_DATASET@$PREFIX" | sort | tail -1
    fi
}

# 送信側の再開トークンの取得
get_send_resume_token() {
    if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
        ssh "$SOURCE_SSH" "zfs get -H -o value send_resume_token $SOURCE_DATASET@$LATEST_SNAPSHOT" 2>/dev/null || true
    else
        zfs get -H -o value send_resume_token "$SOURCE_DATASET@$LATEST_SNAPSHOT" 2>/dev/null || true
    fi
}

# 受信側の再開トークンの取得
get_receive_resume_token() {
    if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
        ssh "$DESTINATION_SSH" "zfs get -H -o value receive_resume_token $DESTINATION_DATASET" 2>/dev/null || true
    else
        zfs get -H -o value receive_resume_token "$DESTINATION_DATASET" 2>/dev/null || true
    fi
}

# 再開トークンを使用した転送の再開
resume_transfer() {
    local send_resume_token="$1"
    local receive_resume_token="$2"
    echo "Resuming transfer using send_resume_token: $send_resume_token and receive_resume_token: $receive_resume_token"

    if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
        ssh "$SOURCE_SSH" "sudo zfs send $ZFS_SEND_OPTION -t $send_resume_token" | $MBUFFER_CMD | \
        if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
            ssh "$DESTINATION_SSH" "$MBUFFER_CMD | sudo zfs receive -F $DESTINATION_DATASET"
        else
            sudo zfs receive -F "$DESTINATION_DATASET"
        fi
    else
        sudo zfs send $ZFS_SEND_OPTION -t "$send_resume_token" | $MBUFFER_CMD | \
        if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
            ssh "$DESTINATION_SSH" "$MBUFFER_CMD | sudo zfs receive -F $DESTINATION_DATASET"
        else
            sudo zfs receive -F "$DESTINATION_DATASET"
        fi
    fi
}

# 完全な初期転送
full_transfer() {
    local latest_snapshot="$1"

    if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
        echo "Performing initial full send of $SOURCE_DATASET@$latest_snapshot from $SOURCE_SSH"
        ssh "$SOURCE_SSH" "sudo zfs send $ZFS_SEND_OPTION $SOURCE_DATASET@$latest_snapshot" | $MBUFFER_CMD | \
        if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
            ssh "$DESTINATION_SSH" "$MBUFFER_CMD | sudo zfs receive -F $DESTINATION_DATASET"
        else
            sudo zfs receive -F "$DESTINATION_DATASET"
        fi
    else
        echo "Performing initial full send of $SOURCE_DATASET@$latest_snapshot to destination"
        sudo zfs send $ZFS_SEND_OPTION "$SOURCE_DATASET@$latest_snapshot" | $MBUFFER_CMD | \
        if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
            ssh "$DESTINATION_SSH" "$MBUFFER_CMD | sudo zfs receive -F $DESTINATION_DATASET"
        else
            sudo zfs receive -F "$DESTINATION_DATASET"
        fi
    fi
}

# インクリメンタル転送
incremental_transfer() {
    local previous_snapshot="$1"
    local latest_snapshot="$2"
    local source_snapshot_name=$(echo "$previous_snapshot" | awk -F@ '{print $2}')

    echo "Sending incremental changes from $source_snapshot_name to $latest_snapshot"
    if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
        ssh "$SOURCE_SSH" "sudo zfs send $ZFS_SEND_OPTION -I $source_snapshot_name $SOURCE_DATASET@$latest_snapshot" | $MBUFFER_CMD | \
        if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
            ssh "$DESTINATION_SSH" "$MBUFFER_CMD | sudo zfs receive -F $DESTINATION_DATASET"
        else
            sudo zfs receive -F "$DESTINATION_DATASET"
        fi
    else
        sudo zfs send "$ZFS_SEND_OPTION" -I "$source_snapshot_name" "$SOURCE_DATASET@$latest_snapshot" | $MBUFFER_CMD | \
        if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
            ssh "$DESTINATION_SSH" "$MBUFFER_CMD | sudo zfs receive -F $DESTINATION_DATASET"
        else
            sudo zfs receive -F "$DESTINATION_DATASET"
        fi
    fi

    if [ $? -eq 0 ]; then
        cleanup_snapshots "$source_snapshot_name" "$latest_snapshot"
    else
        echo "Error: zfs send/receive failed"
        exit 1
    fi
}

# スナップショットのクリーンアップ
cleanup_snapshots() {
    local previous_snapshot="$1"
    local latest_snapshot="$2"

    if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
        ssh "$SOURCE_SSH" "sudo zfs destroy $SOURCE_DATASET@$previous_snapshot" 2>/dev/null || true
    else
        sudo zfs destroy "$SOURCE_DATASET@$previous_snapshot" 2>/dev/null || true
    fi

    if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
        ssh "$DESTINATION_SSH" "sudo zfs destroy $DESTINATION_DATASET@$previous_snapshot" 2>/dev/null || true
    else
        sudo zfs destroy "$DESTINATION_DATASET@$previous_snapshot" 2>/dev/null || true
    fi
}

# 転送処理の実行
perform_transfer() {
    local latest_snapshot="$1"
    local send_resume_token
    local receive_resume_token

    # 最新のスナップショット名をグローバル変数として設定
    LATEST_SNAPSHOT="$latest_snapshot"

    # 送信側と受信側の再開トークンを取得
    send_resume_token=$(get_send_resume_token)
    receive_resume_token=$(get_receive_resume_token)

    if [ -n "$send_resume_token" ] && [ "$send_resume_token" != "-" ] && [ -n "$receive_resume_token" ] && [ "$receive_resume_token" != "-" ]; then
        echo "Resuming previous transfer."
        resume_transfer "$send_resume_token" "$receive_resume_token"
    else
        local previous_snapshot=$(get_latest_destination_snapshot)
        if [ -z "$previous_snapshot" ]; then
            echo "No previous snapshot found. Performing full transfer."
            full_transfer "$latest_snapshot"
        else
            echo "Previous snapshot found. Performing incremental transfer."
            incremental_transfer "$previous_snapshot" "$latest_snapshot"
        fi
    fi
}

# メイン処理
main() {
    process_arguments "$@"
    initialize_parameters

    echo "Transfer configuration:"
    if [ "$IS_REMOTE_SOURCE" -eq 1 ]; then
        echo "  Source: Remote $SOURCE_SSH:$SOURCE_DATASET"
    else
        echo "  Source: Local $SOURCE_DATASET"
    fi
    if [ "$IS_REMOTE_DESTINATION" -eq 1 ]; then
        echo "  Destination: Remote $DESTINATION_SSH:$DESTINATION_DATASET"
    else
        echo "  Destination: Local $DESTINATION_DATASET"
    fi

    # スナップショットを作成
    latest_snapshot=$(create_source_snapshot)

    # 同じプール内でのローカル転送かどうかを確認
    if [ "$IS_REMOTE_SOURCE" -eq 0 ] && [ "$IS_REMOTE_DESTINATION" -eq 0 ]; then
        SOURCE_POOL=$(get_pool_name "$SOURCE_DATASET")
        DESTINATION_POOL=$(get_pool_name "$DESTINATION_DATASET")

        if [ "$SOURCE_POOL" = "$DESTINATION_POOL" ]; then
            echo "同じプール内でのローカル転送が検出されました。"
            clone_and_promote "$latest_snapshot"
        else
            perform_transfer "$latest_snapshot"
        fi
    else
        perform_transfer "$latest_snapshot"
    fi
}

# メイン処理の実行
main "$@"

