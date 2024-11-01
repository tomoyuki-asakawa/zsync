#!/bin/sh

# 互換性情報
# このスクリプトは FreeBSD の sh、macOS の zsh、および bash との互換性があります。

# グローバル変数
VERSION="5.33"

# シグナルハンドラ関数
handle_interrupt() {
    print_message "\nInterrupt received. Cleaning up and exiting..."
    exit 1
}

# シグナルをキャッチする
trap 'handle_interrupt' INT TERM

# 設定変数
SOURCE_SSH=""
SOURCE_DATASET=""
DESTINATION_SSH=""
DESTINATION_DATASET=""
SOURCE_PREFIX=""
DESTINATION_PREFIX=""
FULL_INCREMENTAL=0
VERBOSE=0
FORCE_SEND=0
CLEANUP_MODE=0
SHOW_DIFF_FILES=0
DRY_RUN=0
KEEP_SNAPSHOTS=3  # デフォルトで保持するスナップショットの数

# 実行コマンド
SOURCE_CMD=""
DESTINATION_CMD=""

# 事前計算された設定
NEW_SNAPSHOT_NAME=""


# メッセージ出力関数
print_message() {
    echo "$1" >&2
}

# Verboseメッセージ出力関数
verbose_message() {
    if [ "$VERBOSE" -eq 1 ]; then
        echo "VERBOSE: $1" >&2
    fi
}

# コマンドを構築する関数
build_command() {
    local user_host="$1"
    local command="$2"
    
    if [ -z "$user_host" ] || [ "$user_host" = "localhost" ]; then
        echo "$command"
    else
        echo "ssh $user_host $command"
    fi
}

# ZFS send/receive を実行する関数
execute_zfs_send_receive() {
    local send_cmd="$1"
    local receive_cmd="$2"
    
    local full_cmd
    if [ -n "$SOURCE_CMD" ]; then
        full_cmd="$SOURCE_CMD $send_cmd | $DESTINATION_CMD $receive_cmd"
    else
        full_cmd="$send_cmd | $DESTINATION_CMD $receive_cmd"
    fi

    verbose_message "ZFS send/receive コマンド: $full_cmd"
    
    if [ "$DRY_RUN" -eq 0 ]; then
        eval "$full_cmd"
        local exit_code=$?
        if [ $exit_code -ne 0 ]; then
            print_message "エラー: ZFS 転送が失敗しました。終了コード: $exit_code"
            return 1
        fi
    else
        print_message "ドライラン: 以下のコマンドを実行します: $full_cmd"
    fi
}

# ホスト識別子を取得する関数
get_host_identifier() {
    local user_host="$1"
    local use_ip="$2"
    
    if [ -z "$user_host" ] || [ "$user_host" = "localhost" ]; then
        if [ "$use_ip" = "1" ]; then
            hostname -I | awk '{print $1}'
        else
            hostname
        fi
    else
        if [ "$use_ip" = "1" ]; then
            ssh "$user_host" "hostname -I | awk '{print \$1}'" 2>/dev/null
        else
            ssh "$user_host" hostname 2>/dev/null
        fi
    fi
}

# デフォルトプレフィックスを生成する関数
generate_default_prefix() {
    local source_host=$(get_host_identifier "$SOURCE_SSH" "$USE_IP")
    local dest_host=$(get_host_identifier "$DESTINATION_SSH" "$USE_IP")
    local sanitized_source_dataset=$(sanitize_name "$SOURCE_DATASET")
    local sanitized_dest_dataset=$(sanitize_name "$DESTINATION_DATASET")

    local prefix="zsync"

    if [ "$source_host" != "$dest_host" ]; then
        prefix="${prefix}-${source_host}-${dest_host}"
    fi

    if [ "$sanitized_source_dataset" != "$sanitized_dest_dataset" ]; then
        prefix="${prefix}-${sanitized_source_dataset}-${sanitized_dest_dataset}"
    fi

    echo "$prefix"
}

# 設定を初期化し、事前計算を行う関数
initialize_config() {
    parse_arguments "$@"
    
    if [ -n "$SOURCE_SSH" ]; then
        SOURCE_CMD="ssh $SOURCE_SSH sudo"
    else
        SOURCE_CMD=""
    fi

    if [ -n "$DESTINATION_SSH" ]; then
        DESTINATION_CMD="ssh $DESTINATION_SSH sudo"
    else
        DESTINATION_CMD=""
    fi

    if [ -z "$SOURCE_PREFIX" ]; then
        SOURCE_PREFIX=$(generate_default_prefix)
    fi
    SNAPSHOT_PREFIX="$SOURCE_PREFIX"

    if [ -z "$DESTINATION_PREFIX" ]; then
        DESTINATION_PREFIX="$SNAPSHOT_PREFIX"
    fi

    verbose_message "使用するスナップショットプレフィックス: $SNAPSHOT_PREFIX"
}

# ソースとデスティネーションの情報を解析する関数
parse_source_destination() {
    SOURCE="$1"
    SOURCE_SSH=$(echo "$SOURCE" | cut -d: -f1)
    SOURCE_DATASET=$(echo "$SOURCE" | cut -d: -f2)
    if [ "$SOURCE" = "$SOURCE_DATASET" ]; then
        SOURCE_SSH=""
    fi

    DESTINATION="$2"
    DESTINATION_SSH=$(echo "$DESTINATION" | cut -d: -f1)
    DESTINATION_DATASET=$(echo "$DESTINATION" | cut -d: -f2)
    if [ "$DESTINATION" = "$DESTINATION_DATASET" ]; then
        DESTINATION_SSH=""
    fi
}

# 名前をサニタイズする関数
sanitize_name() {
    echo "$1" | sed 's/[^A-Za-z0-9._-]/_/g'
}

# スナップショット名を生成する関数
generate_snapshot_name() {
    local prefix="$1"
    local timestamp=$(date +%Y%m%d%H%M%S)
    echo "${prefix}-${timestamp}"
}

# 最新のスナップショットを取得する関数
get_latest_snapshot() {
    local dataset="$1"
    local prefix="$2"
    local cmd_prefix="$3"
    
    verbose_message "最新スナップショットを取得: データセット=$dataset, プレフィックス=$prefix"
    
    local cmd="zfs list -t snapshot -H -o name -s creation '$dataset' | grep '$prefix' | tail -1 | cut -d'@' -f2"
    
    if [ -n "$cmd_prefix" ]; then
        verbose_message "リモートコマンドを実行: $cmd_prefix \"$cmd\""
        result=$($cmd_prefix "$cmd")
    else
        verbose_message "ローカルコマンドを実行: $cmd"
        result=$(eval "$cmd")
    fi
    
    local exit_code=$?
    verbose_message "コマンド実行結果: $result (終了コード: $exit_code)"
    
    echo "$result"
}

# 対応するソーススナップショットを見つける関数
find_corresponding_source_snapshot() {
    local dest_snapshot="$1"
    local cmd="zfs list -t snapshot -H -o name | grep \"$SOURCE_DATASET@$dest_snapshot\""
    
    verbose_message "対応するソーススナップショットを確認: $cmd"
    
    local result
    if [ -n "$SOURCE_CMD" ]; then
        result=$($SOURCE_CMD "$cmd")
    else
        result=$(eval "$cmd")
    fi
    
    local exit_code=$?
    verbose_message "コマンド実行結果: $result (終了コード: $exit_code)"
    
    echo "$result"
}

# 新しいスナップショットを作成する関数
create_snapshot() {
    local dataset="$1"
    local cmd="$2"
    local snapshot_name=$(generate_snapshot_name "$SNAPSHOT_PREFIX")
    
    verbose_message "新しいスナップショットを作成します: ${dataset}@${snapshot_name}"
    local result=$(execute_command_with_error "$cmd" zfs snapshot "${dataset}@${snapshot_name}")
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        verbose_message "スナップショットを作成しました: ${dataset}@${snapshot_name}"
        echo "$snapshot_name"
    else
        print_message "エラー: スナップショットの作成に失敗しました。$result"
        echo ""
    fi
}

# フルセンドを実行する関数
perform_full_send() {
    local new_snapshot=$(create_snapshot "$SOURCE_DATASET" "$SOURCE_CMD")
    if [ -n "$new_snapshot" ]; then
        print_message "スナップショットのフルセンドを実行します: $new_snapshot"
        local send_cmd="zfs send -v ${SOURCE_DATASET}@${new_snapshot}"
        local receive_cmd="zfs receive -s -F ${DESTINATION_DATASET}"
        execute_zfs_send_receive "$send_cmd" "$receive_cmd"
    else
        print_message "エラー: 新しいスナップショットの作成に失敗しました。"
        return 1
    fi
}

# インクリメンタルセンドを実行する関数
perform_incremental_send() {
    local base_snapshot="$1"
    # ベーススナップショットからデータセット名とスナップショット名を分離
    local base_dataset=$(echo "$base_snapshot" | cut -d'@' -f1)
    local base_snapshot_name=$(echo "$base_snapshot" | cut -d'@' -f2)

    local new_snapshot=$(create_snapshot "$SOURCE_DATASET" "$SOURCE_CMD")
    if [ -n "$new_snapshot" ]; then
        verbose_message "新しいスナップショットを作成しました: $new_snapshot"
        verbose_message "インクリメンタルセンドを実行: $base_snapshot から ${SOURCE_DATASET}@${new_snapshot} へ"
        
        # 送信コマンドを適切に構築
        local send_cmd="zfs send -v -i ${base_dataset}@${base_snapshot_name} ${SOURCE_DATASET}@${new_snapshot}"
        local receive_cmd="zfs receive -F ${DESTINATION_DATASET}"
        
        execute_zfs_send_receive "$send_cmd" "$receive_cmd"
    else
        print_message "エラー: 新しいスナップショットの作成に失敗しました。"
        return 1
    fi
}

# コマンドを実行し、結果とエラーコードを返す関数
execute_command_with_error() {
    local cmd_prefix="$1"
    shift
    local full_cmd="$cmd_prefix $*"
    
    verbose_message "実行するコマンド: $full_cmd"
    
    local output
    local exit_code
    
    if [ -n "$cmd_prefix" ]; then
        output=$($cmd_prefix "$@" 2>&1)
        exit_code=$?
    else
        output=$("$@" 2>&1)
        exit_code=$?
    fi
    
    echo "$output"
    return $exit_code
}

# 古いスナップショットを削除する関数
cleanup_old_snapshots() {
    local dataset="$1"
    local prefix="$2"
    local cmd_prefix="$3"
    
    verbose_message "古いスナップショットの削除を開始: データセット=$dataset, プレフィックス=$prefix"
    
    local snapshots_list=$(get_snapshots_list "$dataset" "$prefix" "$cmd_prefix")
    local total_snapshots=$(echo "$snapshots_list" | wc -l)
    
    verbose_message "合計スナップショット数: $total_snapshots"
    verbose_message "保持するスナップショット数: $KEEP_SNAPSHOTS"
    
    if [ "$total_snapshots" -le "$KEEP_SNAPSHOTS" ]; then
        verbose_message "削除する古いスナップショットはありません。"
        return 0
    fi
    
    local snapshots_to_delete=$(echo "$snapshots_list" | sort -r | tail -n +$((KEEP_SNAPSHOTS + 1)))
    local delete_count=$(echo "$snapshots_to_delete" | wc -l)
    
    verbose_message "削除対象のスナップショット数: $delete_count"
    verbose_message "削除対象のスナップショット:"
    echo "$snapshots_to_delete"
    
    local actually_deleted=0
    
    if [ "$DRY_RUN" -eq 0 ]; then
        local old_IFS="$IFS"
        IFS=$'\n'
        for snapshot in $snapshots_to_delete; do
            verbose_message "処理中のスナップショット: $snapshot"
            local delete_cmd="zfs destroy $snapshot"
            verbose_message "実行コマンド: $delete_cmd"
# cleanup_old_snapshots 関数内の修正後の部分
if execute_command "$cmd_prefix $delete_cmd"; then
    verbose_message "スナップショット $snapshot を削除しました。"
    actually_deleted=$((actually_deleted + 1))
else
    print_message "エラー: スナップショット $snapshot の削除に失敗しました。"
fi
            verbose_message "現在の削除数: $actually_deleted"
        done
        IFS="$old_IFS"
    else
        verbose_message "ドライランモード: 以下のコマンドが実行されます。"
        echo "$snapshots_to_delete" | while read -r snapshot; do
            verbose_message "$ $cmd_prefix zfs destroy $snapshot"
        done
        actually_deleted=$delete_count
    fi
    
    verbose_message "削除処理が完了しました。削除されたスナップショット数: $actually_deleted"
}

# スナップショットのリストを取得する関数
get_snapshots_list() {
    local dataset="$1"
    local prefix="$2"
    local cmd_prefix="$3"
    
    local cmd="zfs list -t snapshot -H -o name -s creation | grep '${dataset}@${prefix}' | sort"
    
    verbose_message "スナップショットリスト取得コマンド: $cmd_prefix $cmd"
    
    local result
    if [ -n "$cmd_prefix" ]; then
        result=$($cmd_prefix "$cmd")
    else
        result=$(eval "$cmd")
    fi
    
    echo "$result"
}

execute_command() {
    local full_cmd="$1"
    
    verbose_message "execute_command: full_cmd='$full_cmd'"
    
    if [ -n "$full_cmd" ]; then
        eval "$full_cmd"
    else
        return 1
    fi
    return $?
}

# エラー報告関数
report_error() {
    print_message "エラー: $1"
    exit 1
}

# 初期情報を表示する関数
display_initial_info() {
    print_message "ソース: ${SOURCE_SSH:+$SOURCE_SSH:}$SOURCE_DATASET"
    print_message "デスティネーション: ${DESTINATION_SSH:+$DESTINATION_SSH:}$DESTINATION_DATASET"
    verbose_message "ソーススナップショットプレフィックス: $SOURCE_PREFIX"
    verbose_message "デスティネーションスナップショットプレフィックス: $DESTINATION_PREFIX"
}

get_resume_token() {
    local dataset="$1"
    local cmd="$2"
    
    verbose_message "デスティネーションデータセットのレジュームトークンを確認しています: $dataset"
    result=$(execute_command_with_error "$cmd" zfs get -H -o value receive_resume_token "$dataset")
    local exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        verbose_message "レジュームトークンの取得エラー。終了コード: $exit_code"
        echo ""
    elif [ -z "$result" ] || [ "$result" = "-" ]; then
        verbose_message "有効なレジュームトークンが見つかりません。"
        echo ""
    else
        verbose_message "レジュームトークン: $result"
        echo "$result"
    fi
}

resume_transfer() {
    local resume_token="$1"
    if [ -n "$resume_token" ]; then
        local send_cmd="zfs send -v -t $resume_token"
        local receive_cmd="zfs receive -s $DESTINATION_DATASET"
        if ! execute_zfs_send_receive "$send_cmd" "$receive_cmd"; then
            print_message "エラー: レジュームトークンを使用した転送に失敗しました。"
            return 1
        fi
        return 0
    else
        print_message "エラー: 無効なレジュームトークンです。"
        return 1
    fi
}

# 引数をパースし、設定を更新する関数
parse_arguments() {
    USE_IP=0
    while getopts "fs:d:IVvCk:Dni" opt; do
        case "$opt" in
            f) FORCE_SEND=1 ;;
            s) SOURCE_PREFIX="$OPTARG" ;;
            d) DESTINATION_PREFIX="$OPTARG" ;;
            I) FULL_INCREMENTAL=1 ;;
            V) VERBOSE=1 ;;
            v) show_version ;;
            C) CLEANUP_MODE=1 ;;
            k) KEEP_SNAPSHOTS="$OPTARG" ;;
            D) SHOW_DIFF_FILES=1 ;;
            n) DRY_RUN=1 ;;
            i) USE_IP=1 ;;
            *) usage ;;
        esac
    done
    shift $((OPTIND -1))

    if [ $# -ne 2 ]; then
        usage
    fi

    parse_source_destination "$1" "$2"
}

# 使用方法を表示する関数
usage() {
    echo "使用方法: $0 [オプション] source_user@source_host:source_dataset destination_user@destination_host:destination_dataset" >&2
    echo "オプション:" >&2
    echo "  -f                フルセンドを強制する" >&2
    echo "  -s snapshot_prefix ソーススナップショットプレフィックスを設定（デフォルト: 自動生成）" >&2
    echo "  -d snapshot_prefix デスティネーションスナップショットプレフィックスを設定（デフォルト: ソースと同じ）" >&2
    echo "  -I                フルインクリメンタルセンドを実行する" >&2
    echo "  -V                Verboseモードを有効にする" >&2
    echo "  -v                バージョン情報を表示する" >&2
    echo "  -C                クリーンアップモードを有効にする" >&2
    echo "  -k number          保持するスナップショットの数（デフォルト: 5）" >&2
    echo "  -D                差分ファイルを表示する" >&2
    echo "  -n                ドライラン（実際の変更は行わない）" >&2
    echo "  -i                IPアドレスを使用する（デフォルト: ホスト名）" >&2
    echo "" >&2
    echo "このスクリプトは FreeBSD sh、macOS zsh、bash と互換性があります。" >&2
    exit 1
}

# バージョン情報を表示する関数
show_version() {
    echo "ZSync スクリプトバージョン $VERSION" >&2
    echo "FreeBSD sh、macOS zsh、bash と互換性があります。" >&2
    exit 0
}

# メイン処理関数
main() {
    initialize_config "$@"
    display_initial_info

    if [ "$DRY_RUN" -eq 1 ]; then
        print_message "ドライランモード: 実際の変更は行われません。"
    fi

    local resume_token=$(get_resume_token "$DESTINATION_DATASET" "$DESTINATION_CMD")
    
    if [ -n "$resume_token" ]; then
        print_message "レジュームトークンを使用して転送を再開します。"
        resume_transfer "$resume_token"
    else
        local dest_latest=$(get_latest_snapshot "$DESTINATION_DATASET" "$DESTINATION_PREFIX" "$DESTINATION_CMD")
        
        if [ -z "$dest_latest" ] || [ "$FORCE_SEND" -eq 1 ]; then
            print_message "デスティネーションに既存のスナップショットがありません。フルセンドを実行します。"
            perform_full_send
        else
            verbose_message "デスティネーションの最新スナップショット: $dest_latest"
            local corresponding_source=$(find_corresponding_source_snapshot "$dest_latest")
            if [ -z "$corresponding_source" ]; then
                print_message "対応するソーススナップショットが見つかりません: $dest_latest"
                print_message "フルセンドを実行します。"
                perform_full_send
            else
                verbose_message "対応するソーススナップショットが見つかりました: $corresponding_source"
                print_message "インクリメンタルセンドを実行します。"
                perform_incremental_send "$corresponding_source"
            fi
        fi
    fi

    # クリーンアップ処理
    if [ "$CLEANUP_MODE" -eq 1 ]; then
        print_message "古いスナップショットの削除を開始します。"
        
        verbose_message "ソース側のクリーンアップを開始"
        cleanup_old_snapshots "$SOURCE_DATASET" "$SOURCE_PREFIX" "$SOURCE_CMD"
        
        verbose_message "デスティネーション側のクリーンアップを開始"
        cleanup_old_snapshots "$DESTINATION_DATASET" "$DESTINATION_PREFIX" "$DESTINATION_CMD"
        
        print_message "古いスナップショットの削除が完了しました。"
    fi
    
    if [ "$DRY_RUN" -eq 1 ]; then
        print_message "ドライランが完了しました。変更は行われていません。"
    else
        print_message "ZSync操作が完了しました。"
    fi
}

# スクリプトの実行
main "$@"

