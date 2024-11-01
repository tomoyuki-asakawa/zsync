#!/bin/sh

# 互換性情報
# このスクリプトは FreeBSD の sh、macOS の zsh、および bash との互換性があります。

# グローバル変数
VERSION="5.42"
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
SOURCE_CMD=""
DESTINATION_CMD=""
SNAPSHOT_PREFIX=""
USE_IP=0

# シグナルハンドラ関数
handle_interrupt() {
    print_message "\nInterrupt received. Cleaning up and exiting..."
    exit 1
}

# シグナルをキャッチする
trap 'handle_interrupt' INT TERM

# メッセージ出力関数
print_message() {
    echo "$1" >&2
}

# Verboseメッセージ出力関数（VERBOSEモードがオンの場合のみ出力）
verbose_message() {
    if [ "$VERBOSE" -eq 1 ]; then
        echo "$1" >&2
    fi
}

# コマンドを実行し、結果とエラーコードを返す関数
execute_command_with_error() {
    local source_cmd="$1"
    local dest_cmd="$2"
    
    local full_cmd
    if [ -n "$source_cmd" ] && [ -n "$dest_cmd" ]; then
        full_cmd="$SOURCE_CMD $source_cmd | $DESTINATION_CMD $dest_cmd"
    elif [ -n "$source_cmd" ]; then
        full_cmd="$SOURCE_CMD $source_cmd"
    elif [ -n "$dest_cmd" ]; then
        full_cmd="$DESTINATION_CMD $dest_cmd"
    else
        print_message "エラー: コマンドが指定されていません。"
        return 1
    fi
    
    verbose_message "実行するコマンド: $full_cmd"
    
    if [ "$DRY_RUN" -eq 0 ]; then
        eval "$full_cmd"
        local exit_code=$?
        if [ $exit_code -ne 0 ]; then
            print_message "エラー: コマンド実行が失敗しました。終了コード: $exit_code"
            return $exit_code
        fi
    else
        print_message "ドライラン: 以下のコマンドを実行します: $full_cmd"
    fi
}

# ZFS send/receive を実行する関数
execute_zfs_send_receive() {
    local send_cmd="$1"
    local receive_cmd="$2"
    
    execute_command_with_error "$send_cmd" "$receive_cmd"
}

# ホスト識別子を取得する関数
get_host_identifier() {
    local user_host="$1"
    
    if [ -z "$user_host" ] || [ "$user_host" = "localhost" ]; then
        if [ "$USE_IP" = "1" ]; then
            hostname -I | awk '{print $1}'
        else
            hostname
        fi
    else
        if [ "$USE_IP" = "1" ]; then
            ssh "$user_host" "hostname -I | awk '{print \$1}'" 2>/dev/null
        else
            ssh "$user_host" hostname 2>/dev/null
        fi
    fi
}

# デフォルトプレフィックスを生成する関数
generate_default_prefix() {
    local source_host=$(get_host_identifier "$SOURCE_SSH")
    local dest_host=$(get_host_identifier "$DESTINATION_SSH")
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
        SOURCE_CMD="sudo"
    fi

    if [ -n "$DESTINATION_SSH" ]; then
        DESTINATION_CMD="ssh $DESTINATION_SSH sudo"
    else
        DESTINATION_CMD="sudo"
    fi

    if [ -z "$SOURCE_PREFIX" ]; then
        SOURCE_PREFIX=$(generate_default_prefix)
    fi
    SNAPSHOT_PREFIX="$SOURCE_PREFIX"

    if [ -z "$DESTINATION_PREFIX" ]; then
        DESTINATION_PREFIX="$SNAPSHOT_PREFIX"
    fi
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
    local timestamp=$(date +%Y%m%d%H%M%S)
    echo "${SNAPSHOT_PREFIX}-${timestamp}"
}

# 最新のスナップショットを取得する関数
get_latest_snapshot() {
    verbose_message "最新スナップショットを取得: データセット=$DESTINATION_DATASET, プレフィックス=$DESTINATION_PREFIX"
    
    local cmd="zfs list -t snapshot -H -o name -s creation '$DESTINATION_DATASET' | grep '$DESTINATION_PREFIX' | tail -1 | cut -d'@' -f2"
    
    local result
    result=$(execute_command_with_error "" "$cmd")
    local exit_code=$?
    
    if [ $exit_code -ne 0 ] || [ -z "$result" ]; then
        verbose_message "最新スナップショットの取得に失敗しました。データセットにスナップショットが存在しない可能性があります。"
        return 1
    fi
    
    echo "$result"
}

# 対応するソーススナップショットを見つける関数
find_corresponding_source_snapshot() {
    local dest_snapshot="$1"
    local cmd="zfs list -t snapshot -H -o name | grep \"$SOURCE_DATASET@$dest_snapshot\""
    
    verbose_message "対応するソーススナップショットを確認: $cmd"
    
    local result=$(execute_command_with_error "$cmd" "")
    local exit_code=$?
    
    verbose_message "コマンド実行結果: $result (終了コード: $exit_code)"
    
    echo "$result"
}

# 新しいスナップショットを作成する関数
create_snapshot() {
    local snapshot_name=$(generate_snapshot_name)
    
    verbose_message "新しいスナップショットを作成します: ${SOURCE_DATASET}@${snapshot_name}"
    execute_command_with_error "zfs snapshot ${SOURCE_DATASET}@${snapshot_name}" ""
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        verbose_message "スナップショットを作成しました: ${SOURCE_DATASET}@${snapshot_name}"
        echo "$snapshot_name"
    else
        print_message "エラー: スナップショットの作成に失敗しました。"
        echo ""
    fi
}

# フルセンドを実行する関数
perform_full_send() {
    local new_snapshot=$(create_snapshot)
    if [ -n "$new_snapshot" ]; then
        verbose_message "スナップショットのフルセンドを実行します: $new_snapshot"
        local send_cmd="zfs send"
        if [ "$VERBOSE" -eq 1 ]; then
            send_cmd="$send_cmd -v"
        fi
        send_cmd="$send_cmd ${SOURCE_DATASET}@${new_snapshot}"
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
    local current_snapshot="$2"  # 新しいスナップショットを引数として受け取る

    if [ -z "$current_snapshot" ]; then
        print_message "エラー: 新しいスナップショットが提供されていません。"
        return 1
    fi

    local base_dataset=$(echo "$base_snapshot" | cut -d'@' -f1)
    local base_snapshot_name=$(echo "$base_snapshot" | cut -d'@' -f2)

    verbose_message "インクリメンタルセンドを実行: $base_snapshot から ${SOURCE_DATASET}@${current_snapshot} へ"
    
    local send_cmd="zfs send"
    if [ "$VERBOSE" -eq 1 ]; then
        send_cmd="$send_cmd -v"
    fi
    send_cmd="$send_cmd -i ${base_dataset}@${base_snapshot_name} ${SOURCE_DATASET}@${current_snapshot}"
    local receive_cmd="zfs receive -F ${DESTINATION_DATASET}"
    
    execute_zfs_send_receive "$send_cmd" "$receive_cmd"
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

# 暫定　cleanup　only
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

perfom_cleanup() {
        print_message "古いスナップショットの削除を開始します。"
        
        verbose_message "ソース側のクリーンアップを開始"
        cleanup_old_snapshots "$SOURCE_DATASET" "$SOURCE_PREFIX" "$SOURCE_CMD"
        
        verbose_message "デスティネーション側のクリーンアップを開始"
        cleanup_old_snapshots "$DESTINATION_DATASET" "$DESTINATION_PREFIX" "$DESTINATION_CMD"
        
        print_message "古いスナップショットの削除が完了しました。"
}

# エラー報告関数
report_error() {
    print_message "エラー: $1"
    exit 1
}

# 初期情報を表示する関数
display_initial_info() {
    verbose_message "Source      : ${SOURCE_SSH:+$SOURCE_SSH:}$SOURCE_DATASET@$SOURCE_PREFIX"
    verbose_message "Destination : ${DESTINATION_SSH:+$DESTINATION_SSH:}$DESTINATION_DATASET@$DESTINATION_PREFIX"
}

# レジュームトークンを取得する関数
get_resume_token() {
    verbose_message "デスティネーションデータセットのレジュームトークンを確認しています: $DESTINATION_DATASET"
    local cmd="zfs get -H -o value receive_resume_token $DESTINATION_DATASET"
    local result=$(execute_command_with_error "" "$cmd")
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

# レジューム転送を実行する関数
resume_transfer() {
    local resume_token="$1"
    if [ -n "$resume_token" ]; then
        local send_cmd="zfs send"
        if [ "$VERBOSE" -eq 1 ]; then
            send_cmd="$send_cmd -v"
        fi
        send_cmd="$send_cmd -t $resume_token"
        local receive_cmd="zfs receive -s ${DESTINATION_DATASET}"
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

# 差分を表示し、差分がなければ false を返す関数
show_snapshot_diff() {
    local previous_snapshot="$1"
    local current_snapshot="$2"
    local silent="$3"  # サイレントモードかどうかのフラグ

    if [ -z "$previous_snapshot" ] || [ -z "$current_snapshot" ]; then
        print_message "エラー: 差分を表示するために両方のスナップショットが必要です。"
        return 1
    fi
    
    local diff_cmd="zfs diff -H ${SOURCE_DATASET}@${previous_snapshot} ${SOURCE_DATASET}@${current_snapshot}"
    
    if [ "$silent" -eq 0 ]; then
        verbose_message "差分を表示するコマンド: $diff_cmd"
    fi

    local diff_output=$(execute_command_with_error "$diff_cmd" "")
    
    if [ -z "$diff_output" ];then
        if [ "$silent" -eq 0 ]; then
            print_message "差分がありません。転送をスキップします。"
        fi
        return 1  # 差分がない場合はエラーコード 1 を返す
    fi
    
    if [ "$silent" -eq 0 ]; then
        echo "$diff_output"
    fi
    return 0  # 差分があった場合は成功を示す
}

# 引数をパースし、設定を更新する関数
parse_arguments() {
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
    echo "  -k number          保持するスナップショットの数（デフォルト: 3）" >&2
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
    
    if [ "$VERBOSE" -eq 1 ]; then
        display_initial_info
    fi

    if [ "$DRY_RUN" -eq 1 ]; then
        print_message "ドライランモード: 実際の変更は行われません。"
    fi

    local resume_token=$(get_resume_token)
    
    if [ -n "$resume_token" ]; then
        verbose_message "レジュームトークンを使用して転送を再開します。"
        resume_transfer "$resume_token"
    else
        local dest_latest=$(get_latest_snapshot)
        
        if [ -z "$dest_latest" ] || [ "$FORCE_SEND" -eq 1 ]; then
            if [ "$FORCE_SEND" -eq 1 ]; then
                verbose_message "フルセンドが強制されました。"
            else
                verbose_message "デスティネーションに既存のスナップショットがありません。フルセンドを実行します。"
            fi
            perform_full_send
        else
            verbose_message "デスティネーションの最新スナップショット: $dest_latest"
            local corresponding_source=$(find_corresponding_source_snapshot "$dest_latest")
            if [ -z "$corresponding_source" ]; then
                verbose_message "対応するソーススナップショットが見つかりません: $dest_latest"
                verbose_message "フルセンドを実行します。"
                perform_full_send
            else
                verbose_message "対応するソーススナップショットが見つかりました: $corresponding_source"
                
                # 新しいスナップショットを一度だけ作成
                local current_snapshot=$(create_snapshot)

                local previous_snapshot=$(get_latest_snapshot)
                
                # -D オプションがある場合は差分を表示し、なければサイレントに実行
                if [ "$SHOW_DIFF_FILES" -eq 1 ]; then
                    if ! show_snapshot_diff "$previous_snapshot" "$current_snapshot" 0; then
                        verbose_message "差分がないため、インクリメンタルセンドをスキップします。"
                        return 0
                    fi
                else
                    if ! show_snapshot_diff "$previous_snapshot" "$current_snapshot" 1; then
                        verbose_message "差分がないため、インクリメンタルセンドをスキップします。"
                        return 0
                    fi
                fi

                verbose_message "インクリメンタルセンドを実行します。"
                perform_incremental_send "$corresponding_source" "$current_snapshot"
            fi
        fi
    fi

    # クリーンアップ処理
    if [ "$CLEANUP_MODE" -eq 1 ]; then
        perfom_cleanup
    fi
        
    if [ "$DRY_RUN" -eq 1 ]; then
        print_message "ドライランが完了しました。変更は行われていません。"
    elif [ "$VERBOSE" -eq 1 ]; then
        print_message "ZSync操作が完了しました。"
    fi
}

# スクリプトの実行
main "$@"